import { BadRequestException, ForbiddenException, ConflictException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { DbExtensionRuntime, missingPermissions, parseManifest, resolveExecutable } from '@cajeerbots/extension-runtime';
import { checkEngineCompatibility, type ExtensionManifest } from '@cajeerbots/extension-sdk';
import { hasScope, permissions, type Principal } from '@cajeerbots/security';

const ENGINE_API_VERSION = '1.0.0-rc.5';

@Injectable()
export class ExtensionsService {
  private readonly runtime: DbExtensionRuntime;

  constructor(private readonly prisma: PrismaService) {
    this.runtime = new DbExtensionRuntime(prisma);
  }

  list(status?: 'installed' | 'enabled' | 'disabled' | 'failed') {
    return this.prisma.extension.findMany({
      where: status ? { status } : {},
      orderBy: { installedAt: 'desc' },
      include: { permissionGrants: { where: { revokedAt: null } }, runtimeEvents: { orderBy: { createdAt: 'desc' }, take: 5 } },
    });
  }

  async get(id: string) {
    const extension = await this.prisma.extension.findUnique({
      where: { id },
      include: {
        permissionGrants: { orderBy: { grantedAt: 'desc' } },
        configHistory: { orderBy: { createdAt: 'desc' }, take: 10 },
        runtimeEvents: { orderBy: { createdAt: 'desc' }, take: 20 },
        runtimeLogs: { orderBy: { createdAt: 'desc' }, take: 20 },
      },
    });
    if (!extension) throw new NotFoundException('Расширение не найдено');
    return extension;
  }

  async install(manifest: ExtensionManifest, actor: Principal, signature?: string, autoGrant = false) {
    if (autoGrant && !hasScope(actor, permissions.extensionsPermissions)) throw new ForbiddenException('Permission grant scope required');
    if (signature || manifest.signature) throw new BadRequestException('Signed artifacts must be installed via Registry, not manifest-only install');
    await resolveExecutable({}, manifest).catch((error: Error) => { throw new BadRequestException(error.message); });
    const compatibility = checkEngineCompatibility(manifest.engine.range, ENGINE_API_VERSION);
    if (!compatibility.compatible) throw new BadRequestException({ code: 'EXTENSION_INCOMPATIBLE', reasons: compatibility.reasons });
    try {
      const extension = await this.prisma.extension.create({
        data: {
          name: manifest.name,
          version: manifest.version,
          engineRange: manifest.engine.range,
          manifest: manifest as never,
          signature: signature ?? manifest.signature,
          status: 'installed',
        },
      });
      if (autoGrant && manifest.permissions.length > 0) {
        for (const permission of manifest.permissions) {
          await this.prisma.extensionPermissionGrant.create({ data: { extensionId: extension.id, permission, actorId: actor.id, reason: 'autoGrant при установке' } });
        }
      }
      await this.audit(actor, 'extension.install', extension.id, { name: manifest.name, version: manifest.version, autoGrant });
      return this.get(extension.id);
    } catch (error) {
      if (isUniqueError(error)) throw new ConflictException('Такая версия расширения уже установлена');
      throw error;
    }
  }

  async enable(id: string, actor: Principal) {
    const extension = await this.prisma.extension.findUnique({ where: { id }, include: { permissionGrants: { where: { revokedAt: null } } } });
    if (!extension) throw new NotFoundException('Расширение не найдено');
    const manifest = parseManifest(extension.manifest);
    await resolveExecutable(extension, manifest).catch((error: Error) => { throw new BadRequestException(error.message); });
    const missing = missingPermissions(manifest.permissions, extension.permissionGrants.map((grant) => grant.permission));
    if (missing.length > 0) throw new BadRequestException({ code: 'EXTENSION_PERMISSIONS_NOT_GRANTED', missing });
    await this.prisma.$transaction(async (tx) => {
      await tx.$queryRaw`SELECT id FROM extensions WHERE id = ${id} FOR UPDATE`;
      const current = await tx.extension.findUniqueOrThrow({ where: { id }, include: { permissionGrants: { where: { revokedAt: null } } } });
      if (current.version !== extension.version || missingPermissions(manifest.permissions, current.permissionGrants.map((grant) => grant.permission)).length) throw new ConflictException('Extension changed during enable');
      await tx.extension.update({ where: { id }, data: { status: 'enabled' } });
      await tx.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'extension.enabled', resource: 'extension', resourceId: id } });
    });
    const enabled = await this.get(id);
    const queuedHooks = await this.runtime.enqueueHook({ extensionId: id, hook: 'extension.enabled', payload: { id, name: extension.name, version: extension.version } });
    await this.audit(actor, 'extension.runtime.enqueue', id, { hook: 'extension.enabled', queuedHooks });
    return enabled;
  }

  async setStatus(id: string, status: 'enabled' | 'disabled', actor: Principal, metadata: Record<string, unknown> = {}) {
    const exists = await this.prisma.extension.findUnique({ where: { id } });
    if (!exists) throw new NotFoundException('Расширение не найдено');
    const extension = await this.prisma.extension.update({ where: { id }, data: { status } });
    await this.audit(actor, `extension.${status}`, id, { name: extension.name, version: extension.version, ...metadata });
    return this.get(extension.id);
  }

  async permissions(id: string) {
    await this.assertExists(id);
    return this.prisma.extensionPermissionGrant.findMany({ where: { extensionId: id }, orderBy: { grantedAt: 'desc' } });
  }

  async grantPermissions(id: string, input: { permissions: string[]; reason?: string }, actor: Principal) {
    await this.assertExists(id);
    for (const permission of unique(input.permissions)) {
      await this.prisma.extensionPermissionGrant.upsert({
        where: { extensionId_permission: { extensionId: id, permission } },
        create: { extensionId: id, permission, actorId: actor.id, reason: input.reason },
        update: { revokedAt: null, actorId: actor.id, reason: input.reason },
      });
    }
    await this.audit(actor, 'extension.permissions.grant', id, { permissions: input.permissions, reason: input.reason });
    return this.permissions(id);
  }

  async revokePermissions(id: string, input: { permissions: string[]; reason?: string }, actor: Principal) {
    await this.assertExists(id);
    await this.prisma.extensionPermissionGrant.updateMany({ where: { extensionId: id, permission: { in: unique(input.permissions) } }, data: { revokedAt: new Date(), actorId: actor.id, reason: input.reason } });
    await this.audit(actor, 'extension.permissions.revoke', id, { permissions: input.permissions, reason: input.reason });
    return this.permissions(id);
  }

  async configHistory(id: string) {
    await this.assertExists(id);
    return this.prisma.extensionConfigHistory.findMany({ where: { extensionId: id }, orderBy: { createdAt: 'desc' }, take: 50 });
  }

  async setConfig(id: string, input: { value: Record<string, unknown>; reason?: string }, actor: Principal) {
    const extension = await this.assertExists(id);
    const current = latestConfigValue(extension.manifest, await this.prisma.extensionConfigHistory.findFirst({ where: { extensionId: id }, orderBy: { createdAt: 'desc' } }));
    const history = await this.prisma.extensionConfigHistory.create({ data: { extensionId: id, oldValue: current as never, newValue: input.value as never, actorId: actor.id, reason: input.reason } });
    await this.audit(actor, 'extension.config.update', id, { reason: input.reason });
    return history;
  }

  async enqueueEvent(id: string, input: { hook: string; payload: unknown }) {
    await this.assertExists(id);
    return this.runtime.enqueueHook({ extensionId: id, hook: input.hook, payload: input.payload });
  }

  async events(id: string, take = 50, status?: string) {
    await this.assertExists(id);
    return this.prisma.extensionRuntimeEvent.findMany({ where: { extensionId: id, ...(status ? { status } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Math.max(take, 1), 200) });
  }

  async retryEvent(eventId: string) {
    const event = await this.prisma.extensionRuntimeEvent.findUnique({ where: { id: eventId } });
    if (!event) throw new NotFoundException('Runtime event не найден');
    const changed = await this.prisma.extensionRuntimeEvent.updateMany({ where: { id: eventId, status: { in: ['failed', 'dead'] } }, data: { status: 'pending', attempts: 0, availableAt: new Date(), error: null } });
    if (!changed.count) throw new ConflictException('Only failed or dead events can be retried');
    return this.prisma.extensionRuntimeEvent.findUniqueOrThrow({ where: { id: eventId } });
  }

  async storage(id: string, namespace = 'default') {
    await this.assertExists(id);
    return this.prisma.extensionStorageItem.findMany({ where: { extensionId: id, namespace }, orderBy: { key: 'asc' } });
  }

  async setStorage(id: string, namespace: string, key: string, input: { value: unknown }) {
    await this.assertExists(id);
    return this.prisma.extensionStorageItem.upsert({
      where: { extensionId_namespace_key: { extensionId: id, namespace, key } },
      create: { extensionId: id, namespace, key, value: input.value as never },
      update: { value: input.value as never },
    });
  }

  async deleteStorage(id: string, namespace: string, key: string) {
    await this.assertExists(id);
    await this.prisma.extensionStorageItem.deleteMany({ where: { extensionId: id, namespace, key } });
    return { deleted: true, extensionId: id, namespace, key };
  }

  async logs(id: string, take = 100) {
    await this.assertExists(id);
    return this.prisma.extensionRuntimeLog.findMany({ where: { extensionId: id }, orderBy: { createdAt: 'desc' }, take: Math.min(Math.max(take, 1), 500) });
  }

  private async assertExists(id: string) {
    const extension = await this.prisma.extension.findUnique({ where: { id } });
    if (!extension) throw new NotFoundException('Расширение не найдено');
    return extension;
  }

  private audit(actor: Principal, action: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource: 'extension', resourceId, metadata: metadata as never } });
  }
}

function unique(values: string[]): string[] {
  return Array.from(new Set(values.map((value) => value.trim()).filter(Boolean)));
}

function latestConfigValue(manifest: unknown, latest: { newValue: unknown } | null): unknown {
  if (latest) return latest.newValue;
  return parseManifest(manifest).configSchema ? {} : null;
}

function isUniqueError(error: unknown): boolean {
  return typeof error === 'object' && error !== null && 'code' in error && (error as { code?: string }).code === 'P2002';
}
