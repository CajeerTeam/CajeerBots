import { createHash } from 'node:crypto';
import { access } from 'node:fs/promises';
import { isAbsolute, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { PrismaService } from '@cajeerbots/db';
import { extensionManifestSchema, type CajeerExtension, type ExtensionContext, type ExtensionEventEnvelope, type ExtensionManifest } from '@cajeerbots/extension-sdk';
import { hookMatches, missingPermissions, normalizeHookList } from './runtime-events.js';

export interface DbExtensionRuntimeOptions {
  extensionsRoot?: string;
  maxAttempts?: number;
}

export interface EnqueueExtensionEventOptions {
  extensionId?: string;
  idempotencyKey?: string;
  hook: string;
  payload: unknown;
}

interface ExtensionRecord {
  id: string;
  name: string;
  version: string;
  manifest: unknown;
}

export class DbExtensionRuntime {
  constructor(
    private readonly prisma: PrismaService,
    private readonly options: DbExtensionRuntimeOptions = {},
  ) {}

  async enqueueHook(input: EnqueueExtensionEventOptions): Promise<{ queued: number; skipped: Array<{ extensionId: string; reason: string }> }> {
    const hook = input.hook.trim();
    if (!hook) throw new Error('Extension hook не может быть пустым');

    const extensions = input.extensionId
      ? await this.prisma.extension.findMany({ where: { id: input.extensionId, status: 'enabled' } })
      : await this.prisma.extension.findMany({ where: { status: 'enabled' }, orderBy: { installedAt: 'asc' } });

    let queued = 0;
    const skipped: Array<{ extensionId: string; reason: string }> = [];

    for (const extension of extensions) {
      const manifest = parseManifest(extension.manifest);
      const hooks = normalizeHookList(manifest.hooks);
      if (!hooks.some((pattern) => hookMatches(pattern, hook))) continue;

      const permissions = await this.grantedPermissions(extension.id);
      const missing = missingPermissions(manifest.permissions, permissions);
      if (missing.length > 0) {
        skipped.push({ extensionId: extension.id, reason: `permissions_not_granted:${missing.join(',')}` });
        continue;
      }

      const data = { extensionId: extension.id, hook, payload: input.payload as never, status: 'pending' };
      if (input.idempotencyKey) {
        const id = `hook-${createHash('sha256').update(JSON.stringify([extension.id, hook, input.idempotencyKey])).digest('hex')}`;
        await this.prisma.extensionRuntimeEvent.upsert({ where: { id }, create: { id, ...data }, update: {} });
      } else await this.prisma.extensionRuntimeEvent.create({ data });
      queued += 1;
    }

    return { queued, skipped };
  }

  async processPending(limit = 25): Promise<{ processed: number; failed: number }> {
    const events = await this.prisma.extensionRuntimeEvent.findMany({
      where: { status: { in: ['pending', 'failed'] }, availableAt: { lte: new Date() } },
      orderBy: { createdAt: 'asc' },
      take: limit,
    });

    let processed = 0;
    let failed = 0;
    for (const event of events) {
      try {
        await this.execute(event.id);
        processed += 1;
      } catch {
        failed += 1;
      }
    }
    return { processed, failed };
  }

  async execute(eventId: string): Promise<unknown> {
    const maxAttempts = this.options.maxAttempts ?? 5;
    const claim = await this.prisma.extensionRuntimeEvent.updateMany({ where: { id: eventId, status: { in: ['pending', 'failed'] }, availableAt: { lte: new Date() } }, data: { status: 'processing', startedAt: new Date(), attempts: { increment: 1 }, error: null } });
    if (!claim.count) return { skipped: true };
    const event = await this.prisma.extensionRuntimeEvent.findUniqueOrThrow({ where: { id: eventId } });

    if (!event.extensionId) throw new Error(`ExtensionRuntimeEvent ${eventId} не привязан к extensionId`);
    const extension = await this.prisma.extension.findUnique({ where: { id: event.extensionId } });
    if (!extension || extension.status !== 'enabled') {
      await this.prisma.extensionRuntimeEvent.update({ where: { id: eventId }, data: { status: 'dead', error: 'Extension не найден или отключён', completedAt: new Date() } });
      return { skipped: true };
    }

    const manifest = parseManifest(extension.manifest);
    try {
      if (event.attempts > maxAttempts || missingPermissions(manifest.permissions, await this.grantedPermissions(extension.id)).length) throw new Error('Extension permissions revoked or attempts exhausted');
      const module = await this.loadExtension(extension as ExtensionRecord, manifest);
      const context = this.createContext(extension.id, manifest, eventCommunity(event.payload));
      const envelope: ExtensionEventEnvelope = {
        id: event.id,
        hook: event.hook,
        payload: event.payload,
        extension: { id: extension.id, name: extension.name, version: extension.version },
        createdAt: event.createdAt.toISOString(),
      };

      if (module.activate) await module.activate(context);
      const result = module.handleEvent ? await module.handleEvent(context, envelope) : { handled: false, reason: 'handleEvent_not_exported' };
      await this.prisma.extensionRuntimeEvent.update({
        where: { id: eventId },
        data: { status: 'handled', result: result as never, completedAt: new Date() },
      });
      return result;
    } catch (error) {
      const attempts = event.attempts;
      const dead = attempts >= maxAttempts;
      const message = error instanceof Error ? error.message : String(error);
      await this.prisma.extensionRuntimeEvent.update({
        where: { id: eventId },
        data: {
          status: dead ? 'dead' : 'failed',
          error: message,
          completedAt: new Date(),
          availableAt: new Date(Date.now() + Math.min(300_000, 2 ** attempts * 1000)),
        },
      });
      throw error;
    }
  }

  private async grantedPermissions(extensionId: string): Promise<string[]> {
    const grants = await this.prisma.extensionPermissionGrant.findMany({
      where: { extensionId, revokedAt: null },
      select: { permission: true },
    });
    return grants.map((grant) => grant.permission);
  }

  private async loadExtension(extension: ExtensionRecord, manifest: ExtensionManifest): Promise<CajeerExtension> {
    const entry = resolveExtensionEntry(this.options.extensionsRoot ?? process.env.EXTENSIONS_ROOT ?? './extensions', manifest);
    await access(entry);
    const loaded = await import(`${pathToFileURL(entry).href}?cajeer=${Date.now()}`) as { default?: CajeerExtension } & CajeerExtension;
    const candidate = loaded.default ?? loaded;
    if (typeof candidate !== 'object' || candidate === null) throw new Error(`Extension ${extension.name} не экспортирует объект CajeerExtension`);
    return candidate;
  }

  private createContext(extensionId: string, manifest: ExtensionManifest, communityId?: string): ExtensionContext {
    const log = (level: string, message: string, metadata?: Record<string, unknown>) => this.prisma.extensionRuntimeLog.create({
      data: { extensionId, level, message, metadata: metadata as never },
    }).then(() => undefined);

    return {
      manifest,
      logger: {
        info: (message, metadata) => void log('info', message, metadata),
        warn: (message, metadata) => void log('warn', message, metadata),
        error: (message, metadata) => void log('error', message, metadata),
      },
      events: {
        publish: async (type, payload) => {
          await this.prisma.outboxMessage.create({ data: { topic: type, communityId, payload: { type, payload, source: `extension:${manifest.name}` } as never, headers: { source: 'extension-runtime', extensionId } as never } });
        },
        enqueueHook: async (hook, payload) => {
          await this.enqueueHook({ hook, payload: communityId ? { payload, communityId } : payload });
        },
      },
      storage: {
        get: async (namespace, key) => {
          const item = await this.prisma.extensionStorageItem.findUnique({ where: { extensionId_namespace_key: { extensionId, namespace, key } } });
          return item?.value;
        },
        set: async (namespace, key, value) => {
          await this.prisma.extensionStorageItem.upsert({
            where: { extensionId_namespace_key: { extensionId, namespace, key } },
            create: { extensionId, namespace, key, value: value as never },
            update: { value: value as never },
          });
        },
        delete: async (namespace, key) => {
          await this.prisma.extensionStorageItem.deleteMany({ where: { extensionId, namespace, key } });
        },
      },
    };
  }
}

export function parseManifest(value: unknown): ExtensionManifest {
  return extensionManifestSchema.parse(value);
}

export function resolveExtensionEntry(root: string, manifest: ExtensionManifest): string {
  const entry = manifest.entry || './dist/index.js';
  if (entry.startsWith('file://')) return new URL(entry).pathname;
  if (isAbsolute(entry)) return entry;
  return resolve(root, manifest.name, entry);
}

function eventCommunity(value: unknown): string | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const row = value as { communityId?: unknown; metadata?: { communityId?: unknown } };
  const id = row.communityId ?? row.metadata?.communityId;
  return typeof id === 'string' && id ? id : undefined;
}
