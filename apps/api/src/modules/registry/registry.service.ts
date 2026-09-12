import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { PrismaService } from '@cajeerbots/db';
import { checkEngineCompatibility, type ExtensionManifest } from '@cajeerbots/extension-sdk';
import { compareSemver, latestVersion, parseRegistryManifest, type RegistryChannel, type RegistryManifest } from '@cajeerbots/registry';
import type { Principal } from '@cajeerbots/security';

const ENGINE_API_VERSION = '1.0.0-rc.1';

@Injectable()
export class RegistryService {
  constructor(private readonly prisma: PrismaService) {}

  sources() {
    return this.prisma.registrySource.findMany({ orderBy: { createdAt: 'desc' }, include: { _count: { select: { packages: true } } } });
  }

  createSource(input: { name: string; url: string; type?: string; authSecretRef?: string; publicKey?: string; isActive?: boolean }, actor: Principal) {
    return this.prisma.$transaction(async (tx) => {
      const source = await tx.registrySource.create({ data: { ...input, type: input.type ?? 'gitflic_registry', isActive: input.isActive ?? true } });
      await tx.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'registry.source.create', resource: 'registry_source', resourceId: source.id, metadata: { name: source.name, url: source.url } } });
      return source;
    });
  }

  async updateSource(id: string, input: { name?: string; url?: string; type?: string; authSecretRef?: string; publicKey?: string; isActive?: boolean }, actor: Principal) {
    await this.requireSource(id);
    const source = await this.prisma.registrySource.update({ where: { id }, data: input });
    await this.audit(actor, 'registry.source.update', source.id, { name: source.name });
    return source;
  }

  async syncSource(id: string, actor: Principal) {
    const source = await this.requireSource(id);
    if (!source.isActive) throw new BadRequestException('Registry source отключён');
    try {
      const manifest = await this.loadManifest(source.url);
      const synced = await this.upsertManifest(source.id, manifest);
      await this.prisma.registrySource.update({ where: { id }, data: { lastSyncAt: new Date(), syncStatus: 'ok', syncError: null, packageCount: synced.packages } });
      await this.audit(actor, 'registry.source.sync', id, { packages: synced.packages, versions: synced.versions });
      return { ok: true, sourceId: id, ...synced };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await this.prisma.registrySource.update({ where: { id }, data: { lastSyncAt: new Date(), syncStatus: 'failed', syncError: message } });
      throw new BadRequestException({ code: 'REGISTRY_SYNC_FAILED', message });
    }
  }

  packages(query: { sourceId?: string; kind?: string; q?: string; take: number }) {
    return this.prisma.registryPackage.findMany({
      where: {
        ...(query.sourceId ? { sourceId: query.sourceId } : {}),
        ...(query.kind ? { kind: query.kind } : {}),
        ...(query.q ? { OR: [{ name: { contains: query.q, mode: 'insensitive' as const } }, { displayName: { contains: query.q, mode: 'insensitive' as const } }, { description: { contains: query.q, mode: 'insensitive' as const } }] } : {}),
      },
      orderBy: [{ isOfficial: 'desc' }, { name: 'asc' }],
      take: query.take,
      include: { source: true, versions: { orderBy: { createdAt: 'desc' }, take: 5 } },
    });
  }

  async packageByName(name: string, sourceId?: string) {
    const row = await this.prisma.registryPackage.findFirst({ where: { name, ...(sourceId ? { sourceId } : {}) }, include: { source: true, versions: { orderBy: { createdAt: 'desc' } } } });
    if (!row) throw new NotFoundException('Registry package не найден');
    return row;
  }

  async installPackage(name: string, input: { version?: string; channel?: RegistryChannel; autoGrant?: boolean }, actor: Principal) {
    const pkg = await this.packageByName(name);
    const selected = input.version
      ? pkg.versions.find((version) => version.version === input.version)
      : pkg.versions.filter((version) => !version.isYanked && (!input.channel || version.channel === input.channel)).sort((a, b) => compareSemver(b.version, a.version))[0];
    if (!selected) throw new NotFoundException('Подходящая версия package не найдена');
    if (selected.isYanked) throw new BadRequestException('Выбранная версия package отозвана');
    if (pkg.kind !== 'plugin' && pkg.kind !== 'module') throw new BadRequestException('Через этот endpoint устанавливаются только modules/plugins');
    if (!selected.manifest) throw new BadRequestException('Registry version не содержит extension manifest');
    const manifest = selected.manifest as ExtensionManifest;
    const compatibility = checkEngineCompatibility(manifest.engine.range, ENGINE_API_VERSION);
    if (!compatibility.compatible) throw new BadRequestException({ code: 'EXTENSION_INCOMPATIBLE', reasons: compatibility.reasons });

    const extension = await this.prisma.extension.create({
      data: {
        name: manifest.name,
        version: manifest.version,
        engineRange: manifest.engine.range,
        manifest: manifest as never,
        signature: selected.signature,
        status: 'installed',
        registrySourceId: pkg.sourceId,
        registryPackageName: pkg.name,
        registryVersionId: selected.id,
      },
    });

    if (input.autoGrant && Array.isArray(manifest.permissions)) {
      for (const permission of manifest.permissions) {
        await this.prisma.extensionPermissionGrant.upsert({ where: { extensionId_permission: { extensionId: extension.id, permission } }, create: { extensionId: extension.id, permission, actorId: actor.id, reason: 'autoGrant при установке из registry' }, update: { revokedAt: null, actorId: actor.id } });
      }
    }
    await this.audit(actor, 'registry.package.install', extension.id, { packageName: pkg.name, version: selected.version, autoGrant: Boolean(input.autoGrant) });
    return this.prisma.extension.findUniqueOrThrow({ where: { id: extension.id }, include: { permissionGrants: true } });
  }

  private async upsertManifest(sourceId: string, manifest: RegistryManifest): Promise<{ packages: number; versions: number }> {
    let versionCount = 0;
    for (const item of manifest.packages) {
      const latest = latestVersion(item.versions);
      const pkg = await this.prisma.registryPackage.upsert({
        where: { sourceId_name: { sourceId, name: item.name } },
        create: {
          sourceId,
          name: item.name,
          displayName: item.displayName,
          description: item.description,
          kind: item.kind,
          publisher: item.publisher,
          homepage: item.homepage,
          repository: item.repository,
          license: item.license,
          isOfficial: item.official,
          latestVersion: latest?.version,
          metadata: item.metadata as never,
        },
        update: {
          displayName: item.displayName,
          description: item.description,
          kind: item.kind,
          publisher: item.publisher,
          homepage: item.homepage,
          repository: item.repository,
          license: item.license,
          isOfficial: item.official,
          latestVersion: latest?.version,
          metadata: item.metadata as never,
        },
      });
      for (const version of item.versions) {
        await this.prisma.registryPackageVersion.upsert({
          where: { packageId_version: { packageId: pkg.id, version: version.version } },
          create: {
            packageId: pkg.id,
            version: version.version,
            engineRange: version.engine.range,
            channel: version.channel,
            manifest: version.manifest as never,
            artifactUrl: version.artifact.url,
            artifactSha256: version.artifact.sha256,
            artifactSize: version.artifact.size,
            signature: version.artifact.signature,
            signatureAlgorithm: version.artifact.signatureAlgorithm,
            releaseNotes: version.releaseNotes,
            isYanked: version.yanked,
            metadata: version.metadata as never,
            publishedAt: version.publishedAt ? new Date(version.publishedAt) : undefined,
          },
          update: {
            engineRange: version.engine.range,
            channel: version.channel,
            manifest: version.manifest as never,
            artifactUrl: version.artifact.url,
            artifactSha256: version.artifact.sha256,
            artifactSize: version.artifact.size,
            signature: version.artifact.signature,
            signatureAlgorithm: version.artifact.signatureAlgorithm,
            releaseNotes: version.releaseNotes,
            isYanked: version.yanked,
            metadata: version.metadata as never,
            publishedAt: version.publishedAt ? new Date(version.publishedAt) : undefined,
          },
        });
        versionCount += 1;
      }
    }
    return { packages: manifest.packages.length, versions: versionCount };
  }

  private async loadManifest(url: string): Promise<RegistryManifest> {
    if (url.startsWith('file://')) return parseRegistryManifest(JSON.parse(await readFile(fileURLToPath(url), 'utf8')));
    if (url.startsWith('/') || url.startsWith('./') || url.startsWith('../')) return parseRegistryManifest(JSON.parse(await readFile(url, 'utf8')));
    const response = await fetch(url, { headers: { accept: 'application/json' } });
    if (!response.ok) throw new Error(`HTTP ${response.status} при загрузке registry manifest`);
    return parseRegistryManifest(await response.json());
  }

  private async requireSource(id: string) {
    const source = await this.prisma.registrySource.findUnique({ where: { id } });
    if (!source) throw new NotFoundException('Registry source не найден');
    return source;
  }

  private audit(actor: Principal, action: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource: 'registry', resourceId, metadata: metadata as never } });
  }
}
