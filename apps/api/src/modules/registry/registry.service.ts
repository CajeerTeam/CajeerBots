import { BadRequestException, ForbiddenException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { compareSemver, latestVersion, parseRegistryManifest, fetchExtensionArtifact, type RegistryChannel, type RegistryManifest } from '@cajeerbots/registry';
import { safeHttpRequest, hasScope, permissions, type Principal } from '@cajeerbots/security';

const ENGINE_API_VERSION = '1.0.0';

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
    const rows = await this.prisma.registryPackage.findMany({ where: { name, ...(sourceId ? { sourceId } : {}) }, take: 2, include: { source: true, versions: { orderBy: { createdAt: 'desc' } } } });
    if (rows.length > 1) throw new BadRequestException('Ambiguous registry package; specify sourceId');
    const row = rows[0];
    if (!row) throw new NotFoundException('Registry package не найден');
    return row;
  }

  async installPackage(name: string, input: { sourceId?: string; version?: string; channel?: RegistryChannel; autoGrant?: boolean }, actor: Principal) {
    if (input.autoGrant && !hasScope(actor, permissions.extensionsPermissions)) throw new ForbiddenException('Permission grant scope required');
    const pkg = await this.packageByName(name, input.sourceId);
    const selected = input.version
      ? pkg.versions.find((version) => version.version === input.version)
      : pkg.versions.filter((version) => !version.isYanked && (!input.channel || version.channel === input.channel)).sort((a, b) => compareSemver(b.version, a.version))[0];
    if (!selected) throw new NotFoundException('Подходящая версия package не найдена');
    if (selected.isYanked) throw new BadRequestException('Выбранная версия package отозвана');
    if (pkg.kind !== 'plugin' && pkg.kind !== 'module') throw new BadRequestException('Через этот endpoint устанавливаются только modules/plugins');
    if (!selected.manifest) throw new BadRequestException('Registry version не содержит extension manifest');
    const artifact = await fetchExtensionArtifact(selected, pkg.source, pkg.name, ENGINE_API_VERSION).catch((error: Error) => { throw new BadRequestException(error.message); });
    const { manifest, artifactSource, artifactCodeSha256 } = artifact;
    return this.prisma.$transaction(async (tx) => {
      const existing = await tx.extension.findUnique({ where: { name_version: { name: manifest.name, version: manifest.version } } });
      const data = { name: manifest.name, version: manifest.version, engineRange: manifest.engine.range, manifest: manifest as never, signature: selected.signature, artifactSource, artifactCodeSha256, status: 'installed' as const, registrySourceId: pkg.sourceId, registryPackageName: pkg.name, registryVersionId: selected.id };
      if (existing && (existing.artifactSource || existing.status === 'enabled' || (existing.registrySourceId && existing.registrySourceId !== pkg.sourceId))) throw new BadRequestException('Release already installed, enabled, or belongs to another registry');
      if (existing && await tx.extensionRuntimeEvent.count({ where: { extensionId: existing.id, status: 'processing' } })) throw new BadRequestException('Extension has running events');
      const extension = existing ? await tx.extension.update({ where: { id: existing.id }, data }) : await tx.extension.create({ data });
      if (input.autoGrant) for (const permission of new Set(manifest.permissions)) await tx.extensionPermissionGrant.upsert({ where: { extensionId_permission: { extensionId: extension.id, permission } }, create: { extensionId: extension.id, permission, actorId: actor.id, reason: 'Registry install' }, update: { revokedAt: null, actorId: actor.id } });
      await tx.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'registry.package.install', resource: 'extension', resourceId: extension.id, metadata: { packageName: pkg.name, version: selected.version, sha256: selected.artifactSha256 } } });
      return tx.extension.findUniqueOrThrow({ where: { id: extension.id }, include: { permissionGrants: true } });
    });
  }

  private async upsertManifest(sourceId: string, manifest: RegistryManifest): Promise<{ packages: number; versions: number }> {
    return this.prisma.$transaction(async (tx) => {
    let versionCount = 0;
    for (const item of manifest.packages) {
      const latest = latestVersion(item.versions);
      const pkg = await tx.registryPackage.upsert({
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
        const old = await tx.registryPackageVersion.findUnique({ where: { packageId_version: { packageId: pkg.id, version: version.version } } });
        if (old?.signature && old.artifactSha256 !== version.artifact.sha256) throw new Error(`Immutable signed release changed: ${item.name}@${version.version}`);
        await tx.registryPackageVersion.upsert({
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
    }, { timeout: 30000 });
  }

  private async loadManifest(url: string): Promise<RegistryManifest> {
    if (new URL(url).protocol !== 'https:') throw new Error('Registry source requires HTTPS');
    const response = await safeHttpRequest(url, { method: 'GET', maxResponseBytes: 2 * 1024 * 1024 });
    if (!response.ok) throw new Error(`Registry HTTP ${response.status}`);
    return parseRegistryManifest(JSON.parse(await response.text()));
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
