import { Injectable, NotFoundException } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { PrismaService } from '@cajeerbots/db';
import { createBackupBundle, createConfiguredStorage, decodeBackupBundle, encodeBackupBundle, exportCommunitySnapshot, includedResourcesFromCommunityPayload, sha256Hex, verifyBackupBundle } from '@cajeerbots/backup';
import type { Principal } from '@cajeerbots/security';
import { importCommunitySchema } from '../import-export/import-export.schemas.js';
import { ImportExportService } from '../import-export/import-export.service.js';

interface CreateBackupInput { title?: string; key?: string; metadata?: Record<string, unknown> }
interface RestoreBackupInput { targetCommunityId?: string; apply: boolean; strategy: 'merge' }

@Injectable()
export class BackupsService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly importExport: ImportExportService,
  ) {}

  storageObjects(input: { status?: string; refType?: string; refId?: string; take?: number }) {
    return this.prisma.storageObject.findMany({
      where: { ...(input.status ? { status: input.status } : {}), ...(input.refType ? { refType: input.refType } : {}), ...(input.refId ? { refId: input.refId } : {}) },
      orderBy: { createdAt: 'desc' },
      take: Math.min(input.take ?? 100, 500),
    });
  }

  async storageObject(id: string) {
    const object = await this.prisma.storageObject.findUnique({ where: { id } });
    if (!object) throw new NotFoundException('Storage object не найден');
    return object;
  }

  async deleteStorageObject(id: string, hard: boolean, actor: Principal) {
    const object = await this.storageObject(id);
    if (hard) {
      const storage = createConfiguredStorage();
      await storage.driver.delete(object.key).catch(() => undefined);
      const deleted = await this.prisma.storageObject.update({ where: { id }, data: { status: 'deleted', deletedAt: new Date() } });
      await this.audit(actor, 'storage.object.hard_delete', 'storage_object', id, { key: object.key, driver: object.driver });
      return deleted;
    }
    const deleted = await this.prisma.storageObject.update({ where: { id }, data: { status: 'deleted', deletedAt: new Date() } });
    await this.audit(actor, 'storage.object.delete', 'storage_object', id, { key: object.key, driver: object.driver });
    return deleted;
  }

  backups(input: { communityId?: string; status?: string; take?: number }) {
    return this.prisma.backupArtifact.findMany({
      where: { ...(input.communityId ? { communityId: input.communityId } : {}), ...(input.status ? { status: input.status } : {}) },
      orderBy: { createdAt: 'desc' },
      take: Math.min(input.take ?? 100, 500),
    });
  }

  async backup(id: string) {
    const artifact = await this.prisma.backupArtifact.findUnique({ where: { id } });
    if (!artifact) throw new NotFoundException('Backup artifact не найден');
    return artifact;
  }

  async createCommunityBackup(communityId: string, input: CreateBackupInput, actor: Principal) {
    const community = await this.prisma.community.findUnique({ where: { id: communityId } });
    if (!community) throw new NotFoundException('Community не найдена');

    const payload = await exportCommunitySnapshot(this.prisma, communityId);
    const includedResources = includedResourcesFromCommunityPayload(payload);
    const title = input.title ?? `Backup ${community.slug} ${new Date().toISOString()}`;
    const bundle = createBackupBundle({ scope: 'community', resourceId: communityId, title, payload, includedResources, metadata: input.metadata });
    const bytes = encodeBackupBundle(bundle);
    const sha256 = sha256Hex(bytes);
    const storage = createConfiguredStorage();
    const key = input.key ?? `${process.env.BACKUP_OBJECT_PREFIX ?? 'backups'}/community/${communityId}/${new Date().toISOString().replace(/[:.]/g, '-')}-${randomUUID()}.cajeer-backup.json`;

    const artifact = await this.prisma.$transaction(async (tx) => {
      const storageObject = await tx.storageObject.create({
        data: {
          key,
          driver: storage.driverName,
          bucket: storage.bucket,
          contentType: 'application/vnd.cajeerbots.backup+json',
          sizeBytes: bytes.byteLength,
          sha256,
          refType: 'backup_artifact',
          status: 'uploading',
          metadata: { root: storage.root, resources: includedResources } as never,
        },
      });
      const created = await tx.backupArtifact.create({
        data: {
          scope: 'community',
          communityId,
          title,
          storageObjectId: storageObject.id,
          storageKey: key,
          format: bundle.format,
          sizeBytes: bytes.byteLength,
          sha256,
          status: 'creating',
          includedResources,
          metadata: input.metadata as never,
        },
      });
      await tx.storageObject.update({ where: { id: storageObject.id }, data: { refId: created.id } });
      return created;
    });

    try {
      await storage.driver.put(key, bytes);
      const updated = await this.prisma.backupArtifact.update({ where: { id: artifact.id }, data: { status: 'created', completedAt: new Date() } });
      if (artifact.storageObjectId) await this.prisma.storageObject.update({ where: { id: artifact.storageObjectId }, data: { status: 'active' } });
      await this.audit(actor, 'backup.create', 'backup_artifact', updated.id, { communityId, key, sizeBytes: bytes.byteLength, includedResources });
      return updated;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await this.prisma.backupArtifact.update({ where: { id: artifact.id }, data: { status: 'failed', error: message } });
      if (artifact.storageObjectId) await this.prisma.storageObject.update({ where: { id: artifact.storageObjectId }, data: { status: 'failed' } });
      throw error;
    }
  }

  async verifyBackup(id: string) {
    const artifact = await this.backup(id);
    const storage = createConfiguredStorage();
    const bytes = await storage.driver.get(artifact.storageKey);
    const objectSha256 = sha256Hex(bytes);
    const bundle = decodeBackupBundle(bytes);
    const payload = verifyBackupBundle(bundle);
    return {
      artifactId: id,
      valid: objectSha256 === artifact.sha256 && payload.valid,
      expectedObjectSha256: artifact.sha256,
      actualObjectSha256: objectSha256,
      payload,
      bundle: { title: bundle.title, createdAt: bundle.createdAt, scope: bundle.scope, resourceId: bundle.resourceId, includedResources: bundle.manifest.includedResources },
    };
  }

  async restoreBackup(id: string, input: RestoreBackupInput, actor: Principal) {
    const artifact = await this.backup(id);
    const targetCommunityId = input.targetCommunityId ?? artifact.communityId;
    if (!targetCommunityId) throw new NotFoundException('Для restore нужен targetCommunityId');
    const run = await this.prisma.backupRestoreRun.create({ data: { artifactId: id, communityId: targetCommunityId, mode: input.apply ? 'apply' : 'dry-run', strategy: input.strategy, status: 'running', actorId: actor.id } });
    const started = Date.now();
    try {
      const storage = createConfiguredStorage();
      const bytes = await storage.driver.get(artifact.storageKey);
      if (sha256Hex(bytes) !== artifact.sha256) throw new Error('Backup object checksum verification failed');
      const bundle = decodeBackupBundle(bytes);
      if (bundle.scope !== 'community' || bundle.resourceId !== artifact.communityId) throw new Error('Backup identity mismatch');
      const verification = verifyBackupBundle(bundle);
      if (!verification.valid) throw new Error('Backup payload checksum verification failed');
      const result = await this.importExport.importCommunity(targetCommunityId, importCommunitySchema.parse({ mode: input.apply ? 'apply' : 'dry-run', data: bundle.payload }), actor);
      const saved = await this.prisma.backupRestoreRun.update({ where: { id: run.id }, data: { status: 'completed', finishedAt: new Date(), durationMs: Date.now() - started, result: { verification, import: result } as never } });
      await this.audit(actor, input.apply ? 'backup.restore.apply' : 'backup.restore.dry_run', 'backup_artifact', id, { targetCommunityId, runId: run.id, strategy: input.strategy });
      return saved;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const saved = await this.prisma.backupRestoreRun.update({ where: { id: run.id }, data: { status: 'failed', finishedAt: new Date(), durationMs: Date.now() - started, error: message } });
      await this.audit(actor, 'backup.restore.failed', 'backup_artifact', id, { targetCommunityId, runId: run.id, error: message });
      return saved;
    }
  }

  restoreRuns(input: { artifactId?: string; communityId?: string; status?: string; take?: number }) {
    return this.prisma.backupRestoreRun.findMany({
      where: { ...(input.artifactId ? { artifactId: input.artifactId } : {}), ...(input.communityId ? { communityId: input.communityId } : {}), ...(input.status ? { status: input.status } : {}) },
      orderBy: { createdAt: 'desc' },
      take: Math.min(input.take ?? 100, 500),
    });
  }

  private audit(actor: Principal, action: string, resource: string, resourceId: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource, resourceId, metadata: metadata as never } });
  }
}
