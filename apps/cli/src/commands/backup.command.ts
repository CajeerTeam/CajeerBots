import { readFile, writeFile } from 'node:fs/promises';
import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { createBackupBundle, createConfiguredStorage, decodeBackupBundle, encodeBackupBundle, exportCommunitySnapshot, includedResourcesFromCommunityPayload, sha256Hex, verifyBackupBundle, restoreCommunitySnapshot } from '@cajeerbots/backup';

export function backupCommand(): Command {
  const command = new Command('backup').description('Backup/restore artifacts для community через local/S3 storage');

  command.command('list').option('--community <id>').option('--status <status>').option('--take <n>', 'limit', '100').action(async (options: { community?: string; status?: string; take: string }) => withPrisma(async (prisma) => {
    const rows = await prisma.backupArtifact.findMany({ where: { ...(options.community ? { communityId: options.community } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500) });
    console.log(JSON.stringify(rows, null, 2));
  }));

  command.command('create').requiredOption('--community <id>').option('--title <title>').option('--key <storageKey>').option('--metadata <json>').action(async (options: { community: string; title?: string; key?: string; metadata?: string }) => withPrisma(async (prisma) => {
    const payload = await exportCommunitySnapshot(prisma, options.community);
    const includedResources = includedResourcesFromCommunityPayload(payload);
    const title = options.title ?? `Backup ${payload.community.slug} ${new Date().toISOString()}`;
    const bundle = createBackupBundle({ scope: 'community', resourceId: options.community, title, payload, includedResources, metadata: options.metadata ? JSON.parse(options.metadata) as Record<string, unknown> : undefined });
    const bytes = encodeBackupBundle(bundle);
    const sha256 = sha256Hex(bytes);
    const storage = createConfiguredStorage();
    const key = options.key ?? `${process.env.BACKUP_OBJECT_PREFIX ?? 'backups'}/community/${options.community}/${new Date().toISOString().replace(/[:.]/g, '-')}.cajeer-backup.json`;
    const storageObject = await prisma.storageObject.create({ data: { key, driver: storage.driverName, bucket: storage.bucket, contentType: 'application/vnd.cajeerbots.backup+json', sizeBytes: bytes.byteLength, sha256, refType: 'backup_artifact', status: 'uploading', metadata: { root: storage.root, resources: includedResources } as never } });
    const artifact = await prisma.backupArtifact.create({ data: { scope: 'community', communityId: options.community, title, storageObjectId: storageObject.id, storageKey: key, format: bundle.format, sizeBytes: bytes.byteLength, sha256, status: 'creating', includedResources, metadata: options.metadata ? JSON.parse(options.metadata) as never : undefined } });
    await prisma.storageObject.update({ where: { id: storageObject.id }, data: { refId: artifact.id } });
    try {
      await storage.driver.put(key, bytes);
      const saved = await prisma.backupArtifact.update({ where: { id: artifact.id }, data: { status: 'created', completedAt: new Date() } });
      await prisma.storageObject.update({ where: { id: storageObject.id }, data: { status: 'active' } });
      console.log(JSON.stringify(saved, null, 2));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await prisma.backupArtifact.update({ where: { id: artifact.id }, data: { status: 'failed', error: message } });
      await prisma.storageObject.update({ where: { id: storageObject.id }, data: { status: 'failed' } });
      throw error;
    }
  }));

  command.command('get').argument('<id>').action(async (id: string) => withPrisma(async (prisma) => {
    const row = await prisma.backupArtifact.findUnique({ where: { id } });
    if (!row) throw new Error('Backup artifact не найден');
    console.log(JSON.stringify(row, null, 2));
  }));

  command.command('verify').argument('<id>').action(async (id: string) => withPrisma(async (prisma) => {
    const artifact = await prisma.backupArtifact.findUnique({ where: { id } });
    if (!artifact) throw new Error('Backup artifact не найден');
    const storage = createConfiguredStorage();
    const bytes = await storage.driver.get(artifact.storageKey);
    const actualObjectSha256 = sha256Hex(bytes);
    const bundle = decodeBackupBundle(bytes);
    const payload = verifyBackupBundle(bundle);
    console.log(JSON.stringify({ artifactId: id, valid: artifact.sha256 === actualObjectSha256 && payload.valid, expectedObjectSha256: artifact.sha256, actualObjectSha256, payload }, null, 2));
  }));

  command.command('download').argument('<id>').requiredOption('--output <file>').action(async (id: string, options: { output: string }) => withPrisma(async (prisma) => {
    const artifact = await prisma.backupArtifact.findUnique({ where: { id } });
    if (!artifact) throw new Error('Backup artifact не найден');
    const storage = createConfiguredStorage();
    await writeFile(options.output, await storage.driver.get(artifact.storageKey));
    console.log(JSON.stringify({ downloaded: true, id, output: options.output }, null, 2));
  }));

  command.command('upload').requiredOption('--input <file>').option('--title <title>').option('--community <id>').option('--key <storageKey>').action(async (options: { input: string; title?: string; community?: string; key?: string }) => withPrisma(async (prisma) => {
    const bytes = new Uint8Array(await readFile(options.input));
    const bundle = decodeBackupBundle(bytes);
    const storage = createConfiguredStorage();
    const sha256 = sha256Hex(bytes);
    const key = options.key ?? `${process.env.BACKUP_OBJECT_PREFIX ?? 'backups'}/uploaded/${new Date().toISOString().replace(/[:.]/g, '-')}.cajeer-backup.json`;
    await storage.driver.put(key, bytes);
    const storageObject = await prisma.storageObject.create({ data: { key, driver: storage.driverName, bucket: storage.bucket, contentType: 'application/vnd.cajeerbots.backup+json', sizeBytes: bytes.byteLength, sha256, refType: 'backup_artifact', status: 'active' } });
    const artifact = await prisma.backupArtifact.create({ data: { scope: bundle.scope, communityId: options.community ?? bundle.resourceId, title: options.title ?? bundle.title, storageObjectId: storageObject.id, storageKey: key, format: bundle.format, sizeBytes: bytes.byteLength, sha256, status: 'created', includedResources: bundle.manifest.includedResources, completedAt: new Date() } });
    await prisma.storageObject.update({ where: { id: storageObject.id }, data: { refId: artifact.id } });
    console.log(JSON.stringify(artifact, null, 2));
  }));

  command.command('runs:list').option('--artifact <id>').option('--community <id>').option('--status <status>').option('--take <n>', 'limit', '100').action(async (options: { artifact?: string; community?: string; status?: string; take: string }) => withPrisma(async (prisma) => {
    const rows = await prisma.backupRestoreRun.findMany({ where: { ...(options.artifact ? { artifactId: options.artifact } : {}), ...(options.community ? { communityId: options.community } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500) });
    console.log(JSON.stringify(rows, null, 2));
  }));

  command.command('restore').argument('<id>').requiredOption('--community <id>').option('--apply', 'применить restore; без флага dry-run').action(async (id: string, options: { community: string; apply?: boolean }) => withPrisma(async (prisma) => {
    const artifact = await prisma.backupArtifact.findUnique({ where: { id } });
    if (!artifact) throw new Error('Backup artifact не найден');
    const run = await prisma.backupRestoreRun.create({ data: { artifactId: id, communityId: options.community, mode: options.apply ? 'apply' : 'dry-run', strategy: 'merge', status: 'running' } });
    const started = Date.now();
    try {
      const storage = createConfiguredStorage();
      const bundle = decodeBackupBundle(await storage.driver.get(artifact.storageKey));
      const verification = verifyBackupBundle(bundle);
      const restore = await restoreCommunitySnapshot(prisma, options.community, bundle.payload, options.apply ? 'apply' : 'dry-run');
      const saved = await prisma.backupRestoreRun.update({ where: { id: run.id }, data: { status: 'completed', finishedAt: new Date(), durationMs: Date.now() - started, result: { verification, restore } as never } });
      console.log(JSON.stringify(saved, null, 2));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const saved = await prisma.backupRestoreRun.update({ where: { id: run.id }, data: { status: 'failed', finishedAt: new Date(), durationMs: Date.now() - started, error: message } });
      console.log(JSON.stringify(saved, null, 2));
    }
  }));

  return command;
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try { return await callback(prisma); } finally { await prisma.close(); }
}
