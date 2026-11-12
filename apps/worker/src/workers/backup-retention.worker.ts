import { SerialPoller } from '@cajeerbots/core';
import { PrismaService } from '@cajeerbots/db';
import { createConfiguredStorage } from '@cajeerbots/backup';

export class BackupRetentionWorker {
  private running = false;
  private poller?: SerialPoller;
  private readonly intervalMs = Number(process.env.BACKUP_WORKER_INTERVAL_MS ?? 60_000);

  async start(): Promise<void> {
    this.running = true;
    this.poller = new SerialPoller(this.intervalMs, () => this.tick(), (error) => console.error('[worker] backup-retention.worker tick failed:', error instanceof Error ? error.name : 'unknown'));
    await this.poller.start();
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.poller?.stop();
  }

  private async tick(): Promise<void> {
    if (!this.running) return;
    const prisma = new PrismaService();
    await prisma.connect();
    try {
      const retentionDays = Number(process.env.BACKUP_DELETED_OBJECT_RETENTION_DAYS ?? 7);
      const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000);
      const rows = await prisma.storageObject.findMany({ where: { OR: [{ status: 'deleted', deletedAt: { lte: cutoff } }, { status: 'purge_pending' }] }, orderBy: { deletedAt: 'asc' }, take: 25 });
      if (rows.length === 0) return;
      const storage = createConfiguredStorage();
      for (const object of rows) {
        try {
          await storage.driver.delete(object.key);
          await prisma.storageObject.update({ where: { id: object.id }, data: { status: 'purged' } });
        } catch (error) {
          await prisma.auditLog.create({ data: { actorId: 'backup-retention-worker', actorType: 'system', action: 'storage.object.purge_failed', resource: 'storage_object', resourceId: object.id, metadata: { key: object.key, error: error instanceof Error ? error.message : String(error) } } });
        }
      }
    } finally {
      await prisma.close();
    }
  }
}
