import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { createConfiguredStorage } from '@cajeerbots/backup';

export function storageCommand(): Command {
  const command = new Command('storage').description('Каталог storage objects и lifecycle local/S3 файлов');

  command.command('objects:list').option('--status <status>').option('--ref-type <type>').option('--ref-id <id>').option('--take <n>', 'limit', '100').action(async (options: { status?: string; refType?: string; refId?: string; take: string }) => withPrisma(async (prisma) => {
    const rows = await prisma.storageObject.findMany({ where: { ...(options.status ? { status: options.status } : {}), ...(options.refType ? { refType: options.refType } : {}), ...(options.refId ? { refId: options.refId } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500) });
    console.log(JSON.stringify(rows, null, 2));
  }));

  command.command('objects:get').argument('<id>').action(async (id: string) => withPrisma(async (prisma) => {
    const row = await prisma.storageObject.findUnique({ where: { id } });
    if (!row) throw new Error('Storage object не найден');
    console.log(JSON.stringify(row, null, 2));
  }));

  command.command('objects:delete').argument('<id>').option('--hard', 'удалить физический объект из local/S3').action(async (id: string, options: { hard?: boolean }) => withPrisma(async (prisma) => {
    const object = await prisma.storageObject.findUnique({ where: { id } });
    if (!object) throw new Error('Storage object не найден');
    if (options.hard) await createConfiguredStorage().driver.delete(object.key).catch(() => undefined);
    const row = await prisma.storageObject.update({ where: { id }, data: { status: 'deleted', deletedAt: new Date() } });
    console.log(JSON.stringify(row, null, 2));
  }));

  command.command('diagnose').action(async () => {
    const storage = createConfiguredStorage();
    const key = `.diagnostics/${Date.now()}.txt`;
    const data = new TextEncoder().encode('cajeerbots-storage-diagnostics');
    await storage.driver.put(key, data);
    const read = await storage.driver.get(key);
    await storage.driver.delete(key);
    console.log(JSON.stringify({ ok: new TextDecoder().decode(read) === 'cajeerbots-storage-diagnostics', driver: storage.driverName, bucket: storage.bucket, root: storage.root }, null, 2));
  });

  return command;
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try { return await callback(prisma); } finally { await prisma.close(); }
}
