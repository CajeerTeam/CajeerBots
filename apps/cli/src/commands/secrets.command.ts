import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { decryptSecret, encryptSecret, maskSecret } from '@cajeerbots/security';

export function secretsCommand(): Command {
  const command = new Command('secrets').description('Encrypted secrets store');

  command.command('list').description('Показать список secret refs').action(async () => {
    await withPrisma(async (prisma) => {
      const secrets = await prisma.secret.findMany({ orderBy: { key: 'asc' }, select: { key: true, description: true, version: true, updatedAt: true, rotatedAt: true } });
      console.log(JSON.stringify(secrets.map((secret) => ({ ...secret, ref: `secret:${secret.key}` })), null, 2));
    });
  });

  command.command('set').description('Создать или обновить encrypted secret').argument('<key>').requiredOption('--value <value>', 'значение').option('--description <description>', 'описание').option('--reason <reason>', 'причина изменения').action(async (key: string, options: { value: string; description?: string; reason?: string }) => {
    await withPrisma(async (prisma) => {
      const encrypted = encrypt(options.value);
      const existing = await prisma.secret.findUnique({ where: { key } });
      if (!existing) {
        const created = await prisma.secret.create({ data: { key, description: options.description, ...encrypted } });
        console.log(JSON.stringify({ key: created.key, ref: `secret:${created.key}`, version: created.version, masked: maskSecret(options.value) }, null, 2));
        return;
      }
      await prisma.$transaction(async (tx) => {
        await tx.secretHistory.create({ data: { secretId: existing.id, version: existing.version, algorithm: existing.algorithm, ciphertext: existing.ciphertext, iv: existing.iv, authTag: existing.authTag, actorId: 'cli', reason: options.reason } });
        await tx.secret.update({ where: { id: existing.id }, data: { description: options.description ?? existing.description, ...encrypted, version: { increment: 1 }, rotatedAt: new Date() } });
      });
      const updated = await prisma.secret.findUniqueOrThrow({ where: { key } });
      console.log(JSON.stringify({ key: updated.key, ref: `secret:${updated.key}`, version: updated.version, masked: maskSecret(options.value) }, null, 2));
    });
  });

  command.command('get').description('Расшифровать secret локально').argument('<key>').option('--raw', 'вывести только значение').action(async (key: string, options: { raw?: boolean }) => {
    await withPrisma(async (prisma) => {
      const secret = await prisma.secret.findUnique({ where: { key } });
      if (!secret) throw new Error(`Secret не найден: ${key}`);
      const value = decryptSecret({ algorithm: secret.algorithm as 'aes-256-gcm', ciphertext: secret.ciphertext, iv: secret.iv, authTag: secret.authTag }, encryptionKey());
      if (options.raw) console.log(value);
      else console.log(JSON.stringify({ key, value, masked: maskSecret(value), version: secret.version }, null, 2));
    });
  });

  command.command('delete').description('Удалить secret').argument('<key>').action(async (key: string) => {
    await withPrisma(async (prisma) => {
      await prisma.secret.delete({ where: { key } });
      console.log(JSON.stringify({ deleted: true, key }, null, 2));
    });
  });

  return command;
}

async function withPrisma<T>(fn: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try {
    return await fn(prisma);
  } finally {
    await prisma.close();
  }
}

function encrypt(value: string) {
  return encryptSecret(value, encryptionKey());
}

function encryptionKey(): string {
  const key = process.env.SECRETS_ENCRYPTION_KEY ?? process.env.JWT_SECRET;
  if (!key) throw new Error('SECRETS_ENCRYPTION_KEY или JWT_SECRET должен быть задан');
  return key;
}
