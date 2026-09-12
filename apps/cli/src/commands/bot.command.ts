import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { encryptSecret, maskSecret } from '@cajeerbots/security';

const providers = new Set(['telegram', 'vk', 'discord']);

export function botCommand(): Command {
  const command = new Command('bot').description('Управление bot accounts');

  command
    .command('add')
    .argument('<provider>', 'telegram | vk | discord')
    .requiredOption('--community <id>', 'community id')
    .option('--mode <mode>', 'polling | webhook', 'polling')
    .option('--external-id <id>', 'external provider id')
    .option('--username <username>', 'bot username')
    .option('--token-ref <ref>', 'env/secret reference')
    .option('--token <token>', 'сохранить токен в encrypted secrets store и привязать как tokenRef')
    .action(async (provider: string, options: { community: string; mode: string; externalId?: string; username?: string; tokenRef?: string; token?: string }) => {
      if (!providers.has(provider)) throw new Error(`Неизвестный provider: ${provider}`);
      if (!['polling', 'webhook'].includes(options.mode)) throw new Error(`Некорректный mode: ${options.mode}`);
      const prisma = new PrismaService();
      await prisma.connect();
      try {
        let tokenRef = options.tokenRef;
        if (options.token) {
          const key = `bot.${options.community}.${provider}.${options.externalId ?? options.username ?? 'default'}`;
          const encrypted = encryptSecret(options.token, encryptionKey());
          await prisma.secret.upsert({
            where: { key },
            create: { key, description: `Bot token ${provider}`, ...encrypted },
            update: { ...encrypted, version: { increment: 1 }, rotatedAt: new Date() },
          });
          tokenRef = `secret:${key}`;
        }
        const account = await prisma.botAccount.create({
          data: {
            communityId: options.community,
            provider: provider as never,
            mode: options.mode as never,
            externalId: options.externalId,
            username: options.username,
            tokenRef,
          },
        });
        console.log(JSON.stringify({ ...account, tokenStored: Boolean(options.token), tokenMasked: options.token ? maskSecret(options.token) : undefined }, null, 2));
      } finally {
        await prisma.close();
      }
    });

  command.command('list').requiredOption('--community <id>', 'community id').action(async (options: { community: string }) => {
    const prisma = new PrismaService();
    await prisma.connect();
    try {
      const accounts = await prisma.botAccount.findMany({ where: { communityId: options.community }, orderBy: { createdAt: 'desc' } });
      console.log(JSON.stringify(accounts, null, 2));
    } finally {
      await prisma.close();
    }
  });

  return command;
}

function encryptionKey(): string {
  const key = process.env.SECRETS_ENCRYPTION_KEY ?? process.env.JWT_SECRET;
  if (!key) throw new Error('SECRETS_ENCRYPTION_KEY или JWT_SECRET должен быть задан');
  return key;
}
