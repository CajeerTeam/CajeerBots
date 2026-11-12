import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { encryptSecret, maskSecret, decryptSecret } from '@cajeerbots/security';

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

  command.command('sync-commands').argument('<accountId>').option('--guild <id>', 'Discord guild ID (otherwise global)').description('Publish enabled command list to Telegram/Discord; replaces remote command list').action(async (id: string, o: { guild?: string }) => {
    const prisma = new PrismaService();
    try {
      const account = await prisma.botAccount.findUniqueOrThrow({ where: { id } });
      if (!account.isActive || !account.tokenRef) throw new Error('Active bot with tokenRef required');
      if (account.provider === 'vk') throw new Error('VK commands use message text and keyboard callbacks; no remote command registration');
      let token: string | undefined;
      if (account.tokenRef.startsWith('env:')) token = process.env[account.tokenRef.slice(4)];
      else if (account.tokenRef.startsWith('secret:')) { const secret = await prisma.secret.findUniqueOrThrow({ where: { key: account.tokenRef.slice(7) } }); if (secret.algorithm !== 'aes-256-gcm') throw new Error('Unsupported token encryption'); token = decryptSecret({ ...secret, algorithm: 'aes-256-gcm' }, encryptionKey()); }
      if (!token) throw new Error('Bot token is unavailable');
      const configured = await prisma.interactionCommand.findMany({ where: { communityId: account.communityId, isEnabled: true, OR: [{ provider: account.provider }, { provider: null }] }, orderBy: { name: 'asc' } });
      const commands = new Map<string, string>([['help', 'Помощь'], ['status', 'Статус'], ['settings', 'Настройки'], ['answer', 'Ответ на вопрос'], ['cancel', 'Отменить диалог']]);
      for (const entry of configured.sort((a, b) => Number(a.provider === account.provider) - Number(b.provider === account.provider))) {
        if (!/^[a-z0-9_]{1,32}$/.test(entry.name)) throw new Error(`Unsupported provider command name: ${entry.name}`);
        commands.set(entry.name, entry.description.slice(0, 100));
      }
      if (commands.size > 100) throw new Error('Provider command limit exceeded');
      let response: Response;
      if (account.provider === 'telegram') {
        response = await fetch(`https://api.telegram.org/bot${token}/setMyCommands`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ commands: [...commands].map(([command, description]) => ({ command, description })) }), signal: AbortSignal.timeout(10000), redirect: 'error' }).catch(() => { throw new Error('Telegram command synchronization failed'); });
        const body = await response.json() as { ok?: boolean };
        if (!response.ok || !body.ok) throw new Error(`Telegram command synchronization failed: ${response.status}`);
      } else {
        const headers = { authorization: `Bot ${token}`, 'content-type': 'application/json' };
        const app = await fetch('https://discord.com/api/v10/oauth2/applications/@me', { headers, signal: AbortSignal.timeout(10000), redirect: 'error' });
        if (!app.ok) throw new Error(`Discord application lookup failed: ${app.status}`);
        const { id: appId } = await app.json() as { id: string };
        if (!/^\d+$/.test(appId) || (o.guild && !/^\d+$/.test(o.guild))) throw new Error('Invalid Discord application or guild ID');
        const endpoint = `https://discord.com/api/v10/applications/${appId}${o.guild ? `/guilds/${o.guild}` : ''}/commands`;
        response = await fetch(endpoint, { method: 'PUT', headers, body: JSON.stringify([...commands].map(([name, description]) => ({ name, description, type: 1, ...(name === 'answer' ? { options: [{ type: 3, name: 'value', description: 'Ответ', required: true }] } : {}) }))), signal: AbortSignal.timeout(10000), redirect: 'error' });
        if (!response.ok) throw new Error(`Discord command synchronization failed: ${response.status}`);
      }
      console.log(JSON.stringify({ synced: true, provider: account.provider, count: commands.size }));
    } finally { await prisma.$disconnect(); }
  });

  return command;
}

function encryptionKey(): string {
  const key = process.env.SECRETS_ENCRYPTION_KEY ?? process.env.JWT_SECRET;
  if (!key) throw new Error('SECRETS_ENCRYPTION_KEY или JWT_SECRET должен быть задан');
  return key;
}
