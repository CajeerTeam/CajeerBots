import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';

export function runtimeCommand(): Command {
  const command = new Command('runtime').description('Диагностика bot-runtime: status, inbound updates, outbound messages');

  command.command('status')
    .requiredOption('--community <id>', 'community id')
    .option('--account <id>', 'bot account id')
    .action(async (options: { community: string; account?: string }) => {
      await withPrisma(async (prisma) => {
        const accounts = await prisma.botAccount.findMany({ where: { communityId: options.community, ...(options.account ? { id: options.account } : {}) }, orderBy: { createdAt: 'asc' } });
        const statuses = await prisma.botRuntimeStatus.findMany({ where: { accountId: { in: accounts.map((account) => account.id) } }, orderBy: { lastBeatAt: 'desc' }, take: 200 });
        const latest = new Map<string, (typeof statuses)[number]>();
        for (const status of statuses) if (!latest.has(status.accountId)) latest.set(status.accountId, status);
        console.log(JSON.stringify(accounts.map((account) => ({ accountId: account.id, provider: account.provider, mode: account.mode, isActive: account.isActive, runtime: latest.get(account.id) ?? null })), null, 2));
      });
    });

  command.command('inbound:list')
    .requiredOption('--community <id>', 'community id')
    .option('--account <id>', 'bot account id')
    .option('--status <status>', 'received | handled | ignored | failed')
    .option('--take <n>', 'limit', '50')
    .action(async (options: { community: string; account?: string; status?: string; take: string }) => {
      await withPrisma(async (prisma) => {
        const rows = await prisma.botInboundUpdate.findMany({ where: { communityId: options.community, ...(options.account ? { accountId: options.account } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { receivedAt: 'desc' }, take: Math.min(Number(options.take), 500) });
        console.log(JSON.stringify(rows, null, 2));
      });
    });

  command.command('outbound:list')
    .requiredOption('--community <id>', 'community id')
    .option('--account <id>', 'bot account id')
    .option('--status <status>', 'pending | sending | sent | failed | dead')
    .option('--take <n>', 'limit', '50')
    .action(async (options: { community: string; account?: string; status?: string; take: string }) => {
      await withPrisma(async (prisma) => {
        const rows = await prisma.botOutboundMessage.findMany({ where: { communityId: options.community, ...(options.account ? { accountId: options.account } : {}), ...(options.status ? { status: options.status as never } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500) });
        console.log(JSON.stringify(rows, null, 2));
      });
    });

  command.command('outbound:retry')
    .requiredOption('--id <id>', 'outbound message id')
    .option('--reset-attempts', 'обнулить счётчик попыток')
    .action(async (options: { id: string; resetAttempts?: boolean }) => {
      await withPrisma(async (prisma) => {
        const updated = await prisma.botOutboundMessage.update({ where: { id: options.id }, data: { status: 'pending', availableAt: new Date(), lastError: null, ...(options.resetAttempts ? { attempts: 0 } : {}) } });
        console.log(JSON.stringify(updated, null, 2));
      });
    });

  command.command('webhook-url')
    .requiredOption('--account <id>', 'bot account id')
    .action(async (options: { account: string }) => {
      await withPrisma(async (prisma) => {
        const account = await prisma.botAccount.findUniqueOrThrow({ where: { id: options.account } });
        const base = process.env.BOT_WEBHOOK_PUBLIC_URL ?? process.env.APP_PUBLIC_URL ?? '';
        const path = `/runtime/webhooks/${account.provider}/${account.id}`;
        console.log(JSON.stringify({ accountId: account.id, provider: account.provider, url: base ? new URL(path, base).toString() : path }, null, 2));
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
