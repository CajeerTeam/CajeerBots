import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { recordDebugEvent } from '@cajeerbots/devtools';

export function devCommand(): Command {
  return new Command('dev')
    .description('Запустить dev-mode диагностику без внешней веб-панели')
    .option('--community <id>', 'community id')
    .option('--session-title <title>', 'название debug session', 'CajeerBots dev mode')
    .option('--no-session', 'не создавать debug session')
    .action(async (options: { community?: string; sessionTitle: string; session?: boolean }) => {
      await withPrisma(async (prisma) => {
        const [communities, accounts, jobs, deadOutbox, deadEvents] = await Promise.all([
          prisma.community.count(),
          prisma.botAccount.count({ where: options.community ? { communityId: options.community } : {} }),
          prisma.scheduledJob.count({ where: { isEnabled: true } }),
          prisma.outboxMessage.count({ where: { status: 'dead' } }),
          prisma.eventSubscriptionDelivery.count({ where: { status: 'dead' } }),
        ]);
        let session = null;
        if (options.session !== false) {
          session = await prisma.debugSession.create({ data: { communityId: options.community ?? null, title: options.sessionTitle, actorId: 'cli', actorType: 'system', metadata: { command: 'cajeer dev' } as never } });
          await recordDebugEvent(prisma, { sessionId: session.id, communityId: options.community, category: 'dev.mode', message: 'Dev mode diagnostics created', metadata: { communities, accounts, jobs, deadOutbox, deadEvents } });
        }
        console.log(JSON.stringify({ ok: true, sessionId: session?.id, diagnostics: { communities, accounts, activeScheduledJobs: jobs, deadOutbox, deadEventDeliveries: deadEvents } }, null, 2));
      });
    });
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
