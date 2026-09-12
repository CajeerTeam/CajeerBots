import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { calculateJobNextRun, executeScheduledJob } from '@cajeerbots/scheduler';

export function jobsCommand(): Command {
  const command = new Command('jobs').description('DB-backed scheduler, maintenance tasks и execution history');

  command.command('list')
    .option('--enabled <bool>', 'true/false')
    .option('--type <type>', 'built_in | event_publish | http')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { enabled?: string; type?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.scheduledJob.findMany({
        where: { ...(options.enabled !== undefined ? { isEnabled: options.enabled === 'true' } : {}), ...(options.type ? { type: options.type } : {}) },
        orderBy: [{ isEnabled: 'desc' }, { nextRunAt: 'asc' }, { code: 'asc' }],
        take: Math.min(Number(options.take), 500),
        include: { _count: { select: { runs: true } } },
      });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('get').argument('<idOrCode>').action(async (idOrCode: string) => withPrisma(async (prisma) => {
    const job = await prisma.scheduledJob.findFirst({ where: { OR: [{ id: idOrCode }, { code: idOrCode }] }, include: { runs: { orderBy: { createdAt: 'desc' }, take: 20 } } });
    if (!job) throw new Error('Scheduled job не найден');
    console.log(JSON.stringify(job, null, 2));
  }));

  command.command('set')
    .requiredOption('--code <code>')
    .requiredOption('--title <title>')
    .option('--type <type>', 'built_in | event_publish | http', 'built_in')
    .option('--handler <handler>')
    .option('--schedule <schedule>', 'manual | every:5m | interval:1h | cron:*/5 * * * *', 'manual')
    .option('--timezone <tz>', 'timezone', 'UTC')
    .option('--payload <json>', 'payload JSON')
    .option('--payload-file <path>', 'payload JSON file')
    .option('--max-attempts <n>', 'max attempts', '3')
    .option('--timeout <seconds>', 'timeout seconds', '300')
    .option('--disabled')
    .action(async (options: { code: string; title: string; type: string; handler?: string; schedule: string; timezone: string; payload?: string; payloadFile?: string; maxAttempts: string; timeout: string; disabled?: boolean }) => withPrisma(async (prisma) => {
      const payload = await readJsonPayload(options.payload, options.payloadFile);
      const nextRunAt = options.disabled ? null : calculateJobNextRun({ schedule: options.schedule }, new Date());
      const job = await prisma.scheduledJob.upsert({
        where: { code: options.code },
        create: { code: options.code, title: options.title, type: options.type, handler: options.handler, schedule: options.schedule, timezone: options.timezone, payload: payload as never, isEnabled: !options.disabled, maxAttempts: Number(options.maxAttempts), timeoutSeconds: Number(options.timeout), nextRunAt },
        update: { title: options.title, type: options.type, handler: options.handler, schedule: options.schedule, timezone: options.timezone, payload: payload as never, isEnabled: !options.disabled, maxAttempts: Number(options.maxAttempts), timeoutSeconds: Number(options.timeout), nextRunAt },
      });
      console.log(JSON.stringify(job, null, 2));
    }));

  command.command('enable').argument('<idOrCode>').action(async (idOrCode: string) => setEnabled(idOrCode, true));
  command.command('disable').argument('<idOrCode>').action(async (idOrCode: string) => setEnabled(idOrCode, false));

  command.command('run').argument('<idOrCode>').option('--payload <json>').option('--payload-file <path>').action(async (idOrCode: string, options: { payload?: string; payloadFile?: string }) => withPrisma(async (prisma) => {
    const job = await prisma.scheduledJob.findFirst({ where: { OR: [{ id: idOrCode }, { code: idOrCode }] } });
    if (!job) throw new Error('Scheduled job не найден');
    const payloadPatch = await readJsonPayload(options.payload, options.payloadFile);
    const payload = payloadPatch ? { ...(job.payload as Record<string, unknown> | null ?? {}), ...payloadPatch } : job.payload;
    const run = await prisma.scheduledJobRun.create({ data: { jobId: job.id, status: 'running', triggeredBy: 'cli' } });
    const started = Date.now();
    const result = await executeScheduledJob({ prisma, job: { ...job, payload }, runId: run.id, triggeredBy: 'cli', now: new Date() });
    const finishedAt = new Date();
    const saved = await prisma.scheduledJobRun.update({ where: { id: run.id }, data: { status: result.status, finishedAt, durationMs: Date.now() - started, result: result.result as never, error: result.error, logs: result.logs as never }, include: { job: true } });
    await prisma.scheduledJob.update({ where: { id: job.id }, data: { lastRunAt: finishedAt, nextRunAt: job.isEnabled ? calculateJobNextRun(job, finishedAt) : null, lockUntil: null } });
    console.log(JSON.stringify(saved, null, 2));
  }));

  command.command('runs:list').option('--job <id>').option('--status <status>').option('--take <n>', 'limit', '100').action(async (options: { job?: string; status?: string; take: string }) => withPrisma(async (prisma) => {
    const rows = await prisma.scheduledJobRun.findMany({ where: { ...(options.job ? { jobId: options.job } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500), include: { job: true } });
    console.log(JSON.stringify(rows, null, 2));
  }));

  command.command('queue-summary').action(async () => withPrisma(async (prisma) => {
    const [scheduledDue, outboxPending, eventPending, extensionPending, botPending, deadLetters] = await Promise.all([
      prisma.scheduledJob.count({ where: { isEnabled: true, nextRunAt: { lte: new Date() } } }),
      prisma.outboxMessage.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
      prisma.eventSubscriptionDelivery.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
      prisma.extensionRuntimeEvent.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
      prisma.botOutboundMessage.count({ where: { status: { in: ['pending', 'failed', 'sending'] } } }),
      prisma.eventDeadLetter.count({ where: { status: 'open' } }),
    ]);
    console.log(JSON.stringify({ scheduledDue, outboxPending, eventPending, extensionPending, botPending, deadLetters }, null, 2));
  }));

  command.command('snapshots:list').option('--kind <kind>').option('--status <status>').option('--take <n>', 'limit', '100').action(async (options: { kind?: string; status?: string; take: string }) => withPrisma(async (prisma) => {
    const rows = await prisma.maintenanceSnapshot.findMany({ where: { ...(options.kind ? { kind: options.kind } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500) });
    console.log(JSON.stringify(rows, null, 2));
  }));

  return command;
}

async function setEnabled(idOrCode: string, enabled: boolean): Promise<void> {
  await withPrisma(async (prisma) => {
    const job = await prisma.scheduledJob.findFirst({ where: { OR: [{ id: idOrCode }, { code: idOrCode }] } });
    if (!job) throw new Error('Scheduled job не найден');
    const nextRunAt = enabled ? calculateJobNextRun(job, new Date()) : null;
    const updated = await prisma.scheduledJob.update({ where: { id: job.id }, data: { isEnabled: enabled, nextRunAt, lockUntil: null } });
    console.log(JSON.stringify(updated, null, 2));
  });
}

async function readJsonPayload(payload?: string, payloadFile?: string): Promise<Record<string, unknown> | undefined> {
  if (payloadFile) {
    const { readFile } = await import('node:fs/promises');
    return JSON.parse(await readFile(payloadFile, 'utf8')) as Record<string, unknown>;
  }
  return payload ? JSON.parse(payload) as Record<string, unknown> : undefined;
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try {
    return await callback(prisma);
  } finally {
    await prisma.close();
  }
}
