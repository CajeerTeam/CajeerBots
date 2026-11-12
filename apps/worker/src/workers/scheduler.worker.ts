import { SerialPoller } from '@cajeerbots/core';
import { PrismaService } from '@cajeerbots/db';
import { calculateJobNextRun, executeScheduledJob } from '@cajeerbots/scheduler';

export interface SchedulerWorkerOptions {
  pollIntervalMs?: number;
  batchSize?: number;
  lockSeconds?: number;
}

export class SchedulerWorker {
  private poller?: SerialPoller;
  private running = false;

  constructor(
    private readonly prisma = new PrismaService(),
    private readonly options: SchedulerWorkerOptions = {},
  ) {}

  async start(): Promise<void> {
    await this.prisma.connect();
    this.poller = new SerialPoller(this.options.pollIntervalMs ?? Number(process.env.SCHEDULER_WORKER_INTERVAL_MS ?? 10000), () => this.tick(), (error) => console.error('[worker] scheduler.worker tick failed:', error instanceof Error ? error.name : 'unknown'));
    await this.poller.start();
    console.log('[worker] scheduler worker 1.0.0-rc.4 started');
  }

  async stop(): Promise<void> {
    await this.poller?.stop();
    await this.prisma.close();
  }

  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const now = new Date();
      const jobs = await this.prisma.scheduledJob.findMany({
        where: { isEnabled: true, nextRunAt: { lte: now }, OR: [{ lockUntil: null }, { lockUntil: { lt: now } }] },
        orderBy: { nextRunAt: 'asc' },
        take: this.options.batchSize ?? Number(process.env.SCHEDULER_WORKER_BATCH_SIZE ?? 10),
      });
      for (const job of jobs) await this.runJob(job.id);
    } finally {
      this.running = false;
    }
  }

  async runJob(jobId: string): Promise<void> {
    const now = new Date();
    const lockUntil = new Date(now.getTime() + (this.options.lockSeconds ?? Number(process.env.SCHEDULER_LOCK_SECONDS ?? 600)) * 1000);
    const locked = await this.prisma.scheduledJob.updateMany({ where: { id: jobId, isEnabled: true, nextRunAt: { lte: now }, OR: [{ lockUntil: null }, { lockUntil: { lt: now } }] }, data: { lockUntil } });
    if (locked.count === 0) return;

    const job = await this.prisma.scheduledJob.findUniqueOrThrow({ where: { id: jobId } });
    const lastFailed = await this.prisma.scheduledJobRun.count({ where: { jobId, status: 'failed', createdAt: { gt: new Date(Date.now() - 86_400_000) } } });
    if (lastFailed >= job.maxAttempts) {
      const nextRunAt = calculateJobNextRun(job, now);
      await this.prisma.scheduledJob.update({ where: { id: jobId }, data: { lockUntil: null, nextRunAt } });
      await this.prisma.scheduledJobRun.create({ data: { jobId, status: 'skipped', attempt: lastFailed + 1, triggeredBy: 'scheduler', startedAt: now, finishedAt: new Date(), error: `Превышен суточный лимит неуспешных попыток: ${job.maxAttempts}` } });
      return;
    }

    const run = await this.prisma.scheduledJobRun.create({ data: { jobId, status: 'running', attempt: lastFailed + 1, triggeredBy: 'scheduler' } });
    const started = Date.now();
    let status: 'succeeded' | 'failed' | 'skipped' = 'failed';
    let result: Record<string, unknown> | undefined;
    let error: string | undefined;
    let logs: Array<Record<string, unknown>> | undefined;
    try {
      const execution = await executeScheduledJob({ prisma: this.prisma, job, runId: run.id, triggeredBy: 'scheduler', now });
      status = execution.status;
      result = execution.result;
      error = execution.error;
      logs = execution.logs;
    } catch (caught) {
      status = 'failed';
      error = caught instanceof Error ? caught.message : String(caught);
    }

    const finishedAt = new Date();
    await this.prisma.$transaction([
      this.prisma.scheduledJobRun.update({ where: { id: run.id }, data: { status, finishedAt, durationMs: Date.now() - started, result: result as never, error, logs: logs as never } }),
      this.prisma.scheduledJob.update({ where: { id: jobId }, data: { lockUntil: null, lastRunAt: finishedAt, nextRunAt: job.isEnabled ? calculateJobNextRun(job, finishedAt) : null } }),
      this.prisma.auditLog.create({ data: { actorId: 'scheduler', actorType: 'system', action: 'scheduler.job.completed', resource: 'scheduled_job', resourceId: jobId, metadata: { code: job.code, status, error } as never } }),
    ]);
  }
}
