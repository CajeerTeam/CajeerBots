import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { calculateJobNextRun, executeScheduledJob } from '@cajeerbots/scheduler';
import type { Principal } from '@cajeerbots/security';

@Injectable()
export class JobsService {
  constructor(private readonly prisma: PrismaService) {}

  list(query: { enabled?: boolean; type?: string; q?: string; take: number }) {
    return this.prisma.scheduledJob.findMany({
      where: {
        ...(query.enabled === undefined ? {} : { isEnabled: query.enabled }),
        ...(query.type ? { type: query.type } : {}),
        ...(query.q ? { OR: [{ code: { contains: query.q, mode: 'insensitive' as const } }, { title: { contains: query.q, mode: 'insensitive' as const } }, { handler: { contains: query.q, mode: 'insensitive' as const } }] } : {}),
      },
      orderBy: [{ isEnabled: 'desc' }, { nextRunAt: 'asc' }, { code: 'asc' }],
      take: query.take,
      include: { _count: { select: { runs: true } } },
    });
  }

  async get(idOrCode: string) {
    const job = await this.prisma.scheduledJob.findFirst({ where: { OR: [{ id: idOrCode }, { code: idOrCode }] }, include: { runs: { orderBy: { createdAt: 'desc' }, take: 20 } } });
    if (!job) throw new NotFoundException('Scheduled job не найден');
    return job;
  }

  async upsert(input: { code: string; title: string; type: string; handler?: string; schedule: string; timezone: string; payload?: Record<string, unknown>; isEnabled: boolean; maxAttempts: number; timeoutSeconds: number }, actor: Principal) {
    const now = new Date();
    const nextRun = input.isEnabled ? calculateJobNextRun({ schedule: input.schedule }, now) : null;
    const job = await this.prisma.scheduledJob.upsert({
      where: { code: input.code },
      create: { ...input, payload: input.payload as never, nextRunAt: nextRun },
      update: { title: input.title, type: input.type, handler: input.handler, schedule: input.schedule, timezone: input.timezone, payload: input.payload as never, isEnabled: input.isEnabled, maxAttempts: input.maxAttempts, timeoutSeconds: input.timeoutSeconds, nextRunAt: nextRun },
    });
    await this.audit(actor, 'scheduler.job.upsert', job.id, { code: job.code, schedule: job.schedule, nextRunAt: job.nextRunAt });
    return job;
  }

  async setEnabled(idOrCode: string, enabled: boolean, actor: Principal) {
    const job = await this.requireJob(idOrCode);
    const nextRun = enabled ? calculateJobNextRun(job, new Date()) : null;
    const updated = await this.prisma.scheduledJob.update({ where: { id: job.id }, data: { isEnabled: enabled, nextRunAt: nextRun, lockUntil: null } });
    await this.audit(actor, enabled ? 'scheduler.job.enable' : 'scheduler.job.disable', job.id, { code: job.code });
    return updated;
  }

  async runNow(idOrCode: string, input: { resetLock?: boolean; payloadPatch?: Record<string, unknown> }, actor: Principal) {
    const job = await this.requireJob(idOrCode);
    if (input.resetLock) await this.prisma.scheduledJob.update({ where: { id: job.id }, data: { lockUntil: null } });
    const payload = input.payloadPatch ? { ...(job.payload as Record<string, unknown> | null ?? {}), ...input.payloadPatch } : job.payload;
    const run = await this.prisma.scheduledJobRun.create({ data: { jobId: job.id, status: 'running', attempt: 1, triggeredBy: actor.id } });
    const started = Date.now();
    const result = await executeScheduledJob({ prisma: this.prisma, job: { ...job, payload }, runId: run.id, triggeredBy: actor.id, now: new Date() });
    const finishedAt = new Date();
    await this.prisma.$transaction([
      this.prisma.scheduledJobRun.update({ where: { id: run.id }, data: { status: result.status, finishedAt, durationMs: Date.now() - started, result: result.result as never, error: result.error, logs: result.logs as never } }),
      this.prisma.scheduledJob.update({ where: { id: job.id }, data: { lastRunAt: finishedAt, nextRunAt: job.isEnabled ? calculateJobNextRun(job, finishedAt) : null, lockUntil: null } }),
      this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action: 'scheduler.job.run_now', resource: 'scheduled_job', resourceId: job.id, metadata: { code: job.code, status: result.status, error: result.error } as never } }),
    ]);
    return this.prisma.scheduledJobRun.findUniqueOrThrow({ where: { id: run.id }, include: { job: true } });
  }

  runs(query: { jobId?: string; status?: string; take: number }) {
    return this.prisma.scheduledJobRun.findMany({
      where: { ...(query.jobId ? { jobId: query.jobId } : {}), ...(query.status ? { status: query.status } : {}) },
      orderBy: { createdAt: 'desc' },
      take: query.take,
      include: { job: true },
    });
  }

  snapshots(query: { kind?: string; status?: string; take: number }) {
    return this.prisma.maintenanceSnapshot.findMany({
      where: { ...(query.kind ? { kind: query.kind } : {}), ...(query.status ? { status: query.status } : {}) },
      orderBy: { createdAt: 'desc' },
      take: query.take,
    });
  }

  async queueSummary() {
    const [scheduledDue, scheduledLocked, outboxPending, webhookFailed, eventPending, extensionPending, botPending, deadLetters] = await Promise.all([
      this.prisma.scheduledJob.count({ where: { isEnabled: true, nextRunAt: { lte: new Date() } } }),
      this.prisma.scheduledJob.count({ where: { lockUntil: { gt: new Date() } } }),
      this.prisma.outboxMessage.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
      this.prisma.webhookDelivery.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
      this.prisma.eventSubscriptionDelivery.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
      this.prisma.extensionRuntimeEvent.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
      this.prisma.botOutboundMessage.count({ where: { status: { in: ['pending', 'failed', 'sending'] } } }),
      this.prisma.eventDeadLetter.count({ where: { status: 'open' } }),
    ]);
    return { scheduledDue, scheduledLocked, outboxPending, webhookFailed, eventPending, extensionPending, botPending, deadLetters };
  }

  private async requireJob(idOrCode: string) {
    const job = await this.prisma.scheduledJob.findFirst({ where: { OR: [{ id: idOrCode }, { code: idOrCode }] } });
    if (!job) throw new NotFoundException('Scheduled job не найден');
    if (job.type === 'http' && !(job.payload as Record<string, unknown> | null)?.url) throw new BadRequestException('HTTP job требует payload.url');
    return job;
  }

  private audit(actor: Principal, action: string, id: string, metadata: Record<string, unknown>) {
    return this.prisma.auditLog.create({ data: { actorId: actor.id, actorType: actor.type, action, resource: 'scheduled_job', resourceId: id, metadata: metadata as never } });
  }
}
