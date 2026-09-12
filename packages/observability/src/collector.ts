import { createDiagnosticReport } from './diagnostics.js';

export interface MetricSampleInput {
  communityId?: string | null;
  source: string;
  metric: string;
  value: number;
  unit?: string;
  labels?: Record<string, unknown>;
  windowSeconds?: number;
  severity?: 'info' | 'warning' | 'critical' | string;
  collectedAt?: Date;
}

export interface ObservabilityCollectorOptions {
  includeDiagnostics?: boolean;
  retentionDays?: number;
  now?: Date;
}

export interface ObservabilitySnapshotResult {
  collectedAt: Date;
  samples: MetricSampleInput[];
  inserted: number;
  deletedOldSamples: number;
}

type PrismaLike = any;

export async function collectObservabilitySnapshot(prisma: PrismaLike, options: ObservabilityCollectorOptions = {}): Promise<ObservabilitySnapshotResult> {
  const now = options.now ?? new Date();
  const samples: MetricSampleInput[] = [];
  const push = (metric: string, value: number, labels: Record<string, unknown> = {}, unit = 'count', source = 'database'): void => {
    samples.push({ source, metric, value, unit, labels, collectedAt: now });
  };

  const [communities, admins, apiKeysActive, outboxPending, outboxDead, webhookPending, webhookDead, eventPending, eventDead, extensionPending, botOutboundPending, botOutboundDead, scheduledDue, scheduledLocked, openIncidents, activeExtensions, activeBotAccounts, backupFailed] = await Promise.all([
    prisma.community.count(),
    prisma.adminAccount?.count?.({ where: { isActive: true } }) ?? Promise.resolve(0),
    prisma.apiKey?.count?.({ where: { revokedAt: null } }) ?? Promise.resolve(0),
    prisma.outboxMessage.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
    prisma.outboxMessage.count({ where: { status: 'dead' } }),
    prisma.webhookDelivery.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
    prisma.webhookDelivery.count({ where: { status: 'dead' } }),
    prisma.eventSubscriptionDelivery.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
    prisma.eventDeadLetter.count({ where: { status: 'open' } }),
    prisma.extensionRuntimeEvent.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
    prisma.botOutboundMessage.count({ where: { status: { in: ['pending', 'failed', 'sending'] } } }),
    prisma.botOutboundMessage.count({ where: { status: 'dead' } }),
    prisma.scheduledJob.count({ where: { isEnabled: true, nextRunAt: { lte: now } } }),
    prisma.scheduledJob.count({ where: { lockUntil: { gt: now } } }),
    prisma.observabilityIncident?.count?.({ where: { status: 'open' } }) ?? Promise.resolve(0),
    prisma.extension.count({ where: { status: 'enabled' } }),
    prisma.botAccount.count({ where: { isActive: true } }),
    prisma.backupArtifact.count({ where: { status: 'failed' } }),
  ]);

  push('communities.total', communities);
  push('admin_accounts.active', admins);
  push('api_keys.active', apiKeysActive);
  push('outbox.pending', outboxPending);
  push('outbox.dead', outboxDead);
  push('webhooks.pending', webhookPending);
  push('webhooks.dead', webhookDead);
  push('event_api.pending_deliveries', eventPending);
  push('event_api.dead_letters.open', eventDead);
  push('extensions.runtime.pending', extensionPending);
  push('bot_runtime.outbound.pending', botOutboundPending);
  push('bot_runtime.outbound.dead', botOutboundDead);
  push('scheduler.jobs.due', scheduledDue);
  push('scheduler.jobs.locked', scheduledLocked);
  push('observability.incidents.open', openIncidents);
  push('extensions.enabled', activeExtensions);
  push('bot_accounts.active', activeBotAccounts);
  push('backups.failed', backupFailed);

  if (options.includeDiagnostics ?? true) {
    const report = await createDiagnosticReport({
      databaseUrl: process.env.DATABASE_URL,
      redisUrl: process.env.REDIS_URL,
      queueDriver: process.env.QUEUE_DRIVER,
      storageRoot: process.env.STORAGE_LOCAL_ROOT ?? './storage',
      databaseProbe: () => prisma.$queryRaw`SELECT 1`,
    });
    for (const [name, check] of Object.entries(report.checks)) {
      samples.push({ source: 'diagnostics', metric: `diagnostics.${name}.status`, value: check.status === 'ok' ? 1 : check.status === 'degraded' ? 0.5 : 0, unit: 'status', labels: { message: check.message }, severity: check.status === 'ok' ? 'info' : check.status === 'degraded' ? 'warning' : 'critical', collectedAt: now });
      if (typeof check.latencyMs === 'number') samples.push({ source: 'diagnostics', metric: `diagnostics.${name}.latency_ms`, value: check.latencyMs, unit: 'ms', labels: { status: check.status }, severity: check.latencyMs > 1000 ? 'warning' : 'info', collectedAt: now });
    }
  }

  const created = await prisma.observabilityMetricSample.createMany({ data: samples.map((sample) => ({ ...sample, labels: sample.labels as never, collectedAt: sample.collectedAt ?? now })) });
  let deletedOldSamples = 0;
  const retentionDays = options.retentionDays ?? Number(process.env.OBSERVABILITY_RETENTION_DAYS ?? 14);
  if (retentionDays > 0) {
    const deleted = await prisma.observabilityMetricSample.deleteMany({ where: { collectedAt: { lt: new Date(now.getTime() - retentionDays * 86_400_000) } } });
    deletedOldSamples = deleted.count;
  }
  return { collectedAt: now, samples, inserted: created.count, deletedOldSamples };
}

export function aggregateLatestSamples(samples: Array<{ metric: string; source: string; value: number; unit?: string | null; severity: string; collectedAt: Date | string }>) {
  const latest = new Map<string, unknown>();
  for (const sample of samples) {
    const key = `${sample.source}:${sample.metric}`;
    const current = latest.get(key) as { collectedAt?: Date | string } | undefined;
    if (!current || new Date(sample.collectedAt).getTime() >= new Date(current.collectedAt as string).getTime()) latest.set(key, sample);
  }
  return [...latest.values()];
}
