import { createConnection } from 'node:net';
import { writeFile, rm, mkdir } from 'node:fs/promises';
import { dirname, join } from 'node:path';

type PrismaLike = any;

export interface AlertEvaluationResult {
  evaluated: number;
  fired: number;
  resolved: number;
  events: unknown[];
}

export async function evaluateAlertRules(prisma: PrismaLike, now = new Date()): Promise<AlertEvaluationResult> {
  const rules = await prisma.observabilityAlertRule.findMany({ where: { isEnabled: true }, orderBy: { code: 'asc' } });
  let fired = 0;
  let resolved = 0;
  const events: unknown[] = [];
  for (const rule of rules) {
    const latest = await prisma.observabilityMetricSample.findFirst({ where: { metric: rule.metric }, orderBy: { collectedAt: 'desc' } });
    if (!latest) continue;
    const active = compare(Number(latest.value), rule.operator, Number(rule.threshold));
    const incidentCode = `alert.${rule.code}`;
    const openIncident = await prisma.observabilityIncident.findUnique({ where: { code: incidentCode } });
    const inCooldown = rule.lastTriggeredAt && now.getTime() - new Date(rule.lastTriggeredAt).getTime() < rule.cooldownSeconds * 1000;

    if (active && !inCooldown) {
      const incident = await prisma.observabilityIncident.upsert({
        where: { code: incidentCode },
        create: { code: incidentCode, title: rule.title, status: 'open', severity: rule.severity, source: 'alert_rule', metric: rule.metric, currentValue: latest.value, metadata: { ruleId: rule.id, sampleId: latest.id } as never },
        update: { status: 'open', resolvedAt: null, severity: rule.severity, currentValue: latest.value, metadata: { ruleId: rule.id, sampleId: latest.id } as never },
      });
      const event = await prisma.observabilityAlertEvent.create({ data: { ruleId: rule.id, incidentId: incident.id, status: 'fired', severity: rule.severity, metric: rule.metric, value: latest.value, threshold: rule.threshold, message: `${rule.title}: ${latest.value} ${rule.operator} ${rule.threshold}`, metadata: { sampleId: latest.id } as never } });
      await prisma.observabilityAlertRule.update({ where: { id: rule.id }, data: { lastTriggeredAt: now } });
      fired += 1;
      events.push(event);
    }

    if (!active && openIncident?.status === 'open') {
      const incident = await prisma.observabilityIncident.update({ where: { id: openIncident.id }, data: { status: 'resolved', resolvedAt: now, currentValue: latest.value } });
      const event = await prisma.observabilityAlertEvent.create({ data: { ruleId: rule.id, incidentId: incident.id, status: 'resolved', severity: rule.severity, metric: rule.metric, value: latest.value, threshold: rule.threshold, message: `${rule.title}: состояние восстановлено`, resolvedAt: now, metadata: { sampleId: latest.id } as never } });
      resolved += 1;
      events.push(event);
    }
  }
  return { evaluated: rules.length, fired, resolved, events };
}

export function compare(value: number, operator: string, threshold: number): boolean {
  switch (operator) {
    case 'gt': return value > threshold;
    case 'gte': return value >= threshold;
    case 'lt': return value < threshold;
    case 'lte': return value <= threshold;
    case 'eq': return value === threshold;
    case 'neq': return value !== threshold;
    default: throw new Error(`Unknown alert operator: ${operator}`);
  }
}

export async function runDueProbes(prisma: PrismaLike, now = new Date()): Promise<{ checked: number; results: unknown[] }> {
  const probes = await prisma.observabilityProbe.findMany({
    where: { isEnabled: true, OR: [{ nextCheckAt: null }, { nextCheckAt: { lte: now } }] },
    orderBy: [{ nextCheckAt: 'asc' }, { code: 'asc' }],
    take: Number(process.env.OBSERVABILITY_PROBE_BATCH_SIZE ?? 20),
  });
  const results: unknown[] = [];
  for (const probe of probes) results.push(await runProbe(prisma, probe, now));
  return { checked: probes.length, results };
}

export async function runProbe(prisma: PrismaLike, probe: any, now = new Date()) {
  const started = Date.now();
  let status = 'ok';
  let error: string | null = null;
  try {
    if (probe.type === 'database') await prisma.$queryRaw`SELECT 1`;
    else if (probe.type === 'redis_tcp') await tcpUrl(process.env.REDIS_URL, probe.timeoutMs);
    else if (probe.type === 'tcp') await tcpTarget(probe.target, probe.timeoutMs);
    else if (probe.type === 'storage') await storageProbe(probe.target ?? process.env.STORAGE_LOCAL_ROOT ?? './storage');
    else throw new Error(`Unsupported probe type: ${probe.type}`);
  } catch (caught) {
    status = 'down';
    error = caught instanceof Error ? caught.message : String(caught);
  }
  const latencyMs = Date.now() - started;
  const nextCheckAt = new Date(now.getTime() + Math.max(10, probe.intervalSeconds) * 1000);
  const updated = await prisma.observabilityProbe.update({ where: { id: probe.id }, data: { status, latencyMs, lastError: error, lastCheckedAt: now, nextCheckAt } });
  await prisma.observabilityMetricSample.create({ data: { source: 'probe', metric: `probe.${probe.code}.status`, value: status === 'ok' ? 1 : 0, unit: 'status', labels: { type: probe.type, target: probe.target, error } as never, severity: status === 'ok' ? 'info' : 'critical', collectedAt: now } });
  await prisma.observabilityMetricSample.create({ data: { source: 'probe', metric: `probe.${probe.code}.latency_ms`, value: latencyMs, unit: 'ms', labels: { type: probe.type, target: probe.target } as never, severity: latencyMs > 1000 ? 'warning' : 'info', collectedAt: now } });
  return updated;
}

async function tcpUrl(rawUrl: string | undefined, timeoutMs: number): Promise<void> {
  if (!rawUrl) throw new Error('URL не задан');
  const url = new URL(rawUrl);
  const port = Number(url.port || (url.protocol === 'postgresql:' ? 5432 : 6379));
  await tcpConnect(url.hostname, port, timeoutMs);
}

async function tcpTarget(target: string | null | undefined, timeoutMs: number): Promise<void> {
  if (!target) throw new Error('target не задан');
  const [host, rawPort] = target.split(':');
  await tcpConnect(host, Number(rawPort), timeoutMs);
}

function tcpConnect(host: string, port: number, timeoutMs: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const socket = createConnection({ host, port, timeout: timeoutMs });
    socket.once('connect', () => { socket.destroy(); resolve(); });
    socket.once('timeout', () => { socket.destroy(); reject(new Error('timeout')); });
    socket.once('error', reject);
  });
}

async function storageProbe(root: string): Promise<void> {
  const probe = join(root, '.cajeer-observability-probe');
  await mkdir(dirname(probe), { recursive: true });
  await writeFile(probe, 'ok');
  await rm(probe, { force: true });
}
