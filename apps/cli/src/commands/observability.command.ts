import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { collectObservabilitySnapshot, evaluateAlertRules, runDueProbes, runProbe } from '@cajeerbots/observability';

export function observabilityCommand(): Command {
  const command = new Command('observability').description('Observability metrics, probes, alerts и incidents');

  command.command('summary').action(async () => withPrisma(async (prisma) => {
    const samples = await prisma.observabilityMetricSample.findMany({ orderBy: { collectedAt: 'desc' }, take: 100 });
    const incidents = await prisma.observabilityIncident.findMany({ where: { status: 'open' }, orderBy: { startedAt: 'desc' }, take: 20 });
    const probes = await prisma.observabilityProbe.findMany({ orderBy: { code: 'asc' }, take: 100 });
    console.log(JSON.stringify({ samples, incidents, probes }, null, 2));
  }));

  command.command('collect').option('--no-diagnostics').action(async (options: { diagnostics: boolean }) => withPrisma(async (prisma) => {
    const snapshot = await collectObservabilitySnapshot(prisma, { includeDiagnostics: options.diagnostics });
    const probes = await runDueProbes(prisma);
    const alerts = await evaluateAlertRules(prisma);
    console.log(JSON.stringify({ snapshot, probes, alerts }, null, 2));
  }));

  command.command('samples:list').option('--metric <metric>').option('--source <source>').option('--severity <severity>').option('--take <n>', 'limit', '200').action(async (options: { metric?: string; source?: string; severity?: string; take: string }) => withPrisma(async (prisma) => {
    const rows = await prisma.observabilityMetricSample.findMany({ where: { ...(options.metric ? { metric: options.metric } : {}), ...(options.source ? { source: options.source } : {}), ...(options.severity ? { severity: options.severity } : {}) }, orderBy: { collectedAt: 'desc' }, take: Math.min(Number(options.take), 2000) });
    console.log(JSON.stringify(rows, null, 2));
  }));

  command.command('probes:list').option('--status <status>').option('--enabled <bool>').option('--take <n>', 'limit', '100').action(async (options: { status?: string; enabled?: string; take: string }) => withPrisma(async (prisma) => {
    const rows = await prisma.observabilityProbe.findMany({ where: { ...(options.status ? { status: options.status } : {}), ...(options.enabled !== undefined ? { isEnabled: options.enabled === 'true' } : {}) }, orderBy: [{ isEnabled: 'desc' }, { code: 'asc' }], take: Math.min(Number(options.take), 500) });
    console.log(JSON.stringify(rows, null, 2));
  }));

  command.command('probes:set')
    .requiredOption('--code <code>')
    .requiredOption('--title <title>')
    .option('--type <type>', 'database | redis_tcp | tcp | storage', 'database')
    .option('--target <target>')
    .option('--interval <seconds>', 'interval seconds', '60')
    .option('--timeout-ms <ms>', 'timeout milliseconds', '2000')
    .option('--disabled')
    .action(async (options: { code: string; title: string; type: string; target?: string; interval: string; timeoutMs: string; disabled?: boolean }) => withPrisma(async (prisma) => {
      const probe = await prisma.observabilityProbe.upsert({ where: { code: options.code }, create: { code: options.code, title: options.title, type: options.type, target: options.target, intervalSeconds: Number(options.interval), timeoutMs: Number(options.timeoutMs), isEnabled: !options.disabled, nextCheckAt: options.disabled ? null : new Date() }, update: { title: options.title, type: options.type, target: options.target, intervalSeconds: Number(options.interval), timeoutMs: Number(options.timeoutMs), isEnabled: !options.disabled, nextCheckAt: options.disabled ? null : new Date() } });
      console.log(JSON.stringify(probe, null, 2));
    }));

  command.command('probes:run').argument('<idOrCode>').action(async (idOrCode: string) => withPrisma(async (prisma) => {
    const probe = await prisma.observabilityProbe.findFirst({ where: { OR: [{ id: idOrCode }, { code: idOrCode }] } });
    if (!probe) throw new Error('Probe не найден');
    console.log(JSON.stringify(await runProbe(prisma, probe), null, 2));
  }));

  command.command('alerts:list').option('--metric <metric>').option('--enabled <bool>').option('--severity <severity>').option('--take <n>', 'limit', '100').action(async (options: { metric?: string; enabled?: string; severity?: string; take: string }) => withPrisma(async (prisma) => {
    const rows = await prisma.observabilityAlertRule.findMany({ where: { ...(options.metric ? { metric: options.metric } : {}), ...(options.enabled !== undefined ? { isEnabled: options.enabled === 'true' } : {}), ...(options.severity ? { severity: options.severity } : {}) }, orderBy: [{ isEnabled: 'desc' }, { code: 'asc' }], take: Math.min(Number(options.take), 500) });
    console.log(JSON.stringify(rows, null, 2));
  }));

  command.command('alerts:set')
    .requiredOption('--code <code>')
    .requiredOption('--title <title>')
    .requiredOption('--metric <metric>')
    .option('--operator <operator>', 'gt | gte | lt | lte | eq | neq', 'gte')
    .requiredOption('--threshold <value>')
    .option('--severity <severity>', 'info | warning | critical', 'warning')
    .option('--cooldown <seconds>', 'cooldown seconds', '300')
    .option('--disabled')
    .action(async (options: { code: string; title: string; metric: string; operator: string; threshold: string; severity: string; cooldown: string; disabled?: boolean }) => withPrisma(async (prisma) => {
      const rule = await prisma.observabilityAlertRule.upsert({ where: { code: options.code }, create: { code: options.code, title: options.title, metric: options.metric, operator: options.operator, threshold: Number(options.threshold), severity: options.severity, cooldownSeconds: Number(options.cooldown), isEnabled: !options.disabled }, update: { title: options.title, metric: options.metric, operator: options.operator, threshold: Number(options.threshold), severity: options.severity, cooldownSeconds: Number(options.cooldown), isEnabled: !options.disabled } });
      console.log(JSON.stringify(rule, null, 2));
    }));

  command.command('alerts:evaluate').action(async () => withPrisma(async (prisma) => console.log(JSON.stringify(await evaluateAlertRules(prisma), null, 2))));

  command.command('incidents:list').option('--status <status>').option('--severity <severity>').option('--metric <metric>').option('--take <n>', 'limit', '100').action(async (options: { status?: string; severity?: string; metric?: string; take: string }) => withPrisma(async (prisma) => {
    const rows = await prisma.observabilityIncident.findMany({ where: { ...(options.status ? { status: options.status } : {}), ...(options.severity ? { severity: options.severity } : {}), ...(options.metric ? { metric: options.metric } : {}) }, orderBy: [{ status: 'asc' }, { startedAt: 'desc' }], take: Math.min(Number(options.take), 500), include: { events: { orderBy: { createdAt: 'desc' }, take: 10 } } });
    console.log(JSON.stringify(rows, null, 2));
  }));

  command.command('incidents:resolve').argument('<idOrCode>').option('--reason <reason>').action(async (idOrCode: string, options: { reason?: string }) => withPrisma(async (prisma) => {
    const incident = await prisma.observabilityIncident.findFirst({ where: { OR: [{ id: idOrCode }, { code: idOrCode }] } });
    if (!incident) throw new Error('Incident не найден');
    const updated = await prisma.observabilityIncident.update({ where: { id: incident.id }, data: { status: 'resolved', resolvedAt: new Date(), metadata: { ...(incident.metadata as Record<string, unknown> | null ?? {}), resolvedBy: 'cli', reason: options.reason } as never } });
    console.log(JSON.stringify(updated, null, 2));
  }));

  return command;
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try { return await callback(prisma); } finally { await prisma.close(); }
}
