import { mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { buildReleaseCandidateSummary, defaultReleaseGates, evaluateReleaseGates, renderMarkdownReport, type ReleaseGateDefinition } from '@cajeerbots/release-gates';

export function releaseGatesCommand(): Command {
  const command = new Command('release-gates').description('Beta/RC release gates, feature-freeze и MVP readiness checks');

  command.command('summary')
    .action(async () => withPrisma(async (prisma) => {
      const [freeze, run, openBlockers, gates] = await Promise.all([
        prisma.releaseFreeze.findFirst({ orderBy: { createdAt: 'desc' } }),
        prisma.releaseGateRun.findFirst({ orderBy: { startedAt: 'desc' } }),
        prisma.releaseBlocker.count({ where: { status: 'open' } }),
        prisma.releaseQualityGate.count({ where: { isEnabled: true } }),
      ]);
      console.log(JSON.stringify({ freeze, lastRun: run, openBlockers, enabledGates: gates, defaultGateCount: defaultReleaseGates.length }, null, 2));
    }));

  command.command('freeze')
    .option('--version <version>', 'release version', '1.0.0-rc.3')
    .option('--channel <channel>', 'alpha|beta|rc|stable|lts|security', 'rc')
    .option('--status <status>', 'open|locked|released|cancelled', 'locked')
    .option('--reason <reason>', 'freeze reason')
    .action(async (options: { version: string; channel: string; status: string; reason?: string }) => withPrisma(async (prisma) => {
      const row = await prisma.releaseFreeze.upsert({
        where: { version_channel: { version: options.version, channel: options.channel } },
        create: { version: options.version, channel: options.channel, status: options.status, reason: options.reason, lockedBy: 'cli', scope: { features: 'frozen', allowedChanges: ['bugfix', 'test', 'docs', 'security'] } as never, releasedAt: options.status === 'released' ? new Date() : null },
        update: { status: options.status, reason: options.reason, releasedAt: options.status === 'released' ? new Date() : null },
      });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('defaults:seed')
    .action(async () => withPrisma(async (prisma) => {
      const rows = [];
      for (const gate of defaultReleaseGates) rows.push(await prisma.releaseQualityGate.upsert({ where: { code: gate.code }, create: { code: gate.code, title: gate.title, category: gate.category, severity: gate.severity, isEnabled: gate.enabled, config: (gate.config ?? {}) as never }, update: { title: gate.title, category: gate.category, severity: gate.severity, isEnabled: gate.enabled, config: (gate.config ?? {}) as never } }));
      console.log(JSON.stringify({ count: rows.length, gates: rows.map((row) => row.code) }, null, 2));
    }));

  command.command('gates:list')
    .option('--category <category>')
    .option('--enabled <enabled>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { category?: string; enabled?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.releaseQualityGate.findMany({ where: { ...(options.category ? { category: options.category } : {}), ...(options.enabled === undefined ? {} : { isEnabled: options.enabled === 'true' }) }, orderBy: [{ category: 'asc' }, { code: 'asc' }], take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('gates:set')
    .requiredOption('--code <code>')
    .requiredOption('--title <title>')
    .requiredOption('--category <category>')
    .option('--severity <severity>', 'info|warning|error|blocker', 'error')
    .option('--enabled <enabled>', 'true|false', 'true')
    .option('--config <json>', 'JSON config', '{}')
    .action(async (options: { code: string; title: string; category: string; severity: string; enabled: string; config: string }) => withPrisma(async (prisma) => {
      const config = JSON.parse(options.config || '{}');
      const row = await prisma.releaseQualityGate.upsert({ where: { code: options.code }, create: { code: options.code, title: options.title, category: options.category, severity: options.severity, isEnabled: options.enabled !== 'false', config: config as never }, update: { title: options.title, category: options.category, severity: options.severity, isEnabled: options.enabled !== 'false', config: config as never } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('run')
    .option('--version <version>', 'release version', '1.0.0-rc.3')
    .option('--channel <channel>', 'release channel', 'rc')
    .option('--root <dir>', 'project root', process.cwd())
    .option('--no-database-gates', 'use built-in gates instead of DB-configured gates')
    .option('--no-persist-blockers', 'do not persist blocker issues')
    .action(async (options: { version: string; channel: any; root: string; databaseGates?: boolean; persistBlockers?: boolean }) => withPrisma(async (prisma) => {
      if (options.databaseGates !== false) await ensureDefaultGates(prisma);
      const gates = options.databaseGates !== false ? await databaseGates(prisma) : defaultReleaseGates;
      const started = Date.now();
      const report = await evaluateReleaseGates({ rootDir: options.root, version: options.version, channel: options.channel, gates });
      const run = await prisma.releaseGateRun.create({ data: { version: options.version, channel: options.channel, status: report.status, rootDir: options.root, trigger: 'cli', summary: report.summary as never, results: report.results as never, finishedAt: new Date(), durationMs: Date.now() - started, createdBy: 'cli' } });
      for (const gate of report.results) await prisma.releaseQualityGate.updateMany({ where: { code: gate.code }, data: { lastStatus: gate.status, lastRunAt: new Date() } });
      if (options.persistBlockers !== false) {
        for (const gate of report.results) for (const issue of gate.issues) if (issue.severity === 'blocker' || issue.severity === 'error') await prisma.releaseBlocker.create({ data: { runId: run.id, code: issue.code, severity: issue.severity, source: issue.source, message: issue.message, details: { gateCode: gate.code, ...(issue.details ?? {}) } as never } });
      }
      console.log(JSON.stringify({ run, report }, null, 2));
      if (report.status === 'failed') process.exitCode = 1;
    }));

  command.command('runs:list')
    .option('--version <version>')
    .option('--status <status>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { version?: string; status?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.releaseGateRun.findMany({ where: { ...(options.version ? { version: options.version } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { startedAt: 'desc' }, take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('blockers:list')
    .option('--status <status>', 'open|accepted|fixed|false_positive')
    .option('--run <runId>')
    .option('--take <n>', 'limit', '200')
    .action(async (options: { status?: string; run?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.releaseBlocker.findMany({ where: { ...(options.status ? { status: options.status } : {}), ...(options.run ? { runId: options.run } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 1000) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('blockers:resolve')
    .argument('<id>')
    .option('--status <status>', 'fixed|accepted|false_positive|open', 'fixed')
    .option('--reason <reason>')
    .action(async (id: string, options: { status: string; reason?: string }) => withPrisma(async (prisma) => {
      const row = await prisma.releaseBlocker.update({ where: { id }, data: { status: options.status, acceptedReason: options.reason, resolvedAt: options.status === 'open' ? null : new Date(), resolvedBy: options.status === 'open' ? null : 'cli' } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('report:create')
    .option('--version <version>', 'release version', '1.0.0-rc.3')
    .option('--channel <channel>', 'release channel', 'rc')
    .option('--run <runId>')
    .option('--root <dir>', 'project root', process.cwd())
    .option('--out <file>', 'write markdown report')
    .action(async (options: { version: string; channel: any; run?: string; root: string; out?: string }) => withPrisma(async (prisma) => {
      let run = options.run ? await prisma.releaseGateRun.findUnique({ where: { id: options.run } }) : await prisma.releaseGateRun.findFirst({ where: { version: options.version, channel: options.channel }, orderBy: { startedAt: 'desc' } });
      let reportPayload: Awaited<ReturnType<typeof evaluateReleaseGates>>;
      if (!run) {
        await ensureDefaultGates(prisma);
        const gates = await databaseGates(prisma);
        reportPayload = await evaluateReleaseGates({ rootDir: options.root, version: options.version, channel: options.channel, gates });
        run = await prisma.releaseGateRun.create({ data: { version: options.version, channel: options.channel, status: reportPayload.status, rootDir: options.root, trigger: 'cli_report', summary: reportPayload.summary as never, results: reportPayload.results as never, finishedAt: new Date(), createdBy: 'cli' } });
      } else reportPayload = { version: run.version, channel: run.channel as never, status: run.status as never, summary: run.summary as never, results: run.results as never, generatedAt: new Date().toISOString() };
      const summary = buildReleaseCandidateSummary(reportPayload, run.id);
      const markdown = renderMarkdownReport(reportPayload);
      const row = await prisma.releaseCandidateReport.create({ data: { version: options.version, channel: options.channel, status: summary.status, gateRunId: run.id, summary: summary as never, markdown, artifactName: `release-gate-report-${options.version}.md`, createdBy: 'cli' } });
      if (options.out) await writeOutput(options.out, markdown);
      console.log(JSON.stringify({ report: row, summary }, null, 2));
    }));

  command.command('reports:list')
    .option('--version <version>')
    .option('--status <status>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { version?: string; status?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.releaseCandidateReport.findMany({ where: { ...(options.version ? { version: options.version } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  return command;
}

async function ensureDefaultGates(prisma: PrismaService): Promise<void> {
  const count = await prisma.releaseQualityGate.count();
  if (count > 0) return;
  for (const gate of defaultReleaseGates) await prisma.releaseQualityGate.create({ data: { code: gate.code, title: gate.title, category: gate.category, severity: gate.severity, isEnabled: gate.enabled, config: (gate.config ?? {}) as never } });
}

async function databaseGates(prisma: PrismaService): Promise<ReleaseGateDefinition[]> {
  const rows = await prisma.releaseQualityGate.findMany({ where: { isEnabled: true }, orderBy: [{ category: 'asc' }, { code: 'asc' }] });
  return rows.map((row) => ({ code: row.code, title: row.title, category: row.category, severity: row.severity as never, enabled: row.isEnabled, config: row.config as never }));
}

async function writeOutput(file: string, content: string): Promise<void> {
  await mkdir(dirname(file), { recursive: true });
  await writeFile(file, content);
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try { return await callback(prisma); } finally { await prisma.close(); }
}
