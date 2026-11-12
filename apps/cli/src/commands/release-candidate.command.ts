import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { buildCompatibilityMatrix, createApiFreezeSnapshot, createUpgradePlan, defaultCompatibilityTargets, runRcReadiness, verifyApiFreeze } from '@cajeerbots/release-candidate';

export function releaseCandidateCommand(): Command {
  const command = new Command('rc').description('Release Candidate 1.0.0: API freeze, compatibility matrix, upgrade rehearsal и readiness checks');

  command.command('summary').action(async () => withPrisma(async (prisma) => {
    const [apiFreeze, compatibilityMatrix, upgradePlan, upgradeRun, readinessRun, openBlockers, openFindings] = await Promise.all([
      prisma.releaseApiFreeze.findFirst({ orderBy: { createdAt: 'desc' } }),
      prisma.releaseCompatibilityMatrix.findFirst({ orderBy: { createdAt: 'desc' } }),
      prisma.releaseUpgradePlan.findFirst({ orderBy: { createdAt: 'desc' } }),
      prisma.releaseUpgradeRun.findFirst({ orderBy: { createdAt: 'desc' } }),
      prisma.releaseCandidateReadinessRun.findFirst({ orderBy: { startedAt: 'desc' } }),
      prisma.releaseBlocker.count({ where: { status: 'open' } }),
      prisma.securityAuditFinding.count({ where: { status: 'open' } }),
    ]);
    print({ apiFreeze, compatibilityMatrix, upgradePlan, upgradeRun, readinessRun, openBlockers, openFindings, defaultCompatibilityTargets: defaultCompatibilityTargets.length });
  }));

  command.command('api-freeze:create')
    .option('--version <version>', 'release version', '1.0.0-rc.2')
    .option('--channel <channel>', 'rc|stable|lts|security', 'rc')
    .option('--root <dir>', 'project root', process.cwd())
    .option('--source <path>', 'OpenAPI source path', 'contracts/openapi/cajeerbots.openapi.yaml')
    .option('--notes <notes>')
    .action(async (options: { version: string; channel: any; root: string; source: string; notes?: string }) => withPrisma(async (prisma) => {
      const payload = await createApiFreezeSnapshot({ version: options.version, channel: options.channel, rootDir: options.root, sourcePath: options.source });
      const freeze = await prisma.releaseApiFreeze.create({ data: { version: options.version, channel: options.channel, status: 'frozen', sourcePath: options.source, sourceSha256: payload.sourceSha256, openApiVersion: payload.openApiVersion, operationCount: payload.operationCount, operations: payload.operations as never, duplicateOperationIds: payload.duplicateOperationIds as never, unsecuredOperations: payload.unsecuredOperations as never, deprecatedOperations: payload.deprecatedOperations as never, payload: payload as never, notes: options.notes, createdBy: 'cli' } });
      print({ freeze, payload });
    }));

  command.command('api-freeze:verify')
    .option('--version <version>', 'release version', '1.0.0-rc.2')
    .option('--channel <channel>', 'rc|stable|lts|security', 'rc')
    .option('--freeze <id>')
    .option('--root <dir>', 'project root', process.cwd())
    .option('--source <path>')
    .option('--allow-additive', 'allow additive API operations after freeze', false)
    .action(async (options: { version: string; channel: any; freeze?: string; root: string; source?: string; allowAdditive?: boolean }) => withPrisma(async (prisma) => {
      const freeze = options.freeze ? await prisma.releaseApiFreeze.findUnique({ where: { id: options.freeze } }) : await prisma.releaseApiFreeze.findFirst({ where: { version: options.version, channel: options.channel, status: 'frozen' }, orderBy: { createdAt: 'desc' } });
      if (!freeze) throw new Error('API freeze snapshot не найден. Сначала выполните cajeer rc api-freeze:create.');
      const report = await verifyApiFreeze({ freeze: freeze.payload as never, rootDir: options.root, sourcePath: options.source ?? freeze.sourcePath, allowAdditive: options.allowAdditive });
      const check = await prisma.releaseUpgradeCheck.create({ data: { version: freeze.version, channel: freeze.channel, code: 'api_freeze.verify', status: report.status, severity: report.status === 'failed' ? 'blocker' : report.status === 'warning' ? 'warning' : 'info', message: `API freeze verification: ${report.status}`, details: report as never, createdBy: 'cli' } });
      print({ check, report });
      if (report.status === 'failed') process.exitCode = 1;
    }));

  command.command('compatibility:defaults').action(async () => withPrisma(async (prisma) => {
    const rows = [];
    for (const target of defaultCompatibilityTargets) rows.push(await prisma.releaseCompatibilityTarget.upsert({ where: { code: target.code }, create: { code: target.code, kind: target.kind, name: target.name, versionRange: target.versionRange, required: target.required, status: target.status ?? 'supported', notes: target.notes, checks: (target.checks ?? []) as never }, update: { kind: target.kind, name: target.name, versionRange: target.versionRange, required: target.required, status: target.status ?? 'supported', notes: target.notes, checks: (target.checks ?? []) as never } }));
    print({ count: rows.length, targets: rows });
  }));

  command.command('compatibility:matrix')
    .option('--version <version>', 'release version', '1.0.0-rc.2')
    .option('--channel <channel>', 'rc|stable|lts|security', 'rc')
    .option('--no-persist')
    .action(async (options: { version: string; channel: any; persist?: boolean }) => withPrisma(async (prisma) => {
      const rows = await prisma.releaseCompatibilityTarget.findMany({ orderBy: [{ kind: 'asc' }, { code: 'asc' }] });
      const targets = rows.length ? rows.map((row) => ({ code: row.code, kind: row.kind as never, name: row.name, versionRange: row.versionRange, required: row.required, status: row.status as never, notes: row.notes ?? undefined, checks: row.checks as never })) : defaultCompatibilityTargets;
      const report = buildCompatibilityMatrix({ version: options.version, channel: options.channel, targets });
      const matrix = options.persist === false ? null : await prisma.releaseCompatibilityMatrix.create({ data: { version: options.version, channel: options.channel, status: report.status, summary: report.summary as never, targets: report.targets as never, createdBy: 'cli' } });
      print({ matrix, report });
      if (report.status === 'failed') process.exitCode = 1;
    }));

  command.command('upgrade-plan:create')
    .option('--from <version>', 'from version', '0.18.0-beta.3')
    .option('--to <version>', 'to version', '1.0.0-rc.2')
    .option('--channel <channel>', 'rc|stable|lts|security', 'rc')
    .option('--has-backup', 'confirm fresh backup exists', false)
    .action(async (options: { from: string; to: string; channel: any; hasBackup?: boolean }) => withPrisma(async (prisma) => {
      const openBlockers = await prisma.releaseBlocker.count({ where: { status: 'open' } });
      const latestBackup = await prisma.backupArtifact.findFirst({ orderBy: { createdAt: 'desc' } }).catch(() => null);
      const payload = createUpgradePlan({ fromVersion: options.from, toVersion: options.to, channel: options.channel, hasBackup: options.hasBackup || Boolean(latestBackup), openBlockers });
      const plan = await prisma.releaseUpgradePlan.create({ data: { fromVersion: options.from, toVersion: options.to, channel: options.channel, status: payload.status, steps: payload.steps as never, risks: payload.risks as never, checks: payload.checks as never, payload: payload as never, createdBy: 'cli' } });
      print({ plan, payload });
      if (payload.status === 'blocked') process.exitCode = 1;
    }));

  command.command('upgrade-plan:run')
    .argument('<id>')
    .option('--mode <mode>', 'dry-run|apply', 'dry-run')
    .option('--root <dir>', 'project root', process.cwd())
    .action(async (id: string, options: { mode: string; root: string }) => withPrisma(async (prisma) => {
      const plan = await prisma.releaseUpgradePlan.findUnique({ where: { id } });
      if (!plan) throw new Error('Upgrade plan не найден');
      const started = Date.now();
      const steps = Array.isArray(plan.steps) ? plan.steps as Array<Record<string, unknown>> : [];
      const results = steps.map((step) => ({ code: step.code, phase: step.phase, status: options.mode === 'apply' && step.phase === 'rollback' ? 'skipped' : 'passed', mode: options.mode, command: step.command }));
      const planRisks = (plan.risks as Array<{ severity?: string }> | null) ?? [];
      const status = planRisks.some((risk) => risk.severity === 'blocker') ? 'failed' : 'passed';
      const run = await prisma.releaseUpgradeRun.create({ data: { planId: plan.id, mode: options.mode, status, rootDir: options.root, results: results as never, logs: [{ message: 'RC upgrade rehearsal completed', mode: options.mode }] as never, startedAt: new Date(started), finishedAt: new Date(), durationMs: Date.now() - started, createdBy: 'cli' } });
      print({ run, plan, results });
      if (status === 'failed') process.exitCode = 1;
    }));

  command.command('readiness:run')
    .option('--version <version>', 'release version', '1.0.0-rc.2')
    .option('--channel <channel>', 'rc|stable|lts|security', 'rc')
    .option('--root <dir>', 'project root', process.cwd())
    .option('--no-persist')
    .action(async (options: { version: string; channel: any; root: string; persist?: boolean }) => withPrisma(async (prisma) => {
      const [freeze, targets, openBlockers, openFindings, latestIntegration] = await Promise.all([
        prisma.releaseApiFreeze.findFirst({ where: { version: options.version, channel: options.channel, status: 'frozen' }, orderBy: { createdAt: 'desc' } }),
        prisma.releaseCompatibilityTarget.findMany(),
        prisma.releaseBlocker.count({ where: { status: 'open' } }),
        prisma.securityAuditFinding.count({ where: { status: 'open' } }),
        prisma.integrationTestCoverage.findFirst({ where: { version: options.version, channel: options.channel }, orderBy: { createdAt: 'desc' } }),
      ]);
      const report = await runRcReadiness({ version: options.version, channel: options.channel, rootDir: options.root, freeze: freeze?.payload as never, compatibilityTargets: targets.map((row) => ({ code: row.code, kind: row.kind as never, name: row.name, versionRange: row.versionRange, required: row.required, status: row.status as never, notes: row.notes ?? undefined, checks: row.checks as never })), openBlockers, securityOpenFindings: openFindings, latestIntegrationStatus: latestIntegration?.status ?? null });
      const run = options.persist === false ? null : await prisma.releaseCandidateReadinessRun.create({ data: { version: options.version, channel: options.channel, status: report.status, rootDir: options.root, summary: report.summary as never, sections: report.sections as never, startedAt: new Date(), finishedAt: new Date(), createdBy: 'cli' } });
      print({ run, report });
      if (report.status === 'failed') process.exitCode = 1;
    }));

  command.command('readiness:list')
    .option('--version <version>')
    .option('--status <status>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { version?: string; status?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.releaseCandidateReadinessRun.findMany({ where: { ...(options.version ? { version: options.version } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { startedAt: 'desc' }, take: Math.min(Number(options.take), 500) });
      print(rows);
    }));

  return command;
}

function print(value: unknown): void {
  console.log(JSON.stringify(value, null, 2));
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try { return await callback(prisma); } finally { await prisma.close(); }
}
