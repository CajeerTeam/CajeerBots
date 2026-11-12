import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { seedDefaultSecurityAuditPolicies, runSecurityAudit } from '@cajeerbots/security-audit';

export function securityCommand(): Command {
  const command = new Command('security').description('Security beta audit, findings, risk snapshots and hardening checks');

  command.command('summary')
    .action(async () => withPrisma(async (prisma) => {
      const [hardening, policies, lastRun, openFindings, criticalFindings, snapshot] = await Promise.all([
        hardeningSummary(prisma),
        prisma.securityAuditPolicy.count({ where: { isEnabled: true } }).catch(() => 0),
        prisma.securityAuditRun.findFirst({ orderBy: { startedAt: 'desc' } }).catch(() => null),
        prisma.securityAuditFinding.count({ where: { status: 'open' } }).catch(() => 0),
        prisma.securityAuditFinding.count({ where: { status: 'open', severity: 'critical' } }).catch(() => 0),
        prisma.securityRiskSnapshot.findFirst({ orderBy: { createdAt: 'desc' } }).catch(() => null),
      ]);
      console.log(JSON.stringify({ hardening, policies, lastRun, openFindings, criticalFindings, latestRiskSnapshot: snapshot }, null, 2));
    }));

  command.command('audit:defaults')
    .action(async () => withPrisma(async (prisma) => {
      const result = await seedDefaultSecurityAuditPolicies(prisma);
      console.log(JSON.stringify(result, null, 2));
    }));

  command.command('audit:run')
    .option('--version <version>', 'release version', '1.0.0-rc.3')
    .option('--channel <channel>', 'release channel', 'beta')
    .option('--trigger <trigger>', 'trigger name', 'cli')
    .option('--no-persist', 'do not persist audit run/findings')
    .action(async (options: { version: string; channel: string; trigger: string; persist?: boolean }) => withPrisma(async (prisma) => {
      await seedDefaultSecurityAuditPolicies(prisma);
      const result = await runSecurityAudit({ prisma, version: options.version, channel: options.channel, trigger: options.trigger, createdBy: 'cli', persist: options.persist !== false });
      console.log(JSON.stringify(result, null, 2));
      if (result.report.status === 'failed') process.exitCode = 1;
    }));

  command.command('policies:list')
    .option('--category <category>')
    .option('--enabled <enabled>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { category?: string; enabled?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.securityAuditPolicy.findMany({ where: { ...(options.category ? { category: options.category } : {}), ...(options.enabled === undefined ? {} : { isEnabled: options.enabled === 'true' }) }, orderBy: [{ category: 'asc' }, { code: 'asc' }], take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('policies:set')
    .requiredOption('--code <code>')
    .requiredOption('--title <title>')
    .requiredOption('--category <category>')
    .option('--severity <severity>', 'info|warning|error|critical', 'error')
    .option('--enabled <enabled>', 'true|false', 'true')
    .option('--config <json>', 'JSON config', '{}')
    .action(async (options: { code: string; title: string; category: string; severity: string; enabled: string; config: string }) => withPrisma(async (prisma) => {
      const config = JSON.parse(options.config || '{}');
      const row = await prisma.securityAuditPolicy.upsert({ where: { code: options.code }, create: { code: options.code, title: options.title, category: options.category, severity: options.severity, isEnabled: options.enabled !== 'false', config: config as never }, update: { title: options.title, category: options.category, severity: options.severity, isEnabled: options.enabled !== 'false', config: config as never } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('runs:list')
    .option('--version <version>')
    .option('--status <status>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { version?: string; status?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.securityAuditRun.findMany({ where: { ...(options.version ? { version: options.version } : {}), ...(options.status ? { status: options.status } : {}) }, orderBy: { startedAt: 'desc' }, take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('runs:get')
    .argument('<id>')
    .action(async (id: string) => withPrisma(async (prisma) => {
      const row = await prisma.securityAuditRun.findUnique({ where: { id }, include: { findings: { orderBy: [{ severity: 'asc' }, { createdAt: 'desc' }] } } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('findings:list')
    .option('--status <status>')
    .option('--severity <severity>')
    .option('--category <category>')
    .option('--run <runId>')
    .option('--take <n>', 'limit', '200')
    .action(async (options: { status?: string; severity?: string; category?: string; run?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.securityAuditFinding.findMany({ where: { ...(options.status ? { status: options.status } : {}), ...(options.severity ? { severity: options.severity } : {}), ...(options.category ? { category: options.category } : {}), ...(options.run ? { runId: options.run } : {}) }, orderBy: [{ status: 'asc' }, { severity: 'asc' }, { createdAt: 'desc' }], take: Math.min(Number(options.take), 1000) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('findings:resolve')
    .argument('<id>')
    .option('--status <status>', 'fixed|accepted|false_positive|open', 'fixed')
    .option('--by <actor>', 'actor id', 'cli')
    .action(async (id: string, options: { status: string; by: string }) => withPrisma(async (prisma) => {
      const row = await prisma.securityAuditFinding.update({ where: { id }, data: { status: options.status, resolvedAt: options.status === 'open' ? null : new Date(), resolvedBy: options.status === 'open' ? null : options.by } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('snapshots:list')
    .option('--status <status>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { status?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.securityRiskSnapshot.findMany({ where: { ...(options.status ? { status: options.status } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('sessions:revoke-stale')
    .option('--older-than-days <days>', 'session age', '30')
    .option('--dry-run', 'only print matched sessions', false)
    .action(async (options: { olderThanDays: string; dryRun?: boolean }) => withPrisma(async (prisma) => {
      const before = new Date(Date.now() - Number(options.olderThanDays) * 86_400_000);
      const where = { revokedAt: null, expiresAt: { gt: new Date() }, createdAt: { lt: before } };
      const sessions = await prisma.adminSession.findMany({ where, orderBy: { createdAt: 'asc' }, select: { id: true, accountId: true, createdAt: true, expiresAt: true, ip: true } });
      if (!options.dryRun) await prisma.adminSession.updateMany({ where, data: { revokedAt: new Date() } });
      console.log(JSON.stringify({ matched: sessions.length, revoked: options.dryRun ? 0 : sessions.length, sessions }, null, 2));
    }));

  return command;
}

async function hardeningSummary(prisma: PrismaService) {
  const now = new Date();
  const [adminAccounts, active2faAccounts, activeApiKeys, openSessions, openFindings] = await Promise.all([
    prisma.adminAccount.count(),
    prisma.adminAccount.count({ where: { isActive: true, twoFactorEnabled: true } }),
    prisma.apiKey.count({ where: { revokedAt: null, OR: [{ expiresAt: null }, { expiresAt: { gt: now } }] } }),
    prisma.adminSession.count({ where: { revokedAt: null, expiresAt: { gt: now } } }),
    prisma.securityAuditFinding.count({ where: { status: 'open', severity: { in: ['critical', 'error'] } } }).catch(() => 0),
  ]);
  return { status: adminAccounts > 0 ? openFindings > 0 ? 'degraded' : 'ok' : 'bootstrap_required', adminAccounts, active2faAccounts, activeApiKeys, openSessions, openFindings };
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try { return await callback(prisma); } finally { await prisma.close(); }
}
