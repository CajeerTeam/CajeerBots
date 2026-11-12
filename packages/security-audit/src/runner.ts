import { defaultSecurityAuditPolicies } from './default-policies.js';
import type { RunSecurityAuditOptions, SecurityAuditFindingInput, SecurityAuditPolicyDefinition, SecurityAuditPolicyResult, SecurityAuditReport, SecurityAuditStatus, SecurityAuditSummary } from './types.js';

export async function seedDefaultSecurityAuditPolicies(prisma: any): Promise<{ count: number; policies: string[] }> {
  const policies: string[] = [];
  for (const policy of defaultSecurityAuditPolicies) {
    await prisma.securityAuditPolicy.upsert({
      where: { code: policy.code },
      create: { code: policy.code, title: policy.title, category: policy.category, severity: policy.severity, isEnabled: policy.enabled !== false, config: (policy.config ?? {}) as never },
      update: { title: policy.title, category: policy.category, severity: policy.severity, isEnabled: policy.enabled !== false, config: (policy.config ?? {}) as never },
    });
    policies.push(policy.code);
  }
  return { count: policies.length, policies };
}

export async function runSecurityAudit(options: RunSecurityAuditOptions): Promise<{ run?: any; report: SecurityAuditReport }> {
  const prisma = options.prisma;
  const version = options.version ?? '1.0.0-rc.5';
  const channel = options.channel ?? 'beta';
  const now = options.now ?? new Date();
  const policies = options.policies?.length ? options.policies : await policiesFromDatabaseOrDefaults(prisma);
  const results: SecurityAuditPolicyResult[] = [];
  for (const policy of policies.filter((item) => item.enabled !== false)) {
    const started = Date.now();
    try {
      const evaluated = await evaluatePolicy(prisma, policy, now);
      results.push({ ...evaluated, durationMs: Date.now() - started });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      results.push({ code: policy.code, title: policy.title, category: policy.category, severity: policy.severity, status: 'failed', summary: { error: message }, findings: [{ code: `${policy.code}.exception`, category: policy.category, severity: policy.severity, message, remediation: 'Проверить реализацию security audit policy и состояние БД.' }], durationMs: Date.now() - started });
    }
  }
  const summary = summarizeSecurityAudit(version, channel, results);
  const report: SecurityAuditReport = { version, channel, status: summary.status, summary, results, generatedAt: now.toISOString() };
  let run: any | undefined;
  if (options.persist !== false) {
    run = await prisma.securityAuditRun.create({ data: { version, channel, trigger: options.trigger ?? 'manual', status: summary.status, summary: summary as never, results: results as never, startedAt: now, finishedAt: new Date(), durationMs: results.reduce((sum, item) => sum + item.durationMs, 0), createdBy: options.createdBy } });
    for (const result of results) {
      await prisma.securityAuditPolicy.updateMany({ where: { code: result.code }, data: { lastStatus: result.status, lastRunAt: new Date() } });
      for (const finding of result.findings) {
        await prisma.securityAuditFinding.create({ data: { runId: run.id, code: finding.code, category: finding.category, severity: finding.severity, status: 'open', source: finding.source, subjectType: finding.subjectType, subjectId: finding.subjectId, message: finding.message, evidence: (finding.evidence ?? {}) as never, remediation: finding.remediation } });
      }
    }
    await prisma.securityRiskSnapshot.create({ data: { version, channel, status: summary.status, score: summary.riskScore, summary: summary as never, findingsBySeverity: findingsBySeverity(results) as never, recommendations: recommendations(results) as never, createdBy: options.createdBy } });
  }
  return { run, report };
}

export function summarizeSecurityAudit(version: string, channel: string, results: SecurityAuditPolicyResult[]): SecurityAuditSummary {
  const findings = results.flatMap((item) => item.findings);
  const critical = findings.filter((item) => item.severity === 'critical').length;
  const errors = findings.filter((item) => item.severity === 'error').length;
  const warnings = findings.filter((item) => item.severity === 'warning').length;
  const riskScore = Math.min(100, critical * 30 + errors * 12 + warnings * 4 + findings.filter((item) => item.severity === 'info').length);
  const failed = results.filter((item) => item.status === 'failed').length;
  const warning = results.filter((item) => item.status === 'warning').length;
  return {
    version,
    channel,
    totalPolicies: results.length,
    passed: results.filter((item) => item.status === 'passed').length,
    warning,
    failed,
    skipped: results.filter((item) => item.status === 'skipped').length,
    findings: findings.length,
    critical,
    errors,
    warnings,
    riskScore,
    status: critical > 0 || errors > 0 || failed > 0 ? 'failed' : warning > 0 || warnings > 0 ? 'warning' : 'passed',
  };
}

async function policiesFromDatabaseOrDefaults(prisma: any): Promise<SecurityAuditPolicyDefinition[]> {
  const rows = await prisma.securityAuditPolicy.findMany({ where: { isEnabled: true }, orderBy: [{ category: 'asc' }, { code: 'asc' }] }).catch(() => []);
  if (!rows.length) return defaultSecurityAuditPolicies;
  return rows.map((row: any) => ({ code: row.code, title: row.title, category: row.category, severity: row.severity, enabled: row.isEnabled, config: row.config ?? {} }));
}

async function evaluatePolicy(prisma: any, policy: SecurityAuditPolicyDefinition, now: Date): Promise<Omit<SecurityAuditPolicyResult, 'durationMs'>> {
  switch (policy.code) {
    case 'auth.admin_2fa_required': return admin2faRequired(prisma, policy);
    case 'auth.sessions_not_stale': return sessionsNotStale(prisma, policy, now);
    case 'tokens.expired_api_keys_revoked': return expiredApiKeysRevoked(prisma, policy, now);
    case 'tokens.api_keys_scoped': return apiKeysScoped(prisma, policy);
    case 'secrets.rotation_due': return secretsRotationDue(prisma, policy, now);
    case 'webhooks.secret_required': return webhookSecretRequired(prisma, policy);
    case 'rate_limit.violations_threshold': return rateLimitViolationsThreshold(prisma, policy, now);
    case 'release.no_open_security_blockers': return noOpenSecurityBlockers(prisma, policy);
    default:
      return result(policy, 'skipped', { reason: `Неизвестная security audit policy: ${policy.code}` }, []);
  }
}

async function admin2faRequired(prisma: any, policy: SecurityAuditPolicyDefinition) {
  const accounts = await prisma.adminAccount.findMany({ where: { isActive: true }, select: { id: true, email: true, twoFactorEnabled: true, twoFactorConfirmedAt: true } });
  const findings = accounts.filter((account: any) => !account.twoFactorEnabled || !account.twoFactorConfirmedAt).map((account: any) => finding(policy, `${policy.code}.missing_2fa`, `Admin account ${account.email} не имеет подтвержденной 2FA.`, { subjectType: 'admin_account', subjectId: account.id, evidence: { email: account.email, twoFactorEnabled: account.twoFactorEnabled, twoFactorConfirmedAt: account.twoFactorConfirmedAt }, remediation: 'Включить TOTP 2FA и сохранить recovery codes.' }));
  return result(policy, findings.length ? 'failed' : 'passed', { checked: accounts.length, missing2fa: findings.length }, findings);
}

async function sessionsNotStale(prisma: any, policy: SecurityAuditPolicyDefinition, now: Date) {
  const maxAgeDays = Number(policy.config?.maxAgeDays ?? 30);
  const before = new Date(now.getTime() - maxAgeDays * 86_400_000);
  const sessions = await prisma.adminSession.findMany({ where: { revokedAt: null, expiresAt: { gt: now }, createdAt: { lt: before } }, select: { id: true, accountId: true, createdAt: true, expiresAt: true, ip: true } });
  const findings = sessions.map((session: any) => finding(policy, `${policy.code}.stale_session`, `Активная session старше ${maxAgeDays} дней.`, { subjectType: 'admin_session', subjectId: session.id, evidence: { accountId: session.accountId, createdAt: session.createdAt, expiresAt: session.expiresAt, ip: session.ip }, remediation: 'Отозвать старые refresh sessions через cajeer security sessions:revoke-stale или API.' }));
  return result(policy, findings.length ? 'warning' : 'passed', { maxAgeDays, staleSessions: findings.length }, findings);
}

async function expiredApiKeysRevoked(prisma: any, policy: SecurityAuditPolicyDefinition, now: Date) {
  const keys = await prisma.apiKey.findMany({ where: { revokedAt: null, expiresAt: { lt: now } }, select: { id: true, title: true, expiresAt: true } });
  const findings = keys.map((key: any) => finding(policy, `${policy.code}.expired_active_key`, `API key ${key.title} истек, но не отозван.`, { subjectType: 'api_key', subjectId: key.id, evidence: { title: key.title, expiresAt: key.expiresAt }, remediation: 'Отозвать ключ или создать новый scoped key.' }));
  return result(policy, findings.length ? 'failed' : 'passed', { expiredActiveKeys: findings.length }, findings);
}

async function apiKeysScoped(prisma: any, policy: SecurityAuditPolicyDefinition) {
  const keys = await prisma.apiKey.findMany({ where: { revokedAt: null }, select: { id: true, title: true, scopes: true } });
  const findings = keys.filter((key: any) => !Array.isArray(key.scopes) || key.scopes.length === 0).map((key: any) => finding(policy, `${policy.code}.empty_scopes`, `API key ${key.title} активен без scopes.`, { subjectType: 'api_key', subjectId: key.id, evidence: { title: key.title, scopes: key.scopes }, remediation: 'Выдать минимальные scopes или отозвать ключ.' }));
  return result(policy, findings.length ? 'failed' : 'passed', { activeKeys: keys.length, emptyScopeKeys: findings.length }, findings);
}

async function secretsRotationDue(prisma: any, policy: SecurityAuditPolicyDefinition, now: Date) {
  const maxAgeDays = Number(policy.config?.maxAgeDays ?? 90);
  const before = new Date(now.getTime() - maxAgeDays * 86_400_000);
  const secrets = await prisma.secret.findMany({ where: { updatedAt: { lt: before } }, select: { id: true, key: true, updatedAt: true, rotatedAt: true, version: true } });
  const findings = secrets.map((secret: any) => finding(policy, `${policy.code}.rotation_due`, `Secret ${secret.key} старше ${maxAgeDays} дней.`, { subjectType: 'secret', subjectId: secret.id, evidence: { key: secret.key, updatedAt: secret.updatedAt, rotatedAt: secret.rotatedAt, version: secret.version }, remediation: 'Выполнить cajeer secrets set/rotate и обновить tokenRef, если требуется.' }));
  return result(policy, findings.length ? 'warning' : 'passed', { maxAgeDays, rotationDue: findings.length }, findings);
}

async function webhookSecretRequired(prisma: any, policy: SecurityAuditPolicyDefinition) {
  const endpoints = await prisma.webhookEndpoint.findMany({ where: { isActive: true }, select: { id: true, title: true, url: true, secretRef: true } });
  const findings = endpoints.filter((endpoint: any) => !endpoint.secretRef).map((endpoint: any) => finding(policy, `${policy.code}.missing_secret_ref`, `Webhook endpoint ${endpoint.title} активен без secretRef.`, { subjectType: 'webhook_endpoint', subjectId: endpoint.id, source: endpoint.url, evidence: { title: endpoint.title, url: endpoint.url }, remediation: 'Добавить secretRef и проверку подписи на принимающей стороне.' }));
  return result(policy, findings.length ? 'failed' : 'passed', { activeEndpoints: endpoints.length, missingSecretRef: findings.length }, findings);
}

async function rateLimitViolationsThreshold(prisma: any, policy: SecurityAuditPolicyDefinition, now: Date) {
  const windowHours = Number(policy.config?.windowHours ?? 24);
  const maxViolations = Number(policy.config?.maxViolations ?? 100);
  const since = new Date(now.getTime() - windowHours * 3_600_000);
  const count = await prisma.rateLimitViolation.count({ where: { createdAt: { gte: since } } });
  const findings = count > maxViolations ? [finding(policy, `${policy.code}.threshold_exceeded`, `Rate-limit violations за ${windowHours}ч: ${count}, порог: ${maxViolations}.`, { evidence: { count, windowHours, maxViolations }, remediation: 'Проверить источники нарушений, усилить лимиты, заблокировать abuse traffic.' })] : [];
  return result(policy, findings.length ? 'warning' : 'passed', { count, windowHours, maxViolations }, findings);
}

async function noOpenSecurityBlockers(prisma: any, policy: SecurityAuditPolicyDefinition) {
  const blockers = await prisma.releaseBlocker.findMany({ where: { status: 'open', OR: [{ severity: { in: ['blocker', 'critical', 'error'] } }, { code: { contains: 'security' } }, { source: { contains: 'security' } }] }, take: 100, orderBy: { createdAt: 'desc' } }).catch(() => []);
  const findings = blockers.map((blocker: any) => finding(policy, `${policy.code}.open_blocker`, `Открытый release/security blocker: ${blocker.message}`, { subjectType: 'release_blocker', subjectId: blocker.id, source: blocker.source, evidence: { code: blocker.code, severity: blocker.severity, createdAt: blocker.createdAt }, remediation: 'Закрыть blocker или явно принять риск перед RC.' }));
  return result(policy, findings.length ? 'failed' : 'passed', { openBlockers: findings.length }, findings);
}

function result(policy: SecurityAuditPolicyDefinition, status: SecurityAuditStatus, summary: Record<string, unknown>, findings: SecurityAuditFindingInput[]): Omit<SecurityAuditPolicyResult, 'durationMs'> {
  return { code: policy.code, title: policy.title, category: policy.category, severity: policy.severity, status, summary, findings };
}

function finding(policy: SecurityAuditPolicyDefinition, code: string, message: string, overrides: Partial<SecurityAuditFindingInput> = {}): SecurityAuditFindingInput {
  return { code, category: policy.category, severity: policy.severity, message, remediation: 'Устранить нарушение security policy.', ...overrides };
}

function findingsBySeverity(results: SecurityAuditPolicyResult[]) {
  const all = results.flatMap((item) => item.findings);
  return {
    critical: all.filter((item) => item.severity === 'critical').length,
    error: all.filter((item) => item.severity === 'error').length,
    warning: all.filter((item) => item.severity === 'warning').length,
    info: all.filter((item) => item.severity === 'info').length,
  };
}

function recommendations(results: SecurityAuditPolicyResult[]) {
  return results.flatMap((result) => result.findings.map((finding) => ({ code: finding.code, severity: finding.severity, message: finding.message, remediation: finding.remediation }))).slice(0, 50);
}
