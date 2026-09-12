import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { seedDefaultSecurityAuditPolicies, runSecurityAudit } from '@cajeerbots/security-audit';
import type { Principal } from '@cajeerbots/security';
import type { z } from 'zod';
import type { listRiskSnapshotsQuerySchema, listSecurityAuditRunsQuerySchema, listSecurityFindingsQuerySchema, listSecurityPoliciesQuerySchema, resolveSecurityFindingSchema, runSecurityAuditSchema, upsertSecurityPolicySchema } from './security.schemas.js';

@Injectable()
export class SecurityService {
  constructor(private readonly prisma: PrismaService) {}

  async hardeningStatus() {
    const adminAccounts = await this.prisma.adminAccount.count();
    const active2faAccounts = await this.prisma.adminAccount.count({ where: { twoFactorEnabled: true, isActive: true } });
    const activeApiKeys = await this.prisma.apiKey.count({ where: { revokedAt: null, OR: [{ expiresAt: null }, { expiresAt: { gt: new Date() } }] } });
    const openSessions = await this.prisma.adminSession.count({ where: { revokedAt: null, expiresAt: { gt: new Date() } } });
    const openSecurityFindings = await this.prisma.securityAuditFinding.count({ where: { status: 'open', severity: { in: ['critical', 'error'] } } }).catch(() => 0);
    return {
      status: adminAccounts > 0 ? openSecurityFindings > 0 ? 'degraded' : 'ok' : 'bootstrap_required',
      adminAccounts,
      active2faAccounts,
      activeApiKeys,
      openSessions,
      openSecurityFindings,
      configuration: {
        jwtSecret: Boolean(process.env.JWT_SECRET),
        apiTokenPepper: Boolean(process.env.API_TOKEN_PEPPER),
        refreshTokenPepper: Boolean(process.env.REFRESH_TOKEN_PEPPER ?? process.env.API_TOKEN_PEPPER),
        passwordPepper: Boolean(process.env.PASSWORD_PEPPER ?? process.env.API_TOKEN_PEPPER),
        adminToken: Boolean(process.env.CAJEER_ADMIN_TOKEN),
      },
    };
  }

  rateLimitViolations(take = 100) {
    return this.prisma.rateLimitViolation.findMany({ orderBy: { createdAt: 'desc' }, take: Math.min(Math.max(take, 1), 500) });
  }

  async auditSummary() {
    const [policies, lastRun, openFindings, criticalFindings, snapshot] = await Promise.all([
      this.prisma.securityAuditPolicy.count({ where: { isEnabled: true } }).catch(() => 0),
      this.prisma.securityAuditRun.findFirst({ orderBy: { startedAt: 'desc' } }).catch(() => null),
      this.prisma.securityAuditFinding.count({ where: { status: 'open' } }).catch(() => 0),
      this.prisma.securityAuditFinding.count({ where: { status: 'open', severity: 'critical' } }).catch(() => 0),
      this.prisma.securityRiskSnapshot.findFirst({ orderBy: { createdAt: 'desc' } }).catch(() => null),
    ]);
    return { policies, lastRun, openFindings, criticalFindings, latestRiskSnapshot: snapshot };
  }

  async seedAuditDefaults(principal: Principal) {
    const result = await seedDefaultSecurityAuditPolicies(this.prisma);
    await this.audit(principal, 'security.audit.defaults.seed', 'security_audit_policy', null, result);
    return result;
  }

  listAuditPolicies(query: z.infer<typeof listSecurityPoliciesQuerySchema>) {
    return this.prisma.securityAuditPolicy.findMany({ where: { ...(query.category ? { category: query.category } : {}), ...(query.enabled === undefined ? {} : { isEnabled: query.enabled }) }, orderBy: [{ category: 'asc' }, { code: 'asc' }], take: query.take });
  }

  async upsertAuditPolicy(input: z.infer<typeof upsertSecurityPolicySchema>, principal: Principal) {
    const row = await this.prisma.securityAuditPolicy.upsert({ where: { code: input.code }, create: { code: input.code, title: input.title, category: input.category, severity: input.severity, isEnabled: input.isEnabled, config: input.config as never }, update: { title: input.title, category: input.category, severity: input.severity, isEnabled: input.isEnabled, config: input.config as never } });
    await this.audit(principal, 'security.audit.policy.upsert', 'security_audit_policy', row.id, input);
    return row;
  }

  async runAudit(input: z.infer<typeof runSecurityAuditSchema>, principal: Principal) {
    await seedDefaultSecurityAuditPolicies(this.prisma);
    const result = await runSecurityAudit({ prisma: this.prisma, version: input.version, channel: input.channel, trigger: input.trigger, persist: input.persist, createdBy: principal.sub });
    await this.audit(principal, 'security.audit.run', 'security_audit_run', result.run?.id ?? null, result.report.summary);
    return result;
  }

  listAuditRuns(query: z.infer<typeof listSecurityAuditRunsQuerySchema>) {
    return this.prisma.securityAuditRun.findMany({ where: { ...(query.version ? { version: query.version } : {}), ...(query.channel ? { channel: query.channel } : {}), ...(query.status ? { status: query.status } : {}) }, orderBy: { startedAt: 'desc' }, take: query.take });
  }

  async getAuditRun(id: string) {
    const run = await this.prisma.securityAuditRun.findUnique({ where: { id }, include: { findings: { orderBy: [{ severity: 'asc' }, { createdAt: 'desc' }] } } });
    if (!run) throw new NotFoundException('Security audit run не найден');
    return run;
  }

  listAuditFindings(query: z.infer<typeof listSecurityFindingsQuerySchema>) {
    return this.prisma.securityAuditFinding.findMany({ where: { ...(query.runId ? { runId: query.runId } : {}), ...(query.status ? { status: query.status } : {}), ...(query.severity ? { severity: query.severity } : {}), ...(query.category ? { category: query.category } : {}), ...(query.subjectType ? { subjectType: query.subjectType } : {}), ...(query.subjectId ? { subjectId: query.subjectId } : {}) }, orderBy: [{ status: 'asc' }, { severity: 'asc' }, { createdAt: 'desc' }], take: query.take });
  }

  async resolveFinding(id: string, input: z.infer<typeof resolveSecurityFindingSchema>, principal: Principal) {
    const row = await this.prisma.securityAuditFinding.update({ where: { id }, data: { status: input.status, resolvedAt: input.status === 'open' ? null : new Date(), resolvedBy: input.status === 'open' ? null : principal.sub, evidence: { resolutionReason: input.reason ?? null } as never } });
    await this.audit(principal, 'security.audit.finding.resolve', 'security_audit_finding', id, input);
    return row;
  }

  listRiskSnapshots(query: z.infer<typeof listRiskSnapshotsQuerySchema>) {
    return this.prisma.securityRiskSnapshot.findMany({ where: { ...(query.version ? { version: query.version } : {}), ...(query.channel ? { channel: query.channel } : {}), ...(query.status ? { status: query.status } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take });
  }

  private audit(principal: Principal, action: string, resource: string, resourceId: string | null, metadata: unknown) {
    return this.prisma.auditLog.create({ data: { actorId: principal.sub, actorType: principal.type, action, resource, resourceId, metadata: metadata as never } });
  }
}
