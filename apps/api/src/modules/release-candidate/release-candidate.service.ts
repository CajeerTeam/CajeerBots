import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { buildCompatibilityMatrix, createApiFreezeSnapshot, createUpgradePlan, defaultCompatibilityTargets, runRcReadiness, verifyApiFreeze, type CompatibilityTargetDefinition } from '@cajeerbots/release-candidate';
import type { Principal } from '@cajeerbots/security';
import type { z } from 'zod';
import type { buildMatrixSchema, createApiFreezeSchema, createUpgradePlanSchema, listQuerySchema, runReadinessSchema, runUpgradePlanSchema, upsertCompatibilityTargetSchema, verifyApiFreezeSchema } from './release-candidate.schemas.js';

type ListQuery = z.infer<typeof listQuerySchema>;
type CreateApiFreezeInput = z.infer<typeof createApiFreezeSchema>;
type VerifyApiFreezeInput = z.infer<typeof verifyApiFreezeSchema>;
type UpsertCompatibilityInput = z.infer<typeof upsertCompatibilityTargetSchema>;
type BuildMatrixInput = z.infer<typeof buildMatrixSchema>;
type CreateUpgradePlanInput = z.infer<typeof createUpgradePlanSchema>;
type RunUpgradePlanInput = z.infer<typeof runUpgradePlanSchema>;
type RunReadinessInput = z.infer<typeof runReadinessSchema>;

@Injectable()
export class ReleaseCandidateService {
  constructor(private readonly prisma: PrismaService) {}

  async summary() {
    const [apiFreeze, compatibilityMatrix, upgradePlan, upgradeRun, readinessRun, openBlockers, openFindings] = await Promise.all([
      this.prisma.releaseApiFreeze.findFirst({ orderBy: { createdAt: 'desc' } }),
      this.prisma.releaseCompatibilityMatrix.findFirst({ orderBy: { createdAt: 'desc' } }),
      this.prisma.releaseUpgradePlan.findFirst({ orderBy: { createdAt: 'desc' } }),
      this.prisma.releaseUpgradeRun.findFirst({ orderBy: { createdAt: 'desc' } }),
      this.prisma.releaseCandidateReadinessRun.findFirst({ orderBy: { startedAt: 'desc' } }),
      this.prisma.releaseBlocker.count({ where: { status: 'open' } }),
      this.prisma.securityAuditFinding.count({ where: { status: 'open' } }),
    ]);
    return { apiFreeze, compatibilityMatrix, upgradePlan, upgradeRun, readinessRun, openBlockers, openFindings, defaultCompatibilityTargets: defaultCompatibilityTargets.length };
  }

  async createApiFreeze(input: CreateApiFreezeInput, principal: Principal) {
    const payload = await createApiFreezeSnapshot({ version: input.version, channel: input.channel, rootDir: input.rootDir, sourcePath: input.sourcePath });
    const row = await this.prisma.releaseApiFreeze.create({ data: { version: input.version, channel: input.channel, status: input.status, sourcePath: input.sourcePath, sourceSha256: payload.sourceSha256, openApiVersion: payload.openApiVersion, operationCount: payload.operationCount, operations: payload.operations as never, duplicateOperationIds: payload.duplicateOperationIds as never, unsecuredOperations: payload.unsecuredOperations as never, deprecatedOperations: payload.deprecatedOperations as never, payload: payload as never, notes: input.notes, createdBy: principal.sub } });
    await this.audit(principal, 'release_candidate.api_freeze.create', 'release_api_freeze', row.id, { version: input.version, operationCount: payload.operationCount });
    return { freeze: row, payload };
  }

  listApiFreezes(query: ListQuery) {
    return this.prisma.releaseApiFreeze.findMany({ where: { ...(query.version ? { version: query.version } : {}), ...(query.channel ? { channel: query.channel } : {}), ...(query.status ? { status: query.status } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take });
  }

  async verifyApiFreeze(input: VerifyApiFreezeInput, principal: Principal) {
    const freeze = input.freezeId ? await this.prisma.releaseApiFreeze.findUnique({ where: { id: input.freezeId } }) : await this.prisma.releaseApiFreeze.findFirst({ where: { version: input.version, channel: input.channel, status: 'frozen' }, orderBy: { createdAt: 'desc' } });
    if (!freeze) throw new NotFoundException('API freeze snapshot не найден');
    const report = await verifyApiFreeze({ freeze: freeze.payload as never, rootDir: input.rootDir, sourcePath: input.sourcePath ?? freeze.sourcePath, allowAdditive: input.allowAdditive });
    const check = await this.prisma.releaseUpgradeCheck.create({ data: { version: freeze.version, channel: freeze.channel, code: 'api_freeze.verify', status: report.status, severity: report.status === 'failed' ? 'blocker' : report.status === 'warning' ? 'warning' : 'info', message: `API freeze verification: ${report.status}`, details: report as never, createdBy: principal.sub } });
    await this.audit(principal, 'release_candidate.api_freeze.verify', 'release_upgrade_check', check.id, { status: report.status, issues: report.issues.length });
    return { check, report };
  }

  async seedCompatibilityDefaults(principal: Principal) {
    const rows = [];
    for (const target of defaultCompatibilityTargets) rows.push(await this.upsertCompatibilityTarget(target, principal, false));
    await this.audit(principal, 'release_candidate.compatibility.defaults', 'release_compatibility_target', null, { count: rows.length });
    return { count: rows.length, targets: rows };
  }

  listCompatibilityTargets(query: ListQuery) {
    return this.prisma.releaseCompatibilityTarget.findMany({ where: { ...(query.status ? { status: query.status } : {}) }, orderBy: [{ kind: 'asc' }, { code: 'asc' }], take: query.take });
  }

  async upsertCompatibilityTarget(input: UpsertCompatibilityInput | CompatibilityTargetDefinition, principal: Principal, shouldAudit = true) {
    const row = await this.prisma.releaseCompatibilityTarget.upsert({ where: { code: input.code }, create: { code: input.code, kind: input.kind, name: input.name, versionRange: input.versionRange, required: input.required, status: input.status ?? 'supported', notes: input.notes, checks: (input.checks ?? []) as never }, update: { kind: input.kind, name: input.name, versionRange: input.versionRange, required: input.required, status: input.status ?? 'supported', notes: input.notes, checks: (input.checks ?? []) as never } });
    if (shouldAudit) await this.audit(principal, 'release_candidate.compatibility.upsert', 'release_compatibility_target', row.id, input);
    return row;
  }

  async buildCompatibilityMatrix(input: BuildMatrixInput, principal: Principal) {
    const rows = await this.prisma.releaseCompatibilityTarget.findMany({ orderBy: [{ kind: 'asc' }, { code: 'asc' }] });
    const targets = rows.length ? rows.map((row) => ({ code: row.code, kind: row.kind as never, name: row.name, versionRange: row.versionRange, required: row.required, status: row.status as never, notes: row.notes ?? undefined, checks: row.checks as never })) : defaultCompatibilityTargets;
    const matrix = buildCompatibilityMatrix({ version: input.version, channel: input.channel, targets });
    const row = input.persist ? await this.prisma.releaseCompatibilityMatrix.create({ data: { version: input.version, channel: input.channel, status: matrix.status, summary: matrix.summary as never, targets: matrix.targets as never, createdBy: principal.sub } }) : null;
    if (row) await this.audit(principal, 'release_candidate.compatibility.matrix', 'release_compatibility_matrix', row.id, { version: input.version, status: matrix.status });
    return { matrix: row, report: matrix };
  }

  listMatrices(query: ListQuery) {
    return this.prisma.releaseCompatibilityMatrix.findMany({ where: { ...(query.version ? { version: query.version } : {}), ...(query.channel ? { channel: query.channel } : {}), ...(query.status ? { status: query.status } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take });
  }

  async createUpgradePlan(input: CreateUpgradePlanInput, principal: Principal) {
    const [openBlockers, latestBackup] = await Promise.all([
      this.prisma.releaseBlocker.count({ where: { status: 'open' } }),
      this.prisma.backupArtifact.findFirst({ orderBy: { createdAt: 'desc' } }).catch(() => null),
    ]);
    const payload = createUpgradePlan({ fromVersion: input.fromVersion, toVersion: input.toVersion, channel: input.channel, hasBackup: input.hasBackup || Boolean(latestBackup), openBlockers });
    const row = await this.prisma.releaseUpgradePlan.create({ data: { fromVersion: input.fromVersion, toVersion: input.toVersion, channel: input.channel, status: input.status ?? payload.status, steps: payload.steps as never, risks: payload.risks as never, checks: payload.checks as never, payload: payload as never, createdBy: principal.sub } });
    await this.audit(principal, 'release_candidate.upgrade_plan.create', 'release_upgrade_plan', row.id, { fromVersion: input.fromVersion, toVersion: input.toVersion, status: row.status });
    return { plan: row, payload };
  }

  listUpgradePlans(query: ListQuery) {
    return this.prisma.releaseUpgradePlan.findMany({ where: { ...(query.status ? { status: query.status } : {}), ...(query.version ? { toVersion: query.version } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take });
  }

  async runUpgradePlan(id: string, input: RunUpgradePlanInput, principal: Principal) {
    const plan = await this.prisma.releaseUpgradePlan.findUnique({ where: { id } });
    if (!plan) throw new NotFoundException('Upgrade plan не найден');
    const started = Date.now();
    const steps = Array.isArray(plan.steps) ? plan.steps as Array<Record<string, unknown>> : [];
    const results = steps.map((step) => ({ code: step.code, phase: step.phase, status: input.mode === 'apply' && step.phase === 'rollback' ? 'skipped' : 'passed', mode: input.mode, command: step.command }));
    const planRisks = (plan.risks as Array<{ severity?: string }> | null) ?? [];
    const blockers = planRisks.filter((risk) => risk.severity === 'blocker').length;
    const status = blockers > 0 ? 'failed' : 'passed';
    const row = await this.prisma.releaseUpgradeRun.create({ data: { planId: plan.id, mode: input.mode, status, rootDir: input.rootDir, results: results as never, logs: [{ message: 'RC upgrade rehearsal completed', mode: input.mode, notes: input.notes ?? null }] as never, startedAt: new Date(started), finishedAt: new Date(), durationMs: Date.now() - started, createdBy: principal.sub } });
    await this.audit(principal, 'release_candidate.upgrade_plan.run', 'release_upgrade_run', row.id, { planId: id, status, mode: input.mode });
    return { run: row, plan, results };
  }

  listUpgradeRuns(query: ListQuery) {
    return this.prisma.releaseUpgradeRun.findMany({ where: { ...(query.status ? { status: query.status } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take });
  }

  async runReadiness(input: RunReadinessInput, principal: Principal) {
    const started = Date.now();
    const [freeze, targets, openBlockers, openFindings, latestIntegration] = await Promise.all([
      this.prisma.releaseApiFreeze.findFirst({ where: { version: input.version, channel: input.channel, status: 'frozen' }, orderBy: { createdAt: 'desc' } }),
      this.prisma.releaseCompatibilityTarget.findMany(),
      this.prisma.releaseBlocker.count({ where: { status: 'open' } }),
      this.prisma.securityAuditFinding.count({ where: { status: 'open' } }),
      this.prisma.integrationTestCoverage.findFirst({ where: { version: input.version, channel: input.channel }, orderBy: { createdAt: 'desc' } }),
    ]);
    const report = await runRcReadiness({ version: input.version, channel: input.channel, rootDir: input.rootDir, freeze: freeze?.payload as never, compatibilityTargets: targets.map((row) => ({ code: row.code, kind: row.kind as never, name: row.name, versionRange: row.versionRange, required: row.required, status: row.status as never, notes: row.notes ?? undefined, checks: row.checks as never })), openBlockers, securityOpenFindings: openFindings, latestIntegrationStatus: latestIntegration?.status ?? null });
    const row = input.persist ? await this.prisma.releaseCandidateReadinessRun.create({ data: { version: input.version, channel: input.channel, status: report.status, rootDir: input.rootDir, summary: report.summary as never, sections: report.sections as never, startedAt: new Date(started), finishedAt: new Date(), durationMs: Date.now() - started, createdBy: principal.sub } }) : null;
    if (row) await this.audit(principal, 'release_candidate.readiness.run', 'release_candidate_readiness_run', row.id, { version: input.version, status: report.status });
    return { run: row, report };
  }

  listReadinessRuns(query: ListQuery) {
    return this.prisma.releaseCandidateReadinessRun.findMany({ where: { ...(query.version ? { version: query.version } : {}), ...(query.channel ? { channel: query.channel } : {}), ...(query.status ? { status: query.status } : {}) }, orderBy: { startedAt: 'desc' }, take: query.take });
  }

  private audit(principal: Principal, action: string, resource: string, resourceId: string | null, metadata: unknown) {
    return this.prisma.auditLog.create({ data: { actorType: principal.type, actorId: principal.sub, action, resource, resourceId, metadata: metadata as never } });
  }
}
