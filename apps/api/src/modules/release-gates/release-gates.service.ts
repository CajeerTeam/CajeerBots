import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { buildReleaseCandidateSummary, defaultReleaseGates, evaluateReleaseGates, renderMarkdownReport, type ReleaseGateDefinition } from '@cajeerbots/release-gates';
import type { Principal } from '@cajeerbots/security';
import type { z } from 'zod';
import type { createReportSchema, listBlockersQuerySchema, listFreezesQuerySchema, listQualityGatesQuerySchema, listReportsQuerySchema, listRunsQuerySchema, resolveBlockerSchema, runGatesSchema, upsertFreezeSchema, upsertQualityGateSchema } from './release-gates.schemas.js';

type UpsertFreezeInput = z.infer<typeof upsertFreezeSchema>;
type UpsertGateInput = z.infer<typeof upsertQualityGateSchema>;
type RunGatesInput = z.infer<typeof runGatesSchema>;
type CreateReportInput = z.infer<typeof createReportSchema>;

@Injectable()
export class ReleaseGatesService {
  constructor(private readonly prisma: PrismaService) {}

  summary() {
    return Promise.all([
      this.prisma.releaseFreeze.findFirst({ orderBy: { createdAt: 'desc' } }),
      this.prisma.releaseGateRun.findFirst({ orderBy: { startedAt: 'desc' } }),
      this.prisma.releaseBlocker.count({ where: { status: 'open' } }),
      this.prisma.releaseQualityGate.count({ where: { isEnabled: true } }),
    ]).then(([freeze, lastRun, openBlockers, enabledGates]) => ({ freeze, lastRun, openBlockers, enabledGates, defaultGateCount: defaultReleaseGates.length }));
  }

  listFreezes(query: z.infer<typeof listFreezesQuerySchema>) {
    return this.prisma.releaseFreeze.findMany({ where: { ...(query.version ? { version: query.version } : {}), ...(query.channel ? { channel: query.channel } : {}), ...(query.status ? { status: query.status } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take });
  }

  async upsertFreeze(input: UpsertFreezeInput, principal: Principal) {
    const row = await this.prisma.releaseFreeze.upsert({
      where: { version_channel: { version: input.version, channel: input.channel } },
      create: { version: input.version, channel: input.channel, status: input.status, reason: input.reason, scope: input.scope as never, lockedBy: principal.id, lockedAt: new Date(), releasedAt: input.status === 'released' ? new Date() : null },
      update: { status: input.status, reason: input.reason, scope: input.scope as never, releasedAt: input.status === 'released' ? new Date() : null },
    });
    await this.audit(principal, 'release.freeze.upsert', 'release_freeze', row.id, input);
    return row;
  }

  async seedDefaultGates(principal: Principal) {
    const rows = [];
    for (const gate of defaultReleaseGates) {
      rows.push(await this.prisma.releaseQualityGate.upsert({
        where: { code: gate.code },
        create: { code: gate.code, title: gate.title, category: gate.category, severity: gate.severity, isEnabled: gate.enabled, config: (gate.config ?? {}) as never },
        update: { title: gate.title, category: gate.category, severity: gate.severity, isEnabled: gate.enabled, config: (gate.config ?? {}) as never },
      }));
    }
    await this.audit(principal, 'release.gates.seed_defaults', 'release_quality_gate', null, { count: rows.length });
    return { count: rows.length, gates: rows };
  }

  listQualityGates(query: z.infer<typeof listQualityGatesQuerySchema>) {
    return this.prisma.releaseQualityGate.findMany({ where: { ...(query.category ? { category: query.category } : {}), ...(query.enabled === undefined ? {} : { isEnabled: query.enabled }) }, orderBy: [{ category: 'asc' }, { code: 'asc' }], take: query.take });
  }

  async upsertQualityGate(input: UpsertGateInput, principal: Principal) {
    const row = await this.prisma.releaseQualityGate.upsert({ where: { code: input.code }, create: { code: input.code, title: input.title, category: input.category, severity: input.severity, isEnabled: input.isEnabled, config: input.config as never }, update: { title: input.title, category: input.category, severity: input.severity, isEnabled: input.isEnabled, config: input.config as never } });
    await this.audit(principal, 'release.gate.upsert', 'release_quality_gate', row.id, input);
    return row;
  }

  async runGates(input: RunGatesInput, principal: Principal) {
    if (input.useDatabaseGates) await this.ensureDefaultGates();
    const gates = input.useDatabaseGates ? await this.databaseGates() : defaultReleaseGates;
    const started = Date.now();
    const report = await evaluateReleaseGates({ rootDir: input.rootDir, version: input.version, channel: input.channel, gates });
    const run = await this.prisma.releaseGateRun.create({ data: { version: input.version, channel: input.channel, status: report.status, rootDir: input.rootDir, trigger: input.trigger, summary: report.summary as never, results: report.results as never, finishedAt: new Date(), durationMs: Date.now() - started, createdBy: principal.id } });
    for (const result of report.results) {
      await this.prisma.releaseQualityGate.updateMany({ where: { code: result.code }, data: { lastStatus: result.status, lastRunAt: new Date() } });
    }
    if (input.persistBlockers) {
      const issues = report.results.flatMap((gate) => gate.issues.map((issue) => ({ gate, issue })));
      for (const { gate, issue } of issues) {
        if (issue.severity !== 'blocker' && issue.severity !== 'error') continue;
        await this.prisma.releaseBlocker.create({ data: { runId: run.id, code: issue.code, severity: issue.severity, source: issue.source, message: issue.message, details: { gateCode: gate.code, ...(issue.details ?? {}) } as never } });
      }
    }
    await this.audit(principal, 'release.gates.run', 'release_gate_run', run.id, { version: input.version, channel: input.channel, status: report.status });
    return { run, report };
  }

  listRuns(query: z.infer<typeof listRunsQuerySchema>) {
    return this.prisma.releaseGateRun.findMany({ where: { ...(query.version ? { version: query.version } : {}), ...(query.channel ? { channel: query.channel } : {}), ...(query.status ? { status: query.status } : {}) }, orderBy: { startedAt: 'desc' }, take: query.take });
  }

  async run(id: string) {
    const row = await this.prisma.releaseGateRun.findUnique({ where: { id }, include: { blockers: true, reports: true } });
    if (!row) throw new NotFoundException('Release gate run не найден');
    return row;
  }

  listBlockers(query: z.infer<typeof listBlockersQuerySchema>) {
    return this.prisma.releaseBlocker.findMany({ where: { ...(query.status ? { status: query.status } : {}), ...(query.severity ? { severity: query.severity } : {}), ...(query.runId ? { runId: query.runId } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take });
  }

  async resolveBlocker(id: string, input: z.infer<typeof resolveBlockerSchema>, principal: Principal) {
    const row = await this.prisma.releaseBlocker.update({ where: { id }, data: { status: input.status, acceptedReason: input.reason, resolvedAt: input.status === 'open' ? null : new Date(), resolvedBy: input.status === 'open' ? null : principal.id } });
    await this.audit(principal, 'release.blocker.resolve', 'release_blocker', row.id, input);
    return row;
  }

  async createReport(input: CreateReportInput, principal: Principal) {
    let run = input.gateRunId ? await this.prisma.releaseGateRun.findUnique({ where: { id: input.gateRunId } }) : await this.prisma.releaseGateRun.findFirst({ where: { version: input.version, channel: input.channel }, orderBy: { startedAt: 'desc' } });
    let reportPayload: Awaited<ReturnType<typeof evaluateReleaseGates>>;
    if (!run && input.createRunIfMissing) {
      const created = await this.runGates({ version: input.version, channel: input.channel, rootDir: input.rootDir, trigger: 'report', persistBlockers: true, useDatabaseGates: true }, principal);
      run = created.run;
      reportPayload = created.report;
    } else if (run) {
      reportPayload = { version: run.version, channel: run.channel as never, status: run.status as never, summary: run.summary as never, results: run.results as never, generatedAt: new Date().toISOString() };
    } else throw new NotFoundException('Нет gate run для отчёта');
    const summary = buildReleaseCandidateSummary(reportPayload, run.id);
    const markdown = renderMarkdownReport(reportPayload);
    const report = await this.prisma.releaseCandidateReport.create({ data: { version: input.version, channel: input.channel, status: summary.status, gateRunId: run.id, summary: summary as never, markdown, artifactName: `release-gate-report-${input.version}.md`, createdBy: principal.id } });
    await this.audit(principal, 'release.report.create', 'release_candidate_report', report.id, { version: input.version, channel: input.channel, status: summary.status });
    return { report, summary, markdown };
  }

  listReports(query: z.infer<typeof listReportsQuerySchema>) {
    return this.prisma.releaseCandidateReport.findMany({ where: { ...(query.version ? { version: query.version } : {}), ...(query.channel ? { channel: query.channel } : {}), ...(query.status ? { status: query.status } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take });
  }

  private async ensureDefaultGates() {
    const count = await this.prisma.releaseQualityGate.count();
    if (count > 0) return;
    for (const gate of defaultReleaseGates) await this.prisma.releaseQualityGate.create({ data: { code: gate.code, title: gate.title, category: gate.category, severity: gate.severity, isEnabled: gate.enabled, config: (gate.config ?? {}) as never } });
  }

  private async databaseGates(): Promise<ReleaseGateDefinition[]> {
    const rows = await this.prisma.releaseQualityGate.findMany({ where: { isEnabled: true }, orderBy: [{ category: 'asc' }, { code: 'asc' }] });
    return rows.map((row) => ({ code: row.code, title: row.title, category: row.category, severity: row.severity as never, enabled: row.isEnabled, config: row.config as never }));
  }

  private audit(principal: Principal, action: string, resource: string, resourceId: string | null, metadata: unknown) {
    return this.prisma.auditLog.create({ data: { actorType: principal.type, actorId: principal.id, action, resource, resourceId, metadata: metadata as never } });
  }
}
