import { collectObservabilitySnapshot, evaluateAlertRules, runDueProbes } from '@cajeerbots/observability';
import { checkDockerComposeProduction } from '@cajeerbots/deployment';
import { assertOpenApiSnapshot, buildOpenApiSnapshot, diffSnapshots, evaluateSdkCoverage } from '@cajeerbots/contracts';
import { checkDocumentationPages, scanWikiDirectory, sha256Hex } from '@cajeerbots/docs';
import { defaultReleaseGates, evaluateReleaseGates } from '@cajeerbots/release-gates';
import { calculateIntegrationCoverage, runIntegrationTestSuite } from '@cajeerbots/integration-tests';
import { runSecurityAudit, seedDefaultSecurityAuditPolicies } from '@cajeerbots/security-audit';
import { buildCompatibilityMatrix, runRcReadiness } from '@cajeerbots/release-candidate';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { nextRunAt } from './schedule.js';

export interface SchedulerPrismaLike {
  [key: string]: any;
}

export interface JobExecutionContext {
  prisma: SchedulerPrismaLike;
  job: any;
  runId?: string;
  triggeredBy?: string;
  now?: Date;
}

export interface JobExecutionResult {
  status: 'succeeded' | 'failed' | 'skipped';
  result?: Record<string, unknown>;
  error?: string;
  logs?: Array<Record<string, unknown>>;
}

export async function executeScheduledJob(context: JobExecutionContext): Promise<JobExecutionResult> {
  const type = String(context.job.type ?? 'built_in');
  if (type === 'built_in') return executeBuiltIn(context);
  if (type === 'event_publish') return publishEvent(context);
  if (type === 'http') return callHttp(context);
  return { status: 'failed', error: `Неизвестный тип scheduled job: ${type}` };
}

export function calculateJobNextRun(job: { schedule?: string | null }, from = new Date()): Date | null {
  return nextRunAt(job.schedule ?? 'manual', from);
}

async function executeBuiltIn(context: JobExecutionContext): Promise<JobExecutionResult> {
  const handler = String(context.job.handler ?? context.job.code);
  const prisma = context.prisma;
  const logs: Array<Record<string, unknown>> = [];
  switch (handler) {
    case 'maintenance.cleanup_expired_idempotency': {
      const deleted = await prisma.idempotencyKey.deleteMany({ where: { expiresAt: { lt: context.now ?? new Date() } } });
      logs.push({ action: 'deleteMany', model: 'idempotencyKey', count: deleted.count });
      return { status: 'succeeded', result: { deleted: deleted.count }, logs };
    }
    case 'maintenance.event_retention': {
      const topics = await prisma.eventTopic.findMany({ where: { isActive: true } });
      let deleted = 0;
      for (const topic of topics) {
        const retentionDays = Number(topic.retentionDays ?? 30);
        const before = new Date((context.now ?? new Date()).getTime() - retentionDays * 86_400_000);
        const result = await prisma.eventRecord.deleteMany({ where: { topicName: topic.name, publishedAt: { lt: before } } });
        deleted += result.count;
        logs.push({ action: 'deleteMany', topic: topic.name, retentionDays, count: result.count });
      }
      return { status: 'succeeded', result: { deleted }, logs };
    }
    case 'maintenance.retry_stale_processing': {
      const now = context.now ?? new Date();
      const olderThan = new Date(now.getTime() - Number(context.job.payload?.olderThanSeconds ?? 900) * 1000);
      const webhookDeliveries = await prisma.webhookDelivery.updateMany({ where: { status: 'processing', updatedAt: { lt: olderThan } }, data: { status: 'failed', lastError: 'stale_processing_requeued_by_scheduler', availableAt: now } });
      const eventDeliveries = await prisma.eventSubscriptionDelivery.updateMany({ where: { status: 'processing', updatedAt: { lt: olderThan } }, data: { status: 'failed', lockedAt: null, lastError: 'stale_processing_requeued_by_scheduler', availableAt: now } });
      const botMessages = await prisma.botOutboundMessage.updateMany({ where: { status: 'sending', updatedAt: { lt: olderThan } }, data: { status: 'failed', lastError: 'stale_sending_requeued_by_scheduler', availableAt: now } });
      return { status: 'succeeded', result: { webhookDeliveries: webhookDeliveries.count, eventDeliveries: eventDeliveries.count, botMessages: botMessages.count }, logs };
    }
    case 'maintenance.diagnostics_snapshot': {
      const [outboxPending, outboxDead, eventPending, eventDead, botPending, botDead, extensionPending, extensionDead] = await Promise.all([
        prisma.outboxMessage.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
        prisma.outboxMessage.count({ where: { status: 'dead' } }),
        prisma.eventSubscriptionDelivery.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
        prisma.eventSubscriptionDelivery.count({ where: { status: 'dead' } }),
        prisma.botOutboundMessage.count({ where: { status: { in: ['pending', 'failed', 'sending'] } } }),
        prisma.botOutboundMessage.count({ where: { status: 'dead' } }),
        prisma.extensionRuntimeEvent.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }),
        prisma.extensionRuntimeEvent.count({ where: { status: 'dead' } }),
      ]);
      const metrics = { outboxPending, outboxDead, eventPending, eventDead, botPending, botDead, extensionPending, extensionDead };
      const status = Object.entries(metrics).some(([key, value]) => key.endsWith('Dead') && Number(value) > 0) ? 'degraded' : 'ok';
      const snapshot = await prisma.maintenanceSnapshot.create({ data: { kind: 'diagnostics', status, metrics: metrics as never, startedAt: context.now ?? new Date(), finishedAt: new Date() } });
      return { status: 'succeeded', result: { snapshotId: snapshot.id, ...metrics }, logs };
    }
    case 'maintenance.observability_collect': {
      const payload = context.job.payload ?? {};
      const snapshot = await collectObservabilitySnapshot(context.prisma, { includeDiagnostics: (payload.includeDiagnostics as boolean | undefined) ?? true });
      const probes = (payload.runProbes as boolean | undefined) === false ? { checked: 0 } : await runDueProbes(context.prisma);
      const alerts = (payload.evaluateAlerts as boolean | undefined) === false ? { evaluated: 0, fired: 0, resolved: 0 } : await evaluateAlertRules(context.prisma);
      return { status: 'succeeded', result: { snapshot: { inserted: snapshot.inserted, deletedOldSamples: snapshot.deletedOldSamples }, probes, alerts }, logs: [{ message: 'observability snapshot collected' }] };
    }
    case 'maintenance.deployment_readiness': {
      const targetCode = String(context.job.payload?.targetCode ?? 'docker.production');
      const rootDir = String(context.job.payload?.rootDir ?? process.cwd());
      const target = await prisma.deploymentTarget.findUnique({ where: { code: targetCode } });
      const checks = await checkDockerComposeProduction(rootDir, target?.composeFiles?.length ? [...target.composeFiles, target.envFile ?? '.env.production'] : ['docker-compose.prod.yml', '.env.production']);
      const status = checks.some((check) => check.status === 'fail') ? 'failed' : checks.some((check) => check.status === 'warn') ? 'warning' : 'ready';
      const created = [];
      for (const check of checks) {
        created.push(await prisma.deploymentCheck.create({ data: { targetId: target?.id ?? null, code: check.code, status: check.status, severity: check.severity, message: check.message, details: { ...(check.details ?? {}), rootDir } as never } }));
      }
      if (target) await prisma.deploymentTarget.update({ where: { id: target.id }, data: { status, lastCheckedAt: new Date() } });
      return { status: status === 'failed' ? 'failed' : 'succeeded', result: { targetCode, status, checks: created.length }, logs: checks.map((check) => ({ code: check.code, status: check.status, message: check.message })) };
    }

    case 'maintenance.contracts_check': {
      const sourcePath = String(context.job.payload?.sourcePath ?? 'contracts/openapi/cajeerbots.openapi.yaml');
      const sdkSourcePath = String(context.job.payload?.sdkSourcePath ?? 'packages/sdk/src/client.ts');
      const minSdkCoveragePercent = Number(context.job.payload?.minSdkCoveragePercent ?? 50);
      const openApiText = await readFile(resolve(process.cwd(), sourcePath), 'utf8');
      const snapshot = buildOpenApiSnapshot(openApiText, context.now ?? new Date());
      const assertion = assertOpenApiSnapshot(snapshot);
      const sdkSource = await readFile(resolve(process.cwd(), sdkSourcePath), 'utf8').catch(() => '');
      const coverage = evaluateSdkCoverage(snapshot, sdkSource);
      const created = await prisma.apiContractSnapshot.create({ data: { version: snapshot.version, sourcePath, sourceSha256: snapshot.sourceSha256, operationCount: snapshot.operations.length, pathCount: snapshot.paths, tagCount: snapshot.tags.length, duplicateOperationIds: snapshot.duplicateOperationIds, unsecuredOperations: snapshot.unsecuredOperations as never, deprecatedOperations: snapshot.deprecatedOperations as never, snapshot: snapshot as never, createdBy: 'scheduler' } });
      const baseline = await prisma.apiContractSnapshot.findFirst({ where: { isBaseline: true, version: snapshot.version, id: { not: created.id } }, orderBy: { createdAt: 'desc' } });
      const diff = baseline ? diffSnapshots(baseline.snapshot as never, snapshot) : undefined;
      const issues = [
        ...assertion.issues.map((message) => ({ type: 'openapi', message })),
        ...(diff?.breaking ?? []).map((issue) => ({ type: 'breaking_change', ...issue })),
        ...(coverage.percent < minSdkCoveragePercent ? [{ type: 'sdk_coverage', message: `SDK coverage ${coverage.percent}% ниже порога ${minSdkCoveragePercent}%` }] : []),
      ];
      const status = issues.length > 0 ? 'failed' : 'succeeded';
      await prisma.apiContractCheck.create({ data: { snapshotId: created.id, baselineId: baseline?.id ?? null, status: status === 'succeeded' ? 'passed' : 'failed', checkType: 'scheduled_contract_first', summary: { diff: diff?.summary ?? null, sdkCoverage: coverage, operations: snapshot.operations.length } as never, issues: issues as never, breakingCount: diff?.summary.breaking ?? 0, coveragePercent: coverage.percent, createdBy: 'scheduler' } });
      return { status, result: { snapshotId: created.id, baselineId: baseline?.id, operations: snapshot.operations.length, issues: issues.length, sdkCoverage: coverage.percent }, logs: [{ message: 'contract-first check completed', sourcePath, sdkSourcePath }] };
    }


    case 'maintenance.docs_sync': {
      const collectionCode = String(context.job.payload?.collectionCode ?? 'gitflic-wiki');
      const rootDir = String(context.job.payload?.rootDir ?? 'wiki');
      const includeDrafts = Boolean(context.job.payload?.includeDrafts ?? false);
      const collection = await prisma.documentationCollection.upsert({
        where: { code: collectionCode },
        create: { code: collectionCode, title: collectionCode === 'gitflic-wiki' ? 'GitFlic Wiki' : collectionCode, locale: 'ru', sourceType: 'local_wiki', sourcePath: rootDir },
        update: { sourcePath: rootDir, isActive: true },
      });
      const scanned = await scanWikiDirectory({ rootDir: resolve(process.cwd(), rootDir), includeDrafts });
      let created = 0;
      let updated = 0;
      let unchanged = 0;
      const seen = new Set<string>();
      const existing = await prisma.documentationPage.findMany({ where: { collectionId: collection.id } });
      const existingBySlug = new Map(existing.map((page: any) => [page.slug, page]));
      for (const page of scanned.pages) {
        seen.add(page.slug);
        const previous = existingBySlug.get(page.slug) as any | undefined;
        const data = { title: page.title, path: page.path, locale: collection.locale, status: 'published', body: page.body, excerpt: page.excerpt, headings: page.headings as never, links: page.links as never, frontmatter: page.frontmatter as never, wordCount: page.wordCount, sizeBytes: page.sizeBytes, sha256: page.sha256, sourceMtime: page.mtime, syncedAt: scanned.scannedAt, metadata: { sourcePath: page.sourcePath } as never };
        if (!previous) { await prisma.documentationPage.create({ data: { collectionId: collection.id, slug: page.slug, ...data } }); created += 1; }
        else if (previous.sha256 !== page.sha256) { await prisma.documentationPage.update({ where: { id: previous.id }, data }); updated += 1; }
        else { await prisma.documentationPage.update({ where: { id: previous.id }, data: { syncedAt: scanned.scannedAt, status: 'published' } }); unchanged += 1; }
      }
      const missing = existing.filter((page: any) => !seen.has(page.slug));
      if (missing.length > 0) await prisma.documentationPage.updateMany({ where: { id: { in: missing.map((page: any) => page.id) } }, data: { status: 'missing', syncedAt: scanned.scannedAt } });
      const sourceHash = sha256Hex(scanned.pages.map((page) => `${page.slug}:${page.sha256}`).join('\n'));
      const snapshot = await prisma.documentationSnapshot.create({ data: { collectionId: collection.id, version: collection.version + 1, sourceHash, pageCount: scanned.pages.length, totalWords: scanned.pages.reduce((sum, page) => sum + page.wordCount, 0), totalBytes: scanned.totalBytes, toc: scanned.pages.map((page) => ({ slug: page.slug, title: page.title, path: page.path })) as never, metadata: { rootDir, created, updated, unchanged, missing: missing.length } as never, createdBy: 'scheduler' } });
      await prisma.documentationCollection.update({ where: { id: collection.id }, data: { lastSyncedAt: scanned.scannedAt, version: { increment: 1 } } });
      return { status: 'succeeded', result: { snapshotId: snapshot.id, pages: scanned.pages.length, created, updated, unchanged, missing: missing.length }, logs: [{ message: 'documentation synced', collectionCode, rootDir }] };
    }
    case 'maintenance.docs_check': {
      const collectionCode = String(context.job.payload?.collectionCode ?? 'gitflic-wiki');
      const rootDir = String(context.job.payload?.rootDir ?? 'wiki');
      const collection = await prisma.documentationCollection.upsert({ where: { code: collectionCode }, create: { code: collectionCode, title: 'GitFlic Wiki', locale: 'ru', sourceType: 'local_wiki', sourcePath: rootDir }, update: { sourcePath: rootDir, isActive: true } });
      const scanned = await scanWikiDirectory({ rootDir: resolve(process.cwd(), rootDir), includeDrafts: Boolean(context.job.payload?.includeDrafts ?? false) });
      const result = checkDocumentationPages(scanned.pages);
      const check = await prisma.documentationCheck.create({ data: { collectionId: collection.id, checkType: 'scheduled_local_wiki', status: result.status, summary: result.summary as never, issues: result.issues as never, createdBy: 'scheduler' } });
      return { status: result.status === 'failed' ? 'failed' : 'succeeded', result: { checkId: check.id, ...result.summary }, logs: result.issues.slice(0, 50).map((issue) => ({ code: issue.code, severity: issue.severity, pageSlug: issue.pageSlug, message: issue.message })) };
    }


    case 'maintenance.release_gates': {
      const version = String(context.job.payload?.version ?? '1.0.0-rc.2');
      const channel = String(context.job.payload?.channel ?? 'beta') as never;
      const rootDir = String(context.job.payload?.rootDir ?? process.cwd());
      const databaseGates = await prisma.releaseQualityGate.findMany({ where: { isEnabled: true }, orderBy: [{ category: 'asc' }, { code: 'asc' }] }).catch(() => []);
      const gates = databaseGates.length > 0 ? databaseGates.map((gate: any) => ({ code: gate.code, title: gate.title, category: gate.category, severity: gate.severity, enabled: gate.isEnabled, config: gate.config ?? {} })) : defaultReleaseGates;
      const started = Date.now();
      const report = await evaluateReleaseGates({ rootDir, version, channel, gates });
      const run = await prisma.releaseGateRun.create({ data: { version, channel, status: report.status, rootDir, trigger: 'scheduler', summary: report.summary as never, results: report.results as never, finishedAt: new Date(), durationMs: Date.now() - started, createdBy: 'scheduler' } });
      for (const gate of report.results) await prisma.releaseQualityGate.updateMany({ where: { code: gate.code }, data: { lastStatus: gate.status, lastRunAt: new Date() } });
      for (const gate of report.results) {
        for (const issue of gate.issues) {
          if (issue.severity !== 'blocker' && issue.severity !== 'error') continue;
          await prisma.releaseBlocker.create({ data: { runId: run.id, code: issue.code, severity: issue.severity, source: issue.source, message: issue.message, details: { gateCode: gate.code, ...(issue.details ?? {}) } as never } });
        }
      }
      return { status: report.status === 'failed' ? 'failed' : 'succeeded', result: { runId: run.id, ...report.summary, status: report.status }, logs: report.results.map((gate) => ({ code: gate.code, status: gate.status, issues: gate.issues.length })) };
    }


    case 'maintenance.integration_smoke': {
      const suiteCode = String(context.job.payload?.suiteCode ?? 'mvp-smoke');
      const version = String(context.job.payload?.version ?? '1.0.0-rc.2');
      const channel = String(context.job.payload?.channel ?? 'beta');
      const suite = await prisma.integrationTestSuite.findUnique({ where: { code: suiteCode } });
      if (!suite) return { status: 'failed', error: `Integration suite не найден: ${suiteCode}` };
      const cases = await prisma.integrationTestCase.findMany({ where: { suiteId: suite.id, isActive: true }, orderBy: [{ priority: 'asc' }, { code: 'asc' }] });
      const result = await runIntegrationTestSuite({ prisma, suite, cases, version, channel, dryRun: Boolean(context.job.payload?.dryRun ?? true), triggeredBy: 'scheduler', baseUrl: context.job.payload?.baseUrl ? String(context.job.payload.baseUrl) : undefined });
      const coverage = await calculateIntegrationCoverage(prisma, { version, channel, createdBy: 'scheduler' });
      return {
        status: result.summary.status === 'failed' ? 'failed' : 'succeeded',
        result: { runId: result.run.id, suiteCode, ...result.summary, coverage: coverage.summary },
        logs: result.cases.map((item: any) => ({ caseId: item.run.caseId, status: item.summary.status, passed: item.summary.passed, failed: item.summary.failed })),
      };
    }



    case 'maintenance.rc_readiness': {
      const payload = context.job.payload ?? {};
      const version = String(payload.version ?? '1.0.0-rc.2');
      const channel = String(payload.channel ?? 'rc') as never;
      const rootDir = String(payload.rootDir ?? process.cwd());
      const [freeze, targets, openBlockers, openFindings, latestIntegration] = await Promise.all([
        prisma.releaseApiFreeze.findFirst({ where: { version, channel, status: 'frozen' }, orderBy: { createdAt: 'desc' } }),
        prisma.releaseCompatibilityTarget.findMany(),
        prisma.releaseBlocker.count({ where: { status: 'open' } }),
        prisma.securityAuditFinding.count({ where: { status: 'open' } }),
        prisma.integrationTestCoverage.findFirst({ where: { version, channel }, orderBy: { createdAt: 'desc' } }),
      ]);
      const compatibilityTargets = targets.map((row: any) => ({ code: row.code, kind: row.kind, name: row.name, versionRange: row.versionRange, required: row.required, status: row.status, notes: row.notes ?? undefined, checks: row.checks ?? [] }));
      if (compatibilityTargets.length) {
        const matrix = buildCompatibilityMatrix({ version, channel, targets: compatibilityTargets });
        await prisma.releaseCompatibilityMatrix.create({ data: { version, channel, status: matrix.status, summary: matrix.summary as never, targets: matrix.targets as never, createdBy: context.triggeredBy ?? 'scheduler' } });
      }
      const report = await runRcReadiness({ version, channel, rootDir, freeze: freeze?.payload, compatibilityTargets, openBlockers, securityOpenFindings: openFindings, latestIntegrationStatus: latestIntegration?.status ?? null });
      const run = await prisma.releaseCandidateReadinessRun.create({ data: { version, channel, status: report.status, rootDir, summary: report.summary as never, sections: report.sections as never, startedAt: new Date(), finishedAt: new Date(), createdBy: context.triggeredBy ?? 'scheduler' } });
      return { status: report.status === 'failed' ? 'failed' : 'succeeded', result: { runId: run.id, status: report.status, summary: report.summary }, logs: report.sections.map((section) => ({ code: section.code, status: section.status, issues: section.issues.length })) };
    }

    case 'maintenance.security_audit': {
      const version = String(context.job.payload?.version ?? '1.0.0-rc.2');
      const channel = String(context.job.payload?.channel ?? 'beta');
      await seedDefaultSecurityAuditPolicies(prisma);
      const result = await runSecurityAudit({ prisma, version, channel, trigger: 'scheduler', createdBy: 'scheduler', persist: context.job.payload?.persist !== false, now: context.now ?? new Date() });
      return {
        status: result.report.status === 'failed' ? 'failed' : 'succeeded',
        result: { runId: result.run?.id ?? null, ...result.report.summary },
        logs: result.report.results.map((policy) => ({ code: policy.code, status: policy.status, findings: policy.findings.length })),
      };
    }

    case 'maintenance.cleanup_render_logs': {
      const days = Number(context.job.payload?.retentionDays ?? 14);
      const before = new Date((context.now ?? new Date()).getTime() - days * 86_400_000);
      const deleted = await prisma.messageRenderLog.deleteMany({ where: { createdAt: { lt: before } } });
      return { status: 'succeeded', result: { deleted: deleted.count, retentionDays: days }, logs };
    }
    case 'maintenance.backup_retention': {
      const days = Number(context.job.payload?.deletedObjectRetentionDays ?? 7);
      const before = new Date((context.now ?? new Date()).getTime() - days * 86_400_000);
      const purged = await prisma.storageObject.updateMany({ where: { status: 'deleted', deletedAt: { lt: before } }, data: { status: 'purge_pending' } });
      const oldRuns = await prisma.backupRestoreRun.deleteMany({ where: { createdAt: { lt: new Date((context.now ?? new Date()).getTime() - 90 * 86_400_000) }, status: { in: ['completed', 'failed'] } } });
      return { status: 'succeeded', result: { purgePendingObjects: purged.count, deletedRestoreRuns: oldRuns.count, deletedObjectRetentionDays: days }, logs };
    }
    default:
      return { status: 'failed', error: `Неизвестный built-in handler: ${handler}` };
  }
}

async function publishEvent(context: JobExecutionContext): Promise<JobExecutionResult> {
  const prisma = context.prisma;
  const payload = context.job.payload ?? {};
  const topicName = String(payload.topic ?? `scheduler.${context.job.code}`);
  const type = String(payload.type ?? topicName);
  const eventId = payload.eventId ? String(payload.eventId) : createHash('sha256').update(`${context.job.id}:${context.runId ?? ''}:${Date.now()}`).digest('hex');
  const topic = await prisma.eventTopic.upsert({ where: { name: topicName }, create: { name: topicName, description: `Topic scheduled job ${context.job.code}` }, update: {} });
  const record = await prisma.eventRecord.create({ data: { eventId, topicId: topic.id, topicName, type, source: 'scheduler', payload: { type, topic: topicName, payload: payload.payload ?? {}, schedulerJobId: context.job.id, schedulerRunId: context.runId } as never, metadata: { scheduledJobCode: context.job.code } as never } });
  return { status: 'succeeded', result: { eventRecordId: record.id, eventId, topic: topicName } };
}

async function callHttp(context: JobExecutionContext): Promise<JobExecutionResult> {
  const payload = context.job.payload ?? {};
  const url = String(payload.url ?? '');
  if (!url.startsWith('http://') && !url.startsWith('https://')) return { status: 'failed', error: 'HTTP job требует payload.url с http/https' };
  const method = String(payload.method ?? 'POST').toUpperCase();
  const body = payload.body === undefined ? undefined : JSON.stringify(payload.body);
  const response = await fetch(url, { method, headers: { 'content-type': 'application/json', ...(payload.headers ?? {}) }, body });
  const text = await response.text();
  return response.ok
    ? { status: 'succeeded', result: { status: response.status, body: text.slice(0, 2000) } }
    : { status: 'failed', error: `HTTP ${response.status}: ${text.slice(0, 1000)}`, result: { status: response.status } };
}
