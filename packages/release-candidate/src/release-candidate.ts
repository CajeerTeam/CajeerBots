import { readFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { buildOpenApiSnapshot, diffSnapshots, type OpenApiContractSnapshot } from '@cajeerbots/contracts';
import { defaultCompatibilityTargets, defaultUpgradeSteps } from './defaults.js';
import type { ApiFreezeSnapshotPayload, ApiFreezeVerificationIssue, ApiFreezeVerificationReport, CompatibilityMatrixReport, CompatibilityTargetDefinition, RcReadinessReport, ReleaseCandidateChannel, UpgradePlanPayload } from './types.js';

export async function createApiFreezeSnapshot(input: { rootDir?: string; sourcePath?: string; version: string; channel?: ReleaseCandidateChannel; now?: Date }): Promise<ApiFreezeSnapshotPayload> {
  const sourcePath = input.sourcePath ?? 'contracts/openapi/cajeerbots.openapi.yaml';
  const openApiText = await readFile(resolve(input.rootDir ?? process.cwd(), sourcePath), 'utf8');
  const snapshot = buildOpenApiSnapshot(openApiText, input.now ?? new Date());
  return {
    format: 'cajeerbots.api-freeze.v1',
    version: input.version,
    channel: input.channel ?? 'rc',
    sourcePath,
    sourceSha256: snapshot.sourceSha256,
    openApiVersion: snapshot.version,
    operationCount: snapshot.operations.length,
    operations: snapshot.operations.map((operation) => ({ method: operation.method, path: operation.path, operationId: operation.operationId, deprecated: operation.deprecated, tags: operation.tags })),
    duplicateOperationIds: snapshot.duplicateOperationIds,
    unsecuredOperations: snapshot.unsecuredOperations.map((operation) => ({ ...operation, tags: [] })),
    deprecatedOperations: snapshot.deprecatedOperations.map((operation) => ({ ...operation, tags: [] })),
    generatedAt: (input.now ?? new Date()).toISOString(),
  };
}

export async function verifyApiFreeze(input: { freeze: ApiFreezeSnapshotPayload; rootDir?: string; sourcePath?: string; now?: Date; allowAdditive?: boolean }): Promise<ApiFreezeVerificationReport> {
  const sourcePath = input.sourcePath ?? input.freeze.sourcePath;
  const text = await readFile(resolve(input.rootDir ?? process.cwd(), sourcePath), 'utf8');
  const current = buildOpenApiSnapshot(text, input.now ?? new Date());
  const frozen = freezeToOpenApiSnapshot(input.freeze);
  const diff = diffSnapshots(frozen, current);
  const removed = input.freeze.operations.filter((operation) => !current.operations.some((item) => item.method === operation.method && item.path === operation.path));
  const added = current.operations.filter((operation) => !input.freeze.operations.some((item) => item.method === operation.method && item.path === operation.path));
  const changedOperationIds = input.freeze.operations.flatMap((before) => {
    const after = current.operations.find((item) => item.method === before.method && item.path === before.path);
    return after && after.operationId !== before.operationId ? [{ before, after: { method: after.method, path: after.path, operationId: after.operationId, deprecated: after.deprecated, tags: after.tags } }] : [];
  });
  const issues: ApiFreezeVerificationIssue[] = [];
  for (const operation of removed) issues.push({ code: 'api_freeze.operation_removed', severity: 'blocker', source: `${operation.method.toUpperCase()} ${operation.path}`, message: `Операция удалена после API freeze: ${operation.method.toUpperCase()} ${operation.path}` });
  for (const operation of changedOperationIds) issues.push({ code: 'api_freeze.operation_id_changed', severity: 'blocker', source: `${operation.before.method.toUpperCase()} ${operation.before.path}`, message: `operationId изменён: ${operation.before.operationId} -> ${operation.after.operationId}` });
  if (!input.allowAdditive) for (const operation of added) issues.push({ code: 'api_freeze.operation_added', severity: 'warning', source: `${operation.method.toUpperCase()} ${operation.path}`, message: `Новая операция после freeze: ${operation.method.toUpperCase()} ${operation.path}` });
  for (const duplicate of current.duplicateOperationIds) issues.push({ code: 'api_freeze.duplicate_operation_id', severity: 'blocker', message: `Дублирующий operationId в текущем OpenAPI: ${duplicate}` });
  for (const item of diff.breaking) issues.push({ code: 'api_freeze.breaking_change', severity: 'blocker', source: `${item.method.toUpperCase()} ${item.path}`, message: `Breaking change: ${item.reason}`, details: item as Record<string, unknown> });
  const blockers = issues.filter((issue) => issue.severity === 'blocker' || issue.severity === 'error').length;
  return { status: blockers > 0 ? 'failed' : issues.length > 0 ? 'warning' : 'passed', version: input.freeze.version, channel: input.freeze.channel, frozenOperations: input.freeze.operationCount, currentOperations: current.operations.length, removedOperations: removed, addedOperations: added.map((operation) => ({ method: operation.method, path: operation.path, operationId: operation.operationId, deprecated: operation.deprecated, tags: operation.tags })), changedOperationIds, issues, generatedAt: (input.now ?? new Date()).toISOString() };
}

export function buildCompatibilityMatrix(input: { version: string; channel?: ReleaseCandidateChannel; targets?: CompatibilityTargetDefinition[]; now?: Date }): CompatibilityMatrixReport {
  const targets = input.targets?.length ? input.targets : defaultCompatibilityTargets;
  const summary = {
    total: targets.length,
    required: targets.filter((target) => target.required).length,
    supported: targets.filter((target) => (target.status ?? 'supported') === 'supported').length,
    experimental: targets.filter((target) => target.status === 'experimental').length,
    deprecated: targets.filter((target) => target.status === 'deprecated').length,
    blocked: targets.filter((target) => target.status === 'blocked').length,
  };
  const requiredBlocked = targets.some((target) => target.required && target.status === 'blocked');
  const requiredExperimental = targets.some((target) => target.required && target.status === 'experimental');
  return { format: 'cajeerbots.compatibility-matrix.v1', version: input.version, channel: input.channel ?? 'rc', status: requiredBlocked ? 'failed' : requiredExperimental ? 'warning' : 'passed', targets, summary, generatedAt: (input.now ?? new Date()).toISOString() };
}

export function createUpgradePlan(input: { fromVersion: string; toVersion: string; channel?: ReleaseCandidateChannel; hasBackup?: boolean; openBlockers?: number; now?: Date }): UpgradePlanPayload {
  const risks = [] as Array<{ code: string; severity: string; message: string }>;
  const checks = [] as Array<{ code: string; status: string; message: string }>;
  if (!input.hasBackup) risks.push({ code: 'backup.missing', severity: 'error', message: 'Перед RC upgrade требуется свежий backup.' });
  if ((input.openBlockers ?? 0) > 0) risks.push({ code: 'release.blockers.open', severity: 'blocker', message: `Открытых release/security blockers: ${input.openBlockers}` });
  checks.push({ code: 'version.transition', status: input.fromVersion.includes('beta.3') && input.toVersion.includes('rc.1') ? 'passed' : 'warning', message: `${input.fromVersion} -> ${input.toVersion}` });
  checks.push({ code: 'backup.required', status: input.hasBackup ? 'passed' : 'failed', message: input.hasBackup ? 'Backup подтверждён' : 'Backup не подтверждён' });
  const blockerRisks = risks.filter((risk) => risk.severity === 'blocker' || risk.severity === 'error').length;
  return { format: 'cajeerbots.upgrade-plan.v1', fromVersion: input.fromVersion, toVersion: input.toVersion, channel: input.channel ?? 'rc', status: blockerRisks > 0 ? 'blocked' : 'ready', steps: defaultUpgradeSteps, risks, checks, generatedAt: (input.now ?? new Date()).toISOString() };
}

export async function runRcReadiness(input: { version: string; channel?: ReleaseCandidateChannel; freeze?: ApiFreezeSnapshotPayload | null; rootDir?: string; compatibilityTargets?: CompatibilityTargetDefinition[]; openBlockers?: number; securityOpenFindings?: number; latestIntegrationStatus?: string | null; now?: Date }): Promise<RcReadinessReport> {
  const sections = [] as RcReadinessReport['sections'];
  if (input.freeze) {
    const freezeReport = await verifyApiFreeze({ freeze: input.freeze, rootDir: input.rootDir, allowAdditive: false, now: input.now });
    sections.push({ code: 'api_freeze', status: freezeReport.status, summary: { frozenOperations: freezeReport.frozenOperations, currentOperations: freezeReport.currentOperations }, issues: freezeReport.issues });
  } else {
    sections.push({ code: 'api_freeze', status: 'failed', summary: {}, issues: [{ code: 'api_freeze.missing', severity: 'blocker', message: 'Нет API freeze snapshot для RC.' }] });
  }
  const matrix = buildCompatibilityMatrix({ version: input.version, channel: input.channel ?? 'rc', targets: input.compatibilityTargets, now: input.now });
  sections.push({ code: 'compatibility_matrix', status: matrix.status, summary: matrix.summary, issues: matrix.targets.filter((target) => target.required && target.status === 'blocked').map((target) => ({ code: 'compatibility.required_blocked', severity: 'blocker', source: target.code, message: `Required compatibility target blocked: ${target.name}` })) });
  const blockerCount = Number(input.openBlockers ?? 0);
  sections.push({ code: 'release_blockers', status: blockerCount > 0 ? 'failed' : 'passed', summary: { openBlockers: blockerCount }, issues: blockerCount > 0 ? [{ code: 'release.blockers.open', severity: 'blocker', message: `Открытые blockers: ${blockerCount}` }] : [] });
  const findings = Number(input.securityOpenFindings ?? 0);
  sections.push({ code: 'security_findings', status: findings > 0 ? 'failed' : 'passed', summary: { openFindings: findings }, issues: findings > 0 ? [{ code: 'security.findings.open', severity: 'blocker', message: `Открытые security findings: ${findings}` }] : [] });
  const integrationStatus = input.latestIntegrationStatus ?? 'unknown';
  sections.push({ code: 'integration_smoke', status: integrationStatus === 'passed' ? 'passed' : integrationStatus === 'warning' ? 'warning' : 'failed', summary: { latestIntegrationStatus: integrationStatus }, issues: integrationStatus === 'passed' ? [] : [{ code: 'integration.status_not_passed', severity: 'error', message: `Последний integration status: ${integrationStatus}` }] });
  const failed = sections.filter((section) => section.status === 'failed').length;
  const warning = sections.filter((section) => section.status === 'warning').length;
  const summary = { total: sections.length, passed: sections.filter((section) => section.status === 'passed').length, warning, failed, blockers: sections.flatMap((section) => section.issues).filter((issue) => issue.severity === 'blocker' || issue.severity === 'error').length };
  return { format: 'cajeerbots.rc-readiness.v1', version: input.version, channel: input.channel ?? 'rc', status: failed > 0 ? 'failed' : warning > 0 ? 'warning' : 'passed', sections, summary, generatedAt: (input.now ?? new Date()).toISOString() };
}

function freezeToOpenApiSnapshot(freeze: ApiFreezeSnapshotPayload): OpenApiContractSnapshot {
  return { format: 'cajeerbots.openapi.snapshot.v1', title: 'CajeerBots API', version: freeze.openApiVersion, sourceSha256: freeze.sourceSha256, generatedAt: freeze.generatedAt, paths: new Set(freeze.operations.map((operation) => operation.path)).size, operations: freeze.operations.map((operation) => ({ method: operation.method as never, path: operation.path, operationId: operation.operationId, tags: operation.tags ?? [], deprecated: Boolean(operation.deprecated), secured: true })), tags: Array.from(new Set(freeze.operations.flatMap((operation) => operation.tags ?? []))).sort(), duplicateOperationIds: freeze.duplicateOperationIds, unsecuredOperations: freeze.unsecuredOperations.map((operation) => ({ method: operation.method as never, path: operation.path, operationId: operation.operationId })), deprecatedOperations: freeze.deprecatedOperations.map((operation) => ({ method: operation.method as never, path: operation.path, operationId: operation.operationId })) };
}
