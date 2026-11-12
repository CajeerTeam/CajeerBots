import { assertExpectation } from './assertions.js';
import type { IntegrationRunOptions, IntegrationRunSummary, IntegrationTestDefinition, IntegrationTestStepDefinition, StepExecutionResult } from './types.js';

function normalizeDefinition(value: unknown): IntegrationTestDefinition {
  if (value && typeof value === 'object' && Array.isArray((value as any).steps)) return value as IntegrationTestDefinition;
  return { steps: [] };
}

export async function runIntegrationTestCase(options: IntegrationRunOptions & { testCase: any; suite?: any }) {
  const started = Date.now();
  const definition = normalizeDefinition(options.testCase.definition);
  const run = options.runId
    ? await options.prisma.integrationTestRun.update({ where: { id: options.runId }, data: { status: 'running', startedAt: new Date(), rootDir: options.rootDir, environment: runtimeEnvironment(options) as never } })
    : await options.prisma.integrationTestRun.create({ data: { suiteId: options.testCase.suiteId, caseId: options.testCase.id, status: 'running', version: options.version ?? '1.0.0-rc.6', channel: options.channel ?? 'beta', trigger: options.triggeredBy ?? 'manual', startedAt: new Date(), rootDir: options.rootDir, environment: runtimeEnvironment(options) as never, createdBy: options.triggeredBy } });
  const steps: StepExecutionResult[] = [];
  for (let index = 0; index < definition.steps.length; index += 1) {
    const step = definition.steps[index]!;
    const result = await runStep(options, step, index);
    steps.push(result);
    await options.prisma.integrationTestStepRun.create({ data: { runId: run.id, caseId: options.testCase.id, stepIndex: index, code: result.code, kind: result.kind, status: result.status, startedAt: new Date(Date.now() - result.durationMs), finishedAt: new Date(), durationMs: result.durationMs, request: result.request as never, response: result.response as never, assertion: result.assertion as never, error: result.error } });
    if (result.status === 'failed') break;
  }
  const summary = summarizeSteps(steps, Date.now() - started);
  const updated = await options.prisma.integrationTestRun.update({ where: { id: run.id }, data: { status: summary.status, summary: summary as never, finishedAt: new Date(), durationMs: summary.durationMs } });
  await options.prisma.integrationTestCase.update({ where: { id: options.testCase.id }, data: { lastStatus: summary.status, lastRunAt: new Date() } });
  return { run: updated, summary, steps };
}

export async function runIntegrationTestSuite(options: IntegrationRunOptions & { suite: any; cases: any[] }) {
  const started = Date.now();
  const parent = await options.prisma.integrationTestRun.create({ data: { suiteId: options.suite.id, status: 'running', version: options.version ?? '1.0.0-rc.6', channel: options.channel ?? 'beta', trigger: options.triggeredBy ?? 'manual', startedAt: new Date(), rootDir: options.rootDir, environment: runtimeEnvironment(options) as never, createdBy: options.triggeredBy } });
  const caseResults = [];
  for (const testCase of options.cases) caseResults.push(await runIntegrationTestCase({ ...options, testCase }));
  const failed = caseResults.filter((item) => item.summary.status === 'failed').length;
  const passed = caseResults.filter((item) => item.summary.status === 'passed').length;
  const skipped = caseResults.filter((item) => item.summary.status === 'skipped').length;
  const summary = { status: failed > 0 ? 'failed' : passed > 0 ? 'passed' : 'skipped', total: caseResults.length, passed, failed, skipped, durationMs: Date.now() - started } satisfies IntegrationRunSummary;
  const updated = await options.prisma.integrationTestRun.update({ where: { id: parent.id }, data: { status: summary.status, summary: summary as never, finishedAt: new Date(), durationMs: summary.durationMs } });
  return { run: updated, summary, cases: caseResults };
}

async function runStep(options: IntegrationRunOptions, step: IntegrationTestStepDefinition, index: number): Promise<StepExecutionResult> {
  const started = Date.now();
  const code = step.code ?? `${step.kind}.${index}`;
  const request = { kind: step.kind }; // Never persist caller headers, credentials or arbitrary payloads in run evidence.
  if (options.dryRun) return { code, kind: step.kind, status: 'skipped', request, response: {}, assertion: {}, durationMs: 0 };
  try {
    const response = await executeStep(options, step);
    if (response.status === 'skipped') return { code, kind: step.kind, status: 'skipped', request, response: {}, assertion: {}, durationMs: Date.now() - started };
    const assertion = assertExpectation(response, step.expect ?? {});
    // Evaluate body assertions in memory, but never persist arbitrary HTTP response bodies.
    const evidence = { ...response }; delete evidence.body;
    const details = { failures: assertion.passed ? [] : ['Expectation failed'] };
    return { code, kind: step.kind, status: assertion.passed ? 'passed' : 'failed', request, response: evidence, assertion: details, error: assertion.passed ? undefined : 'Expectation failed', durationMs: Date.now() - started };
  } catch (error) {
    return { code, kind: step.kind, status: 'failed', request, response: {}, assertion: {}, error: error instanceof Error ? error.name : 'ExecutionError', durationMs: Date.now() - started };
  }
}

async function executeStep(options: IntegrationRunOptions, step: IntegrationTestStepDefinition): Promise<Record<string, unknown>> {
  const request = step.request ?? {};
  switch (step.kind) {
    case 'api_health':
    case 'http_request':
      return executeHttp(options, request, step.kind === 'api_health' ? '/api/health' : undefined);
    case 'database_count': {
      const model = String(request.model ?? 'community');
      if (!/^[a-zA-Z][a-zA-Z0-9_]*$/.test(model) || !options.prisma[model]?.count) throw new Error(`Недоступная Prisma model для count: ${model}`);
      const count = await options.prisma[model].count({ where: request.where ?? undefined });
      return { status: 'ok', model, count };
    }
    case 'event_publish': {
      const topicName = String(request.topic ?? 'integration.smoke');
      const type = String(request.type ?? topicName);
      const topic = await options.prisma.eventTopic.upsert({ where: { name: topicName }, create: { name: topicName, description: 'Integration test topic' }, update: {} });
      const event = await options.prisma.eventRecord.create({ data: { eventId: `it-${Date.now()}-${Math.random().toString(16).slice(2)}`, topicId: topic.id, topicName, type, source: 'integration-tests', payload: (request.payload ?? {}) as never, metadata: { dryRun: options.dryRun ?? false } as never } });
      return { status: 'created', id: event.id, topic: topicName };
    }
    case 'outbox_probe': {
      const [pending, dead] = await Promise.all([
        options.prisma.outboxMessage.count({ where: { status: { in: ['pending', 'failed', 'processing'] } } }).catch(() => 0),
        options.prisma.outboxMessage.count({ where: { status: 'dead' } }).catch(() => 0),
      ]);
      return { status: 'ok', pending, dead };
    }
    case 'bot_scenario': {
      const scenarioCode = String(request.scenarioCode ?? 'system.help-smoke');
      const communityId = request.communityId ? String(request.communityId) : await firstCommunityId(options.prisma);
      const scenario = await options.prisma.botScenario.findFirst({ where: { communityId, code: scenarioCode, isActive: true } }).catch(() => null);
      if (!scenario) return { status: 'skipped', exists: false, reason: `scenario ${scenarioCode} не найден` };
      return { status: 'skipped', exists: true, reason: 'Use the real provider rehearsal; existence is not execution' };
    }
    case 'release_gate_summary': {
      const [openBlockers, lastRun] = await Promise.all([
        options.prisma.releaseBlocker.count({ where: { status: 'open' } }).catch(() => 0),
        options.prisma.releaseGateRun.findFirst({ orderBy: { startedAt: 'desc' } }).catch(() => null),
      ]);
      return { status: 'ok', openBlockers, lastRunStatus: lastRun?.status ?? null };
    }
    default:
      return { status: 'skipped', reason: `Неподдерживаемый step kind: ${(step as any).kind}` };
  }
}

export function resolveIntegrationUrl(request: Record<string, unknown>, configuredBase: string, requestedBase?: string, defaultPath = '/api/health'): URL {
  const base = new URL(configuredBase);
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password) throw new Error('Invalid configured integration origin');
  const candidate = new URL(String(request.baseUrl ?? requestedBase ?? base.origin));
  const url = new URL(String(request.url ?? request.path ?? defaultPath), candidate);
  if (candidate.origin !== base.origin || candidate.username || candidate.password || url.origin !== base.origin || url.username || url.password || url.hash || !url.pathname.startsWith('/api/')) throw new Error('Integration request outside configured API origin');
  return url;
}
async function executeHttp(options: IntegrationRunOptions, request: Record<string, unknown>, defaultPath?: string) {
  const url = resolveIntegrationUrl(request, process.env.INTEGRATION_TESTS_API_URL ?? process.env.CAJEER_API_URL ?? 'http://127.0.0.1:3000', options.baseUrl, defaultPath);
  const method = String(request.method ?? 'GET').toUpperCase();
  if (!['GET', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE'].includes(method)) throw new Error('Invalid integration method');
  const headers = new Headers();
  for (const [name, value] of Object.entries((request.headers ?? {}) as Record<string, string>)) {
    if (['authorization', 'x-api-key', 'accept', 'content-type'].includes(name.toLowerCase())) headers.set(name, value);
  }
  if (options.apiToken && !headers.has('authorization') && !headers.has('x-api-key')) headers.set('authorization', `Bearer ${options.apiToken}`);
  const body = request.body === undefined ? undefined : JSON.stringify(request.body);
  if (body && Buffer.byteLength(body) > 1_000_000) throw new Error('Integration request too large');
  if (body) headers.set('content-type', 'application/json');
  const response = await fetch(url, { method, headers, body, redirect: 'error', signal: AbortSignal.timeout(10_000) });
  const reader = response.body?.getReader();
  const chunks: Uint8Array[] = []; let bytes = 0;
  if (reader) try {
    for (;;) {
      const { done, value } = await reader.read(); if (done) break;
      bytes += value.byteLength; if (bytes > 65_536) throw new Error('Integration response too large');
      chunks.push(value);
    }
  } finally { await reader.cancel().catch(() => {}); reader.releaseLock(); }
  return { status: response.status, ok: response.ok, body: Buffer.concat(chunks).toString('utf8'), method };
}

function summarizeSteps(steps: StepExecutionResult[], durationMs: number): IntegrationRunSummary {
  const passed = steps.filter((step) => step.status === 'passed').length;
  const failed = steps.filter((step) => step.status === 'failed').length;
  const skipped = steps.filter((step) => step.status === 'skipped').length;
  return { status: failed > 0 ? 'failed' : passed > 0 ? 'passed' : 'skipped', total: steps.length, passed, failed, skipped, durationMs };
}

function runtimeEnvironment(options: IntegrationRunOptions) {
  return { baseUrl: process.env.INTEGRATION_TESTS_API_URL ?? process.env.CAJEER_API_URL ?? null, node: process.version, dryRun: options.dryRun ?? false, runner: 'cajeerbots.integration-tests.v1' };
}

async function firstCommunityId(prisma: any): Promise<string> {
  const community = await prisma.community.findFirst({ orderBy: { createdAt: 'asc' } });
  if (!community) throw new Error('Не найдено ни одного community для bot scenario integration step');
  return community.id;
}
