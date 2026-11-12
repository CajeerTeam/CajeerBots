import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { calculateIntegrationCoverage, defaultIntegrationSuites, runIntegrationTestCase, runIntegrationTestSuite } from '@cajeerbots/integration-tests';
import type { Principal } from '@cajeerbots/security';
import type { z } from 'zod';
import type { coverageSchema, listCasesQuerySchema, listCoverageQuerySchema, listRunsQuerySchema, listStepRunsQuerySchema, listSuitesQuerySchema, queueRunSchema, runCaseSchema, runSuiteSchema, upsertCaseSchema, upsertSuiteSchema } from './integration-tests.schemas.js';

type UpsertSuiteInput = z.infer<typeof upsertSuiteSchema>;
type UpsertCaseInput = z.infer<typeof upsertCaseSchema>;

@Injectable()
export class IntegrationTestsService {
  constructor(private readonly prisma: PrismaService) {}

  async summary() {
    const [suites, cases, lastRun, openFailed, coverage] = await Promise.all([
      this.prisma.integrationTestSuite.count({ where: { isActive: true } }),
      this.prisma.integrationTestCase.count({ where: { isActive: true } }),
      this.prisma.integrationTestRun.findFirst({ orderBy: { createdAt: 'desc' } }),
      this.prisma.integrationTestRun.count({ where: { status: 'failed' } }),
      this.prisma.integrationTestCoverage.findFirst({ orderBy: { createdAt: 'desc' } }),
    ]);
    return { suites, cases, lastRun, failedRuns: openFailed, coverage, defaultSuiteCount: defaultIntegrationSuites.length };
  }

  async seedDefaults(principal: Principal) {
    let suites = 0;
    let cases = 0;
    for (const suite of defaultIntegrationSuites) {
      const row = await this.prisma.integrationTestSuite.upsert({ where: { code: suite.code }, create: { code: suite.code, title: suite.title, category: suite.category, description: suite.description, metadata: { seededBy: '1.0.0-rc.2' } as never }, update: { title: suite.title, category: suite.category, description: suite.description, isActive: true } });
      suites += 1;
      for (const testCase of suite.cases) {
        await this.prisma.integrationTestCase.upsert({ where: { suiteId_code: { suiteId: row.id, code: testCase.code } }, create: { suiteId: row.id, code: testCase.code, title: testCase.title, target: testCase.target, kind: testCase.kind, priority: testCase.priority, definition: testCase.definition as never, expected: testCase.expected as never }, update: { title: testCase.title, target: testCase.target, kind: testCase.kind, priority: testCase.priority, definition: testCase.definition as never, expected: testCase.expected as never, isActive: true } });
        cases += 1;
      }
    }
    await this.audit(principal, 'integration_tests.defaults.seed', 'integration_test_suite', null, { suites, cases });
    return { suites, cases };
  }

  listSuites(query: z.infer<typeof listSuitesQuerySchema>) {
    return this.prisma.integrationTestSuite.findMany({ where: { ...(query.category ? { category: query.category } : {}), ...(query.active === undefined ? {} : { isActive: query.active }) }, orderBy: [{ category: 'asc' }, { code: 'asc' }], take: query.take, include: { _count: { select: { cases: true, runs: true } } } });
  }

  async upsertSuite(input: UpsertSuiteInput, principal: Principal) {
    const row = await this.prisma.integrationTestSuite.upsert({ where: { code: input.code }, create: { code: input.code, title: input.title, category: input.category, description: input.description, isActive: input.isActive, metadata: input.metadata as never }, update: { title: input.title, category: input.category, description: input.description, isActive: input.isActive, metadata: input.metadata as never } });
    await this.audit(principal, 'integration_tests.suite.upsert', 'integration_test_suite', row.id, input);
    return row;
  }

  listCases(query: z.infer<typeof listCasesQuerySchema>) {
    return this.prisma.integrationTestCase.findMany({ where: { ...(query.suiteCode ? { suite: { code: query.suiteCode } } : {}), ...(query.target ? { target: query.target } : {}), ...(query.kind ? { kind: query.kind } : {}), ...(query.provider ? { provider: query.provider } : {}), ...(query.active === undefined ? {} : { isActive: query.active }) }, orderBy: [{ priority: 'asc' }, { code: 'asc' }], take: query.take, include: { suite: true } });
  }

  async upsertCase(input: UpsertCaseInput, principal: Principal) {
    const suite = await this.prisma.integrationTestSuite.findUnique({ where: { code: input.suiteCode } });
    if (!suite) throw new NotFoundException('Integration test suite не найден');
    const row = await this.prisma.integrationTestCase.upsert({ where: { suiteId_code: { suiteId: suite.id, code: input.code } }, create: { suiteId: suite.id, code: input.code, title: input.title, target: input.target, kind: input.kind, provider: input.provider, priority: input.priority, isActive: input.isActive, timeoutMs: input.timeoutMs, definition: input.definition as never, expected: input.expected as never }, update: { title: input.title, target: input.target, kind: input.kind, provider: input.provider, priority: input.priority, isActive: input.isActive, timeoutMs: input.timeoutMs, definition: input.definition as never, expected: input.expected as never } });
    await this.audit(principal, 'integration_tests.case.upsert', 'integration_test_case', row.id, input);
    return row;
  }

  async getCase(suiteCode: string, caseCode: string) {
    const row = await this.prisma.integrationTestCase.findFirst({ where: { code: caseCode, suite: { code: suiteCode } }, include: { suite: true, runs: { orderBy: { createdAt: 'desc' }, take: 20 } } });
    if (!row) throw new NotFoundException('Integration test case не найден');
    return row;
  }

  async runSuite(code: string, input: z.infer<typeof runSuiteSchema>, principal: Principal) {
    const suite = await this.prisma.integrationTestSuite.findUnique({ where: { code } });
    if (!suite) throw new NotFoundException('Integration test suite не найден');
    const cases = await this.prisma.integrationTestCase.findMany({ where: { suiteId: suite.id, isActive: true }, orderBy: [{ priority: 'asc' }, { code: 'asc' }] });
    const result = await runIntegrationTestSuite({ prisma: this.prisma, suite, cases, version: input.version, channel: input.channel, baseUrl: input.baseUrl, apiToken: input.apiToken, dryRun: input.dryRun, rootDir: input.rootDir, triggeredBy: principal.id });
    await this.audit(principal, 'integration_tests.suite.run', 'integration_test_suite', suite.id, { status: result.summary.status, cases: cases.length });
    return result;
  }

  async runCase(suiteCode: string, caseCode: string, input: z.infer<typeof runCaseSchema>, principal: Principal) {
    const testCase = await this.prisma.integrationTestCase.findFirst({ where: { code: caseCode, suite: { code: suiteCode } }, include: { suite: true } });
    if (!testCase) throw new NotFoundException('Integration test case не найден');
    const result = await runIntegrationTestCase({ prisma: this.prisma, testCase, suite: testCase.suite, version: input.version, channel: input.channel, baseUrl: input.baseUrl, apiToken: input.apiToken, dryRun: input.dryRun, rootDir: input.rootDir, triggeredBy: principal.id });
    await this.audit(principal, 'integration_tests.case.run', 'integration_test_case', testCase.id, { status: result.summary.status });
    return result;
  }

  async queueRun(input: z.infer<typeof queueRunSchema>, principal: Principal) {
    let suite = input.suiteCode ? await this.prisma.integrationTestSuite.findUnique({ where: { code: input.suiteCode } }) : null;
    let testCase = null;
    if (input.caseCode) {
      testCase = await this.prisma.integrationTestCase.findFirst({ where: { code: input.caseCode, ...(suite ? { suiteId: suite.id } : {}) } });
      if (!testCase) throw new NotFoundException('Integration test case не найден');
      suite = suite ?? await this.prisma.integrationTestSuite.findUnique({ where: { id: testCase.suiteId } });
    }
    if (input.suiteCode && !suite) throw new NotFoundException('Integration test suite не найден');
    const run = await this.prisma.integrationTestRun.create({ data: { suiteId: suite?.id ?? null, caseId: testCase?.id ?? null, version: input.version, channel: input.channel, trigger: input.trigger, status: 'queued', rootDir: input.rootDir, environment: input.environment as never, createdBy: principal.id } });
    await this.audit(principal, 'integration_tests.run.queue', 'integration_test_run', run.id, input);
    return run;
  }

  listRuns(query: z.infer<typeof listRunsQuerySchema>) {
    return this.prisma.integrationTestRun.findMany({ where: { ...(query.suiteCode ? { suite: { code: query.suiteCode } } : {}), ...(query.caseCode ? { testCase: { code: query.caseCode } } : {}), ...(query.status ? { status: query.status } : {}), ...(query.version ? { version: query.version } : {}), ...(query.channel ? { channel: query.channel } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take, include: { suite: true, testCase: true, _count: { select: { steps: true } } } });
  }

  async getRun(id: string) {
    const row = await this.prisma.integrationTestRun.findUnique({ where: { id }, include: { suite: true, testCase: true, steps: { orderBy: { stepIndex: 'asc' } } } });
    if (!row) throw new NotFoundException('Integration test run не найден');
    return row;
  }

  listSteps(query: z.infer<typeof listStepRunsQuerySchema>) {
    return this.prisma.integrationTestStepRun.findMany({ where: { ...(query.runId ? { runId: query.runId } : {}), ...(query.caseId ? { caseId: query.caseId } : {}), ...(query.status ? { status: query.status } : {}), ...(query.kind ? { kind: query.kind } : {}) }, orderBy: [{ startedAt: 'desc' }, { stepIndex: 'asc' }], take: query.take });
  }

  async coverage(input: z.infer<typeof coverageSchema>, principal: Principal) {
    const result = await calculateIntegrationCoverage(this.prisma, { version: input.version, channel: input.channel, createdBy: principal.id });
    await this.audit(principal, 'integration_tests.coverage.calculate', 'integration_test_coverage', result.coverage.id, result.summary);
    return result;
  }

  listCoverage(query: z.infer<typeof listCoverageQuerySchema>) {
    return this.prisma.integrationTestCoverage.findMany({ where: { ...(query.version ? { version: query.version } : {}), ...(query.channel ? { channel: query.channel } : {}), ...(query.status ? { status: query.status } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take });
  }

  private audit(principal: Principal, action: string, resource: string, resourceId: string | null, metadata: unknown) {
    return this.prisma.auditLog.create({ data: { actorType: principal.type, actorId: principal.id, action, resource, resourceId, metadata: metadata as never } });
  }
}
