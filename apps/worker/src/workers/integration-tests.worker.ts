import { PrismaService } from '@cajeerbots/db';
import { calculateIntegrationCoverage, runIntegrationTestCase, runIntegrationTestSuite } from '@cajeerbots/integration-tests';

export interface IntegrationTestsWorkerOptions {
  intervalMs?: number;
  batchSize?: number;
}

export class IntegrationTestsWorker {
  private timer?: NodeJS.Timeout;
  private running = false;

  constructor(private readonly prisma = new PrismaService(), private readonly options: IntegrationTestsWorkerOptions = {}) {}

  async start(): Promise<void> {
    await this.prisma.connect();
    await this.tick();
    this.timer = setInterval(() => void this.tick(), this.options.intervalMs ?? Number(process.env.INTEGRATION_TESTS_WORKER_INTERVAL_MS ?? 15000));
    console.log('[worker] integration-tests worker 1.0.0-rc.1 started');
  }

  async stop(): Promise<void> {
    if (this.timer) clearInterval(this.timer);
    await this.prisma.close();
  }

  async tick(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      const runs = await this.prisma.integrationTestRun.findMany({ where: { status: 'queued' }, orderBy: { createdAt: 'asc' }, take: this.options.batchSize ?? Number(process.env.INTEGRATION_TESTS_WORKER_BATCH_SIZE ?? 3), include: { suite: true, testCase: true } });
      for (const run of runs) await this.executeRun(run.id);
    } catch (error) {
      console.error('[worker] integration-tests failed', error);
    } finally {
      this.running = false;
    }
  }

  async executeRun(runId: string): Promise<void> {
    const run = await this.prisma.integrationTestRun.findUnique({ where: { id: runId }, include: { suite: true, testCase: true } });
    if (!run || run.status !== 'queued') return;
    if (run.caseId && run.testCase) {
      await runIntegrationTestCase({ prisma: this.prisma, testCase: run.testCase, suite: run.suite, runId: run.id, version: run.version, channel: run.channel, rootDir: run.rootDir ?? undefined, triggeredBy: 'worker', dryRun: Boolean((run.environment as any)?.dryRun ?? true), baseUrl: (run.environment as any)?.baseUrl });
    } else if (run.suiteId && run.suite) {
      const cases = await this.prisma.integrationTestCase.findMany({ where: { suiteId: run.suiteId, isActive: true }, orderBy: [{ priority: 'asc' }, { code: 'asc' }] });
      await this.prisma.integrationTestRun.update({ where: { id: run.id }, data: { status: 'running', startedAt: new Date() } });
      const result = await runIntegrationTestSuite({ prisma: this.prisma, suite: run.suite, cases, version: run.version, channel: run.channel, rootDir: run.rootDir ?? undefined, triggeredBy: 'worker', dryRun: Boolean((run.environment as any)?.dryRun ?? true), baseUrl: (run.environment as any)?.baseUrl });
      await this.prisma.integrationTestRun.update({ where: { id: run.id }, data: { status: result.summary.status, summary: result.summary as never, finishedAt: new Date(), durationMs: result.summary.durationMs } });
    } else {
      const suite = await this.prisma.integrationTestSuite.findFirst({ where: { code: 'mvp-smoke', isActive: true } });
      if (!suite) {
        await this.prisma.integrationTestRun.update({ where: { id: run.id }, data: { status: 'failed', finishedAt: new Date(), summary: { error: 'Нет suite/case и не найден mvp-smoke' } as never } });
        return;
      }
      const cases = await this.prisma.integrationTestCase.findMany({ where: { suiteId: suite.id, isActive: true }, orderBy: [{ priority: 'asc' }, { code: 'asc' }] });
      const result = await runIntegrationTestSuite({ prisma: this.prisma, suite, cases, version: run.version, channel: run.channel, rootDir: run.rootDir ?? undefined, triggeredBy: 'worker', dryRun: true });
      await this.prisma.integrationTestRun.update({ where: { id: run.id }, data: { suiteId: suite.id, status: result.summary.status, summary: result.summary as never, finishedAt: new Date(), durationMs: result.summary.durationMs } });
    }
    await calculateIntegrationCoverage(this.prisma, { version: run.version, channel: run.channel, createdBy: 'worker' });
  }
}
