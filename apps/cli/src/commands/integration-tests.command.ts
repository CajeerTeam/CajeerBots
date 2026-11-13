import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { calculateIntegrationCoverage, defaultIntegrationSuites, runIntegrationTestCase, runIntegrationTestSuite } from '@cajeerbots/integration-tests';

export function integrationTestsCommand(): Command {
  const command = new Command('integration-tests').alias('it').description('Integration suites/cases/runs для beta regression и MVP smoke');

  command.command('summary').action(async () => withPrisma(async (prisma) => {
    const [suites, cases, lastRun, coverage] = await Promise.all([
      prisma.integrationTestSuite.count({ where: { isActive: true } }),
      prisma.integrationTestCase.count({ where: { isActive: true } }),
      prisma.integrationTestRun.findFirst({ orderBy: { createdAt: 'desc' } }),
      prisma.integrationTestCoverage.findFirst({ orderBy: { createdAt: 'desc' } }),
    ]);
    console.log(JSON.stringify({ suites, cases, lastRun, coverage, defaultSuiteCount: defaultIntegrationSuites.length }, null, 2));
  }));

  command.command('defaults:seed').action(async () => withPrisma(async (prisma) => {
    let suites = 0;
    let cases = 0;
    for (const suite of defaultIntegrationSuites) {
      const row = await prisma.integrationTestSuite.upsert({ where: { code: suite.code }, create: { code: suite.code, title: suite.title, category: suite.category, description: suite.description, metadata: { seededBy: 'cli' } as never }, update: { title: suite.title, category: suite.category, description: suite.description, isActive: true } });
      suites += 1;
      for (const testCase of suite.cases) {
        await prisma.integrationTestCase.upsert({ where: { suiteId_code: { suiteId: row.id, code: testCase.code } }, create: { suiteId: row.id, code: testCase.code, title: testCase.title, target: testCase.target, kind: testCase.kind, priority: testCase.priority, definition: testCase.definition as never, expected: testCase.expected as never }, update: { title: testCase.title, target: testCase.target, kind: testCase.kind, priority: testCase.priority, definition: testCase.definition as never, expected: testCase.expected as never, isActive: true } });
        cases += 1;
      }
    }
    console.log(JSON.stringify({ suites, cases }, null, 2));
  }));

  command.command('suites:list')
    .option('--category <category>')
    .option('--active <active>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { category?: string; active?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.integrationTestSuite.findMany({ where: { ...(options.category ? { category: options.category } : {}), ...(options.active === undefined ? {} : { isActive: options.active === 'true' }) }, orderBy: [{ category: 'asc' }, { code: 'asc' }], take: Math.min(Number(options.take), 500), include: { _count: { select: { cases: true, runs: true } } } });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('suites:set')
    .requiredOption('--code <code>')
    .requiredOption('--title <title>')
    .option('--category <category>', 'category', 'mvp')
    .option('--description <description>')
    .option('--active <active>', 'true|false', 'true')
    .action(async (options: { code: string; title: string; category: string; description?: string; active: string }) => withPrisma(async (prisma) => {
      const row = await prisma.integrationTestSuite.upsert({ where: { code: options.code }, create: { code: options.code, title: options.title, category: options.category, description: options.description, isActive: options.active !== 'false' }, update: { title: options.title, category: options.category, description: options.description, isActive: options.active !== 'false' } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('cases:list')
    .option('--suite <code>')
    .option('--target <target>')
    .option('--active <active>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { suite?: string; target?: string; active?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.integrationTestCase.findMany({ where: { ...(options.suite ? { suite: { code: options.suite } } : {}), ...(options.target ? { target: options.target } : {}), ...(options.active === undefined ? {} : { isActive: options.active === 'true' }) }, orderBy: [{ priority: 'asc' }, { code: 'asc' }], take: Math.min(Number(options.take), 500), include: { suite: true } });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('cases:set')
    .requiredOption('--suite <code>')
    .requiredOption('--code <code>')
    .requiredOption('--title <title>')
    .option('--target <target>', 'api|security|bot-runtime|event-api|extensions|ops', 'api')
    .option('--kind <kind>', 'kind', 'multi_step')
    .option('--priority <n>', 'priority', '100')
    .requiredOption('--definition <json>', 'JSON definition')
    .option('--expected <json>', 'JSON expected', '{}')
    .action(async (options: { suite: string; code: string; title: string; target: string; kind: string; priority: string; definition: string; expected: string }) => withPrisma(async (prisma) => {
      const suite = await prisma.integrationTestSuite.findUnique({ where: { code: options.suite } });
      if (!suite) throw new Error(`Suite не найден: ${options.suite}`);
      const row = await prisma.integrationTestCase.upsert({ where: { suiteId_code: { suiteId: suite.id, code: options.code } }, create: { suiteId: suite.id, code: options.code, title: options.title, target: options.target, kind: options.kind, priority: Number(options.priority), definition: JSON.parse(options.definition) as never, expected: JSON.parse(options.expected) as never }, update: { title: options.title, target: options.target, kind: options.kind, priority: Number(options.priority), definition: JSON.parse(options.definition) as never, expected: JSON.parse(options.expected) as never, isActive: true } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('run:suite')
    .argument('<suiteCode>')
    .option('--version <version>', 'version', '1.0.1')
    .option('--channel <channel>', 'channel', 'stable')
    .option('--base-url <url>')
    .option('--api-token <token>')
    .option('--dry-run', 'dry-run', false)
    .action(async (suiteCode: string, options: { version: string; channel: string; baseUrl?: string; apiToken?: string; dryRun?: boolean }) => withPrisma(async (prisma) => {
      const suite = await prisma.integrationTestSuite.findUnique({ where: { code: suiteCode } });
      if (!suite) throw new Error(`Suite не найден: ${suiteCode}`);
      const cases = await prisma.integrationTestCase.findMany({ where: { suiteId: suite.id, isActive: true }, orderBy: [{ priority: 'asc' }, { code: 'asc' }] });
      const result = await runIntegrationTestSuite({ prisma, suite, cases, version: options.version, channel: options.channel, baseUrl: options.baseUrl, apiToken: options.apiToken, dryRun: options.dryRun, triggeredBy: 'cli' });
      console.log(JSON.stringify(result, null, 2));
      if (result.summary.status === 'failed') process.exitCode = 1;
    }));

  command.command('run:case')
    .requiredOption('--suite <code>')
    .argument('<caseCode>')
    .option('--version <version>', 'version', '1.0.1')
    .option('--channel <channel>', 'channel', 'stable')
    .option('--base-url <url>')
    .option('--api-token <token>')
    .option('--dry-run', 'dry-run', false)
    .action(async (caseCode: string, options: { suite: string; version: string; channel: string; baseUrl?: string; apiToken?: string; dryRun?: boolean }) => withPrisma(async (prisma) => {
      const testCase = await prisma.integrationTestCase.findFirst({ where: { code: caseCode, suite: { code: options.suite } }, include: { suite: true } });
      if (!testCase) throw new Error(`Case не найден: ${options.suite}/${caseCode}`);
      const result = await runIntegrationTestCase({ prisma, testCase, suite: testCase.suite, version: options.version, channel: options.channel, baseUrl: options.baseUrl, apiToken: options.apiToken, dryRun: options.dryRun, triggeredBy: 'cli' });
      console.log(JSON.stringify(result, null, 2));
      if (result.summary.status === 'failed') process.exitCode = 1;
    }));

  command.command('queue')
    .option('--suite <code>')
    .option('--case <code>')
    .option('--version <version>', 'version', '1.0.1')
    .option('--channel <channel>', 'channel', 'stable')
    .option('--base-url <url>')
    .action(async (options: { suite?: string; case?: string; version: string; channel: string; baseUrl?: string }) => withPrisma(async (prisma) => {
      const suite = options.suite ? await prisma.integrationTestSuite.findUnique({ where: { code: options.suite } }) : null;
      const testCase = options.case ? await prisma.integrationTestCase.findFirst({ where: { code: options.case, ...(suite ? { suiteId: suite.id } : {}) } }) : null;
      const row = await prisma.integrationTestRun.create({ data: { suiteId: suite?.id ?? testCase?.suiteId ?? null, caseId: testCase?.id ?? null, version: options.version, channel: options.channel, trigger: 'cli_queue', status: 'queued', environment: { baseUrl: options.baseUrl, dryRun: true } as never, createdBy: 'cli' } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('runs:list')
    .option('--status <status>')
    .option('--suite <code>')
    .option('--case <code>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { status?: string; suite?: string; case?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.integrationTestRun.findMany({ where: { ...(options.status ? { status: options.status } : {}), ...(options.suite ? { suite: { code: options.suite } } : {}), ...(options.case ? { testCase: { code: options.case } } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500), include: { suite: true, testCase: true, _count: { select: { steps: true } } } });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('runs:get')
    .argument('<id>')
    .action(async (id: string) => withPrisma(async (prisma) => {
      const row = await prisma.integrationTestRun.findUnique({ where: { id }, include: { suite: true, testCase: true, steps: { orderBy: { stepIndex: 'asc' } } } });
      console.log(JSON.stringify(row, null, 2));
    }));

  command.command('coverage')
    .option('--version <version>', 'version', '1.0.1')
    .option('--channel <channel>', 'channel', 'stable')
    .action(async (options: { version: string; channel: string }) => withPrisma(async (prisma) => {
      const result = await calculateIntegrationCoverage(prisma, { version: options.version, channel: options.channel, createdBy: 'cli' });
      console.log(JSON.stringify(result, null, 2));
    }));

  return command;
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try { return await callback(prisma); } finally { await prisma.close(); }
}
