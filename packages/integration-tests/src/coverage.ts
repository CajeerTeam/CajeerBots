import { defaultIntegrationCoverageAreas } from './default-suites.js';

export async function calculateIntegrationCoverage(prisma: any, input: { version?: string; channel?: string; createdBy?: string } = {}) {
  const cases = await prisma.integrationTestCase.findMany({ where: { isActive: true }, include: { suite: true } });
  const lastRuns = await prisma.integrationTestRun.findMany({ where: { caseId: { not: null } }, orderBy: { createdAt: 'desc' }, take: 500 });
  const latestByCase = new Map<string, any>();
  for (const run of lastRuns) if (run.caseId && !latestByCase.has(run.caseId)) latestByCase.set(run.caseId, run);
  const targets = new Set<string>();
  const passingTargets = new Set<string>();
  for (const testCase of cases) {
    targets.add(testCase.target);
    const latest = latestByCase.get(testCase.id);
    if (latest?.status === 'passed') passingTargets.add(testCase.target);
  }
  const areas = defaultIntegrationCoverageAreas.map((area) => {
    const covered = area.requiredTargets.filter((target) => targets.has(target));
    const passing = area.requiredTargets.filter((target) => passingTargets.has(target));
    const missing = area.requiredTargets.filter((target) => !targets.has(target));
    return { ...area, covered, passing, missing, coveragePercent: Math.round((covered.length / area.requiredTargets.length) * 100), passingPercent: Math.round((passing.length / area.requiredTargets.length) * 100) };
  });
  const missing = areas.flatMap((area) => area.missing.map((target) => ({ area: area.code, target })));
  const totalRequired = areas.reduce((sum, area) => sum + area.requiredTargets.length, 0);
  const totalCovered = areas.reduce((sum, area) => sum + area.covered.length, 0);
  const totalPassing = areas.reduce((sum, area) => sum + area.passing.length, 0);
  const summary = { activeCases: cases.length, totalRequired, totalCovered, totalPassing, coveragePercent: Math.round((totalCovered / Math.max(totalRequired, 1)) * 100), passingPercent: Math.round((totalPassing / Math.max(totalRequired, 1)) * 100), missing: missing.length };
  const status = missing.length > 0 ? 'incomplete' : totalPassing < totalRequired ? 'needs_runs' : 'covered';
  const row = await prisma.integrationTestCoverage.create({ data: { version: input.version ?? '1.0.0-rc.2', channel: input.channel ?? 'beta', status, source: 'database', summary: summary as never, areas: areas as never, missing: missing as never, createdBy: input.createdBy } });
  return { coverage: row, summary, areas, missing };
}
