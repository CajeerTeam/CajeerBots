import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { Command } from 'commander';
import { PrismaService } from '@cajeerbots/db';
import { assertOpenApiSnapshot, buildOpenApiSnapshot, diffSnapshots, evaluateSdkCoverage, generateSdkClientChecklist, generateSdkOperationMap, sha256Hex } from '@cajeerbots/contracts';

export function contractsCommand(): Command {
  const command = new Command('contracts').description('Contract-first OpenAPI governance и SDK generation');

  command.command('snapshots:list')
    .option('--version <version>')
    .option('--baseline-only')
    .option('--take <n>', 'limit', '50')
    .action(async (options: { version?: string; baselineOnly?: boolean; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.apiContractSnapshot.findMany({ where: { ...(options.version ? { version: options.version } : {}), ...(options.baselineOnly ? { isBaseline: true } : {}) }, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('snapshot:create')
    .option('--source <path>', 'OpenAPI file', 'contracts/openapi/cajeerbots.openapi.yaml')
    .option('--version <version>')
    .option('--baseline')
    .action(async (options: { source: string; version?: string; baseline?: boolean }) => withPrisma(async (prisma) => {
      const text = await readProjectFile(options.source);
      const snapshot = buildOpenApiSnapshot(text);
      const version = options.version ?? snapshot.version;
      if (options.baseline) await prisma.apiContractSnapshot.updateMany({ where: { version, isBaseline: true }, data: { isBaseline: false } });
      const created = await prisma.apiContractSnapshot.create({ data: { version, sourcePath: options.source, sourceSha256: snapshot.sourceSha256, operationCount: snapshot.operations.length, pathCount: snapshot.paths, tagCount: snapshot.tags.length, duplicateOperationIds: snapshot.duplicateOperationIds, unsecuredOperations: snapshot.unsecuredOperations as never, deprecatedOperations: snapshot.deprecatedOperations as never, snapshot: snapshot as never, isBaseline: Boolean(options.baseline), createdBy: 'cli' } });
      console.log(JSON.stringify(created, null, 2));
    }));

  command.command('check')
    .option('--source <path>', 'OpenAPI file', 'contracts/openapi/cajeerbots.openapi.yaml')
    .option('--sdk-source <path>', 'SDK client source', 'packages/sdk/src/client.ts')
    .option('--snapshot <id>')
    .option('--baseline <id>')
    .option('--min-sdk-coverage <percent>', 'minimum coverage', '50')
    .action(async (options: { source: string; sdkSource: string; snapshot?: string; baseline?: string; minSdkCoverage: string }) => withPrisma(async (prisma) => {
      const snapshotRow = options.snapshot
        ? await prisma.apiContractSnapshot.findUniqueOrThrow({ where: { id: options.snapshot } })
        : await createSnapshot(prisma, options.source, false);
      const snapshot = snapshotRow.snapshot as never as ReturnType<typeof buildOpenApiSnapshot>;
      const baselineRow = options.baseline
        ? await prisma.apiContractSnapshot.findUnique({ where: { id: options.baseline } })
        : await prisma.apiContractSnapshot.findFirst({ where: { isBaseline: true, version: snapshotRow.version, id: { not: snapshotRow.id } }, orderBy: { createdAt: 'desc' } });
      const assertion = assertOpenApiSnapshot(snapshot);
      const diff = baselineRow ? diffSnapshots(baselineRow.snapshot as never, snapshot) : undefined;
      const sdkSource = await readProjectFile(options.sdkSource).catch(() => '');
      const coverage = evaluateSdkCoverage(snapshot, sdkSource);
      const issues = [
        ...assertion.issues.map((message) => ({ type: 'openapi', message })),
        ...(diff?.breaking ?? []).map((item) => ({ type: 'breaking_change', ...item })),
        ...(coverage.percent < Number(options.minSdkCoverage) ? [{ type: 'sdk_coverage', message: `SDK coverage ${coverage.percent}% ниже ${options.minSdkCoverage}%`, missing: coverage.missing }] : []),
      ];
      const status = issues.length > 0 ? 'failed' : 'passed';
      const check = await prisma.apiContractCheck.create({ data: { snapshotId: snapshotRow.id, baselineId: baselineRow?.id ?? null, status, checkType: 'cli_contract_first', summary: { diff: diff?.summary ?? null, sdkCoverage: coverage, operations: snapshot.operations.length } as never, issues: issues as never, breakingCount: diff?.summary.breaking ?? 0, coveragePercent: coverage.percent, createdBy: 'cli' } });
      console.log(JSON.stringify({ check, diff, sdkCoverage: coverage, issues }, null, 2));
      if (status === 'failed') process.exitCode = 1;
    }));

  command.command('checks:list')
    .option('--status <status>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { status?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.apiContractCheck.findMany({ where: options.status ? { status: options.status } : {}, orderBy: { createdAt: 'desc' }, take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('sdk:generate')
    .option('--source <path>', 'OpenAPI file', 'contracts/openapi/cajeerbots.openapi.yaml')
    .option('--snapshot <id>')
    .option('--sdk-source <path>', 'SDK client source', 'packages/sdk/src/client.ts')
    .option('--output-dir <path>', 'output directory', 'packages/sdk/src/generated')
    .option('--write')
    .action(async (options: { source: string; snapshot?: string; sdkSource: string; outputDir: string; write?: boolean }) => withPrisma(async (prisma) => {
      const snapshotRow = options.snapshot ? await prisma.apiContractSnapshot.findUniqueOrThrow({ where: { id: options.snapshot } }) : await createSnapshot(prisma, options.source, false);
      const snapshot = snapshotRow.snapshot as never as ReturnType<typeof buildOpenApiSnapshot>;
      const files = [generateSdkOperationMap(snapshot), generateSdkClientChecklist(snapshot)];
      if (options.write) {
        for (const file of files) {
          const target = resolve(process.cwd(), options.outputDir, file.filename);
          await mkdir(dirname(target), { recursive: true });
          await writeFile(target, file.content);
        }
      }
      const sdkSource = await readProjectFile(options.sdkSource).catch(() => '');
      const coverage = evaluateSdkCoverage(snapshot, sdkSource);
      const generatedFiles = files.map((file) => ({ filename: file.filename, sha256: sha256Hex(file.content), sizeBytes: Buffer.byteLength(file.content), operations: file.operations, ...(!options.write ? { content: file.content } : {}) }));
      const run = await prisma.sdkGenerationRun.create({ data: { snapshotId: snapshotRow.id, status: 'generated', outputMode: options.write ? 'file' : 'database', artifactName: options.write ? options.outputDir : 'database:generated_files', artifactSha256: sha256Hex(generatedFiles.map((file) => `${file.filename}:${file.sha256}`).join('\n')), artifactSizeBytes: generatedFiles.reduce((sum, file) => sum + file.sizeBytes, 0), operationCount: snapshot.operations.length, coveragePercent: coverage.percent, missingOperations: coverage.missing as never, generatedFiles: generatedFiles as never, createdBy: 'cli' } });
      console.log(JSON.stringify({ run, coverage, files: generatedFiles }, null, 2));
    }));

  command.command('deprecations:list')
    .option('--status <status>')
    .option('--take <n>', 'limit', '100')
    .action(async (options: { status?: string; take: string }) => withPrisma(async (prisma) => {
      const rows = await prisma.apiDeprecation.findMany({ where: options.status ? { status: options.status } : {}, orderBy: [{ status: 'asc' }, { operationId: 'asc' }], take: Math.min(Number(options.take), 500) });
      console.log(JSON.stringify(rows, null, 2));
    }));

  command.command('deprecations:set')
    .requiredOption('--operation-id <id>')
    .requiredOption('--method <method>')
    .requiredOption('--path <path>')
    .option('--status <status>', 'planned | deprecated | removed | cancelled', 'planned')
    .option('--reason <reason>')
    .option('--replacement <operationId>')
    .option('--deprecated-since <version>')
    .option('--remove-after <version>')
    .action(async (options: { operationId: string; method: string; path: string; status: string; reason?: string; replacement?: string; deprecatedSince?: string; removeAfter?: string }) => withPrisma(async (prisma) => {
      const row = await prisma.apiDeprecation.upsert({ where: { operationId: options.operationId }, create: { operationId: options.operationId, method: options.method.toUpperCase(), path: options.path, status: options.status, reason: options.reason, replacement: options.replacement, deprecatedSince: options.deprecatedSince, removeAfter: options.removeAfter, createdBy: 'cli' }, update: { method: options.method.toUpperCase(), path: options.path, status: options.status, reason: options.reason, replacement: options.replacement, deprecatedSince: options.deprecatedSince, removeAfter: options.removeAfter } });
      console.log(JSON.stringify(row, null, 2));
    }));

  return command;
}

async function createSnapshot(prisma: PrismaService, sourcePath: string, baseline: boolean) {
  const text = await readProjectFile(sourcePath);
  const snapshot = buildOpenApiSnapshot(text);
  if (baseline) await prisma.apiContractSnapshot.updateMany({ where: { version: snapshot.version, isBaseline: true }, data: { isBaseline: false } });
  return prisma.apiContractSnapshot.create({ data: { version: snapshot.version, sourcePath, sourceSha256: snapshot.sourceSha256, operationCount: snapshot.operations.length, pathCount: snapshot.paths, tagCount: snapshot.tags.length, duplicateOperationIds: snapshot.duplicateOperationIds, unsecuredOperations: snapshot.unsecuredOperations as never, deprecatedOperations: snapshot.deprecatedOperations as never, snapshot: snapshot as never, isBaseline: baseline, createdBy: 'cli' } });
}

async function readProjectFile(relativePath: string): Promise<string> {
  const safePath = relativePath.replace(/^\/+/, '');
  const absolute = resolve(process.cwd(), safePath);
  if (!absolute.startsWith(process.cwd())) throw new Error(`Недопустимый путь: ${relativePath}`);
  return readFile(absolute, 'utf8');
}

async function withPrisma<T>(callback: (prisma: PrismaService) => Promise<T>): Promise<T> {
  const prisma = new PrismaService();
  await prisma.connect();
  try {
    return await callback(prisma);
  } finally {
    await prisma.close();
  }
}
