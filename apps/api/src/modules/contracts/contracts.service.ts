import { readFile, mkdir, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '@cajeerbots/db';
import { assertOpenApiSnapshot, buildOpenApiSnapshot, diffSnapshots, evaluateSdkCoverage, generateSdkClientChecklist, generateSdkOperationMap, sha256Hex } from '@cajeerbots/contracts';
import type { Principal } from '@cajeerbots/security';
import type { z } from 'zod';
import type { createSnapshotSchema, generateSdkSchema, listChecksQuerySchema, listDeprecationsQuerySchema, listSdkRunsQuerySchema, listSnapshotsQuerySchema, runContractCheckSchema, upsertDeprecationSchema } from './contracts.schemas.js';

type ListSnapshotsQuery = z.infer<typeof listSnapshotsQuerySchema>;
type CreateSnapshotInput = z.infer<typeof createSnapshotSchema>;
type RunContractCheckInput = z.infer<typeof runContractCheckSchema>;
type GenerateSdkInput = z.infer<typeof generateSdkSchema>;
type UpsertDeprecationInput = z.infer<typeof upsertDeprecationSchema>;

@Injectable()
export class ContractsService {
  constructor(private readonly prisma: PrismaService) {}

  snapshots(query: ListSnapshotsQuery) {
    return this.prisma.apiContractSnapshot.findMany({
      where: { ...(query.version ? { version: query.version } : {}), ...(query.baselineOnly ? { isBaseline: true } : {}) },
      orderBy: { createdAt: 'desc' },
      take: query.take,
    });
  }

  async snapshot(id: string) {
    const row = await this.prisma.apiContractSnapshot.findUnique({ where: { id } });
    if (!row) throw new NotFoundException('Contract snapshot не найден');
    return row;
  }

  async createSnapshot(input: CreateSnapshotInput, principal: Principal) {
    const text = await readProjectFile(input.sourcePath);
    const snapshot = buildOpenApiSnapshot(text);
    const version = input.version ?? snapshot.version;
    if (input.makeBaseline) {
      await this.prisma.apiContractSnapshot.updateMany({ where: { version, isBaseline: true }, data: { isBaseline: false } });
    }
    const created = await this.prisma.apiContractSnapshot.create({
      data: {
        version,
        sourcePath: input.sourcePath,
        sourceSha256: snapshot.sourceSha256,
        operationCount: snapshot.operations.length,
        pathCount: snapshot.paths,
        tagCount: snapshot.tags.length,
        duplicateOperationIds: snapshot.duplicateOperationIds,
        unsecuredOperations: snapshot.unsecuredOperations as never,
        deprecatedOperations: snapshot.deprecatedOperations as never,
        snapshot: snapshot as never,
        isBaseline: input.makeBaseline,
        createdBy: principal.id,
      },
    });
    await this.prisma.auditLog.create({ data: { actorType: principal.type, actorId: principal.id, action: 'contracts.snapshot.create', resource: 'api_contract_snapshot', resourceId: created.id, metadata: { version, sourcePath: input.sourcePath, makeBaseline: input.makeBaseline } as never } });
    return created;
  }

  async runCheck(input: RunContractCheckInput, principal: Principal) {
    const snapshotRow = input.snapshotId ? await this.snapshot(input.snapshotId) : await this.createSnapshot({ sourcePath: input.sourcePath, makeBaseline: false }, principal);
    const snapshot = snapshotRow.snapshot as never as ReturnType<typeof buildOpenApiSnapshot>;
    const baseline = input.baselineId
      ? await this.snapshot(input.baselineId)
      : await this.prisma.apiContractSnapshot.findFirst({ where: { isBaseline: true, version: snapshotRow.version, id: { not: snapshotRow.id } }, orderBy: { createdAt: 'desc' } });
    const baselineSnapshot = baseline?.snapshot as never as ReturnType<typeof buildOpenApiSnapshot> | undefined;
    const contractAssert = assertOpenApiSnapshot(snapshot);
    const diff = baselineSnapshot ? diffSnapshots(baselineSnapshot, snapshot) : undefined;
    const sdkCoverage = await this.sdkCoverage(snapshot, input.sdkSourcePath);
    const issues = [
      ...contractAssert.issues.map((message) => ({ type: 'openapi', severity: 'error', message })),
      ...(diff?.breaking ?? []).map((item) => ({ type: 'breaking_change', severity: 'error', ...item })),
      ...(input.failOnUnsecured ? snapshot.unsecuredOperations.map((operation) => ({ type: 'unsecured_operation', severity: 'error', ...operation })) : []),
      ...(sdkCoverage.percent < input.minSdkCoveragePercent ? [{ type: 'sdk_coverage', severity: 'warning', message: `SDK coverage ${sdkCoverage.percent}% ниже порога ${input.minSdkCoveragePercent}%`, missing: sdkCoverage.missing }] : []),
    ];
    const status = issues.some((issue) => issue.severity === 'error') || (input.failOnBreaking && (diff?.summary.breaking ?? 0) > 0) ? 'failed' : issues.length > 0 ? 'warning' : 'passed';
    const check = await this.prisma.apiContractCheck.create({
      data: {
        snapshotId: snapshotRow.id,
        baselineId: baseline?.id,
        status,
        checkType: 'contract_first',
        summary: { openapi: { operations: snapshot.operations.length, paths: snapshot.paths, tags: snapshot.tags.length }, diff: diff?.summary ?? null, sdkCoverage } as never,
        issues: issues as never,
        breakingCount: diff?.summary.breaking ?? 0,
        coveragePercent: sdkCoverage.percent,
        createdBy: principal.id,
      },
    });
    return { check, snapshot: snapshotRow, baseline, diff, sdkCoverage, issues };
  }

  checks(query: z.infer<typeof listChecksQuerySchema>) {
    return this.prisma.apiContractCheck.findMany({
      where: { ...(query.status ? { status: query.status } : {}), ...(query.checkType ? { checkType: query.checkType } : {}), ...(query.snapshotId ? { snapshotId: query.snapshotId } : {}) },
      orderBy: { createdAt: 'desc' },
      take: query.take,
    });
  }

  async generateSdk(input: GenerateSdkInput, principal: Principal) {
    const snapshotRow = input.snapshotId ? await this.snapshot(input.snapshotId) : await this.createSnapshot({ sourcePath: input.sourcePath, makeBaseline: false }, principal);
    const snapshot = snapshotRow.snapshot as never as ReturnType<typeof buildOpenApiSnapshot>;
    const coverage = await this.sdkCoverage(snapshot, input.sdkSourcePath);
    const files = [generateSdkOperationMap(snapshot), generateSdkClientChecklist(snapshot)];
    if (input.outputMode === 'file') {
      for (const file of files) {
        const path = resolve(process.cwd(), input.outputDir, file.filename);
        await mkdir(dirname(path), { recursive: true });
        await writeFile(path, file.content);
      }
    }
    const serialized = files.map((file) => ({ filename: file.filename, sha256: sha256Hex(file.content), sizeBytes: Buffer.byteLength(file.content), operations: file.operations, ...(input.outputMode === 'database' ? { content: file.content } : {}) }));
    const aggregate = serialized.map((file) => `${file.filename}:${file.sha256}`).join('\n');
    const run = await this.prisma.sdkGenerationRun.create({ data: { snapshotId: snapshotRow.id, status: 'generated', outputMode: input.outputMode, artifactName: input.outputMode === 'file' ? input.outputDir : 'database:generated_files', artifactSha256: sha256Hex(aggregate), artifactSizeBytes: serialized.reduce((sum, file) => sum + file.sizeBytes, 0), operationCount: snapshot.operations.length, coveragePercent: coverage.percent, missingOperations: coverage.missing as never, generatedFiles: serialized as never, createdBy: principal.id } });
    return { run, coverage, files: serialized };
  }

  sdkRuns(query: z.infer<typeof listSdkRunsQuerySchema>) {
    return this.prisma.sdkGenerationRun.findMany({ where: { ...(query.status ? { status: query.status } : {}), ...(query.snapshotId ? { snapshotId: query.snapshotId } : {}) }, orderBy: { createdAt: 'desc' }, take: query.take });
  }

  deprecations(query: z.infer<typeof listDeprecationsQuerySchema>) {
    return this.prisma.apiDeprecation.findMany({ where: query.status ? { status: query.status } : {}, orderBy: [{ status: 'asc' }, { operationId: 'asc' }], take: query.take });
  }

  upsertDeprecation(input: UpsertDeprecationInput, principal: Principal) {
    return this.prisma.apiDeprecation.upsert({
      where: { operationId: input.operationId },
      create: { ...input, createdBy: principal.id, metadata: input.metadata as never },
      update: { ...input, metadata: input.metadata as never },
    });
  }

  async sdkCoverage(snapshot: ReturnType<typeof buildOpenApiSnapshot>, sdkSourcePath: string) {
    const source = await readProjectFile(sdkSourcePath).catch(() => '');
    return evaluateSdkCoverage(snapshot, source);
  }
}

async function readProjectFile(relativePath: string): Promise<string> {
  const safePath = relativePath.replace(/^\/+/, '');
  const absolute = resolve(process.cwd(), safePath);
  if (!absolute.startsWith(process.cwd())) throw new Error(`Недопустимый путь: ${relativePath}`);
  return readFile(absolute, 'utf8');
}
