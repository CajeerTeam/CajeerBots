import { readFile } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import { buildOpenApiSnapshot } from '@cajeerbots/contracts';
import { checkDocumentationPages, scanWikiDirectory } from '@cajeerbots/docs';
import { defaultReleaseGates } from './default-gates.js';
import { directoryExists, fileExists, listFilesRecursive, readTextFile } from './fs-utils.js';
import type { GateSeverity, ReleaseChannel, ReleaseGateDefinition, ReleaseGateIssue, ReleaseGateResult, ReleaseGateRunReport, ReleaseGateRunSummary } from './types.js';

export interface EvaluateReleaseGatesInput {
  rootDir?: string;
  version: string;
  channel: ReleaseChannel;
  gates?: ReleaseGateDefinition[];
  now?: Date;
}

export async function evaluateReleaseGates(input: EvaluateReleaseGatesInput): Promise<ReleaseGateRunReport> {
  const rootDir = resolve(input.rootDir ?? process.cwd());
  const gates = (input.gates?.length ? input.gates : defaultReleaseGates).filter((gate) => gate.enabled !== false);
  const results: ReleaseGateResult[] = [];
  for (const gate of gates) {
    const started = Date.now();
    const result = await evaluateGate(rootDir, input.version, input.channel, gate).catch((error) => ({
      code: gate.code,
      title: gate.title,
      category: gate.category,
      status: 'failed' as const,
      severity: gate.severity,
      summary: { error: error instanceof Error ? error.message : String(error) },
      issues: [{ code: `${gate.code}.exception`, severity: gate.severity, message: error instanceof Error ? error.message : String(error) }],
      durationMs: Date.now() - started,
    }));
    results.push({ ...result, durationMs: Date.now() - started });
  }
  const summary = summarizeResults(input.version, input.channel, results);
  return { version: input.version, channel: input.channel, status: summary.status, summary, results, generatedAt: (input.now ?? new Date()).toISOString() };
}

export function summarizeResults(version: string, channel: ReleaseChannel, results: ReleaseGateResult[]): ReleaseGateRunSummary {
  const failed = results.filter((result) => result.status === 'failed').length;
  const warning = results.filter((result) => result.status === 'warning').length;
  const skipped = results.filter((result) => result.status === 'skipped').length;
  const blockers = results.flatMap((result) => result.issues).filter((issue) => issue.severity === 'blocker' || issue.severity === 'error').length;
  return {
    version,
    channel,
    total: results.length,
    passed: results.filter((result) => result.status === 'passed').length,
    warning,
    failed,
    skipped,
    blockers,
    status: failed > 0 || blockers > 0 ? 'failed' : warning > 0 ? 'warning' : 'passed',
  };
}

async function evaluateGate(rootDir: string, version: string, channel: ReleaseChannel, gate: ReleaseGateDefinition): Promise<ReleaseGateResult> {
  switch (gate.code) {
    case 'project.required_files': return requiredFiles(rootDir, gate);
    case 'project.version_aligned': return versionAligned(rootDir, version, gate);
    case 'database.migrations_present': return migrationsPresent(rootDir, gate);
    case 'release.no_blocker_markers': return noBlockerMarkers(rootDir, gate);
    case 'openapi.parseable': return openApiParseable(rootDir, gate);
    case 'docs.wiki_present': return docsWikiPresent(rootDir, gate);
    case 'deployment.production_files': return requiredFiles(rootDir, gate);
    default:
      return result(gate, 'skipped', { reason: `Неизвестный release gate: ${gate.code}` }, []);
  }
}

async function requiredFiles(rootDir: string, gate: ReleaseGateDefinition): Promise<ReleaseGateResult> {
  const files = arrayOfStrings(gate.config?.files);
  const issues: ReleaseGateIssue[] = [];
  let present = 0;
  for (const file of files) {
    const exists = await fileExists(resolveSafe(rootDir, file));
    if (exists) present += 1;
    else issues.push({ code: `${gate.code}.missing_file`, severity: gate.severity, source: file, message: `Не найден обязательный файл: ${file}` });
  }
  return result(gate, issues.length ? 'failed' : 'passed', { checked: files.length, present, missing: issues.length }, issues);
}

async function versionAligned(rootDir: string, version: string, gate: ReleaseGateDefinition): Promise<ReleaseGateResult> {
  const issues: ReleaseGateIssue[] = [];
  const versionFile = (await readTextFile(join(rootDir, 'VERSION')).catch(() => '')).trim();
  if (versionFile !== version) issues.push({ code: 'version.version_file_mismatch', severity: gate.severity, source: 'VERSION', message: `VERSION=${versionFile || '<empty>'}, ожидается ${version}` });
  const packageFiles = ['package.json', ...await expandPackageGlobs(rootDir, arrayOfStrings(gate.config?.packageGlobs))];
  let checkedPackages = 0;
  for (const packageFile of packageFiles) {
    const absolute = join(rootDir, packageFile);
    const text = await readTextFile(absolute).catch(() => '');
    if (!text) { issues.push({ code: 'version.package_missing', severity: gate.severity, source: packageFile, message: `Не найден ${packageFile}` }); continue; }
    checkedPackages += 1;
    const parsed = JSON.parse(text) as { version?: string; name?: string };
    if (parsed.version !== version) issues.push({ code: 'version.package_mismatch', severity: gate.severity, source: packageFile, message: `${parsed.name ?? packageFile}: version=${parsed.version}, ожидается ${version}` });
  }
  const openApiPath = String(gate.config?.openApiPath ?? 'contracts/openapi/cajeerbots.openapi.yaml');
  const openApiText = await readTextFile(join(rootDir, openApiPath)).catch(() => '');
  if (!openApiText.includes(`version: ${version}`) && !openApiText.includes(`version: '${version}'`) && !openApiText.includes(`version: "${version}"`)) issues.push({ code: 'version.openapi_mismatch', severity: gate.severity, source: openApiPath, message: `OpenAPI info.version должен быть ${version}` });
  return result(gate, issues.length ? 'failed' : 'passed', { version, checkedPackages, openApiPath }, issues);
}

async function migrationsPresent(rootDir: string, gate: ReleaseGateDefinition): Promise<ReleaseGateResult> {
  const prefix = String(gate.config?.migrationPrefix ?? '');
  const migrationPath = join(rootDir, 'prisma/migrations', prefix, 'migration.sql');
  const issues: ReleaseGateIssue[] = [];
  if (!(await fileExists(migrationPath))) issues.push({ code: 'database.migration_missing', severity: gate.severity, source: relative(rootDir, migrationPath), message: `Не найдена migration ${prefix}` });
  const seed = await readTextFile(join(rootDir, 'prisma/seed.ts')).catch(() => '');
  for (const token of arrayOfStrings(gate.config?.requiredSeedStrings)) {
    if (!seed.includes(token)) issues.push({ code: 'database.seed_permission_missing', severity: gate.severity, source: 'prisma/seed.ts', message: `seed.ts не содержит ${token}` });
  }
  return result(gate, issues.length ? 'failed' : 'passed', { migrationPrefix: prefix, checkedSeedTokens: arrayOfStrings(gate.config?.requiredSeedStrings).length }, issues);
}

async function noBlockerMarkers(rootDir: string, gate: ReleaseGateDefinition): Promise<ReleaseGateResult> {
  const roots = arrayOfStrings(gate.config?.roots);
  const patterns = arrayOfStrings(gate.config?.patterns);
  const includeExtensions = arrayOfStrings(gate.config?.includeExtensions);
  const exclude = arrayOfStrings(gate.config?.exclude);
  const issues: ReleaseGateIssue[] = [];
  let scannedFiles = 0;
  for (const root of roots) {
    const absoluteRoot = join(rootDir, root);
    if (!(await directoryExists(absoluteRoot))) continue;
    const files = await listFilesRecursive(absoluteRoot, { includeExtensions, exclude, maxFiles: 10000 });
    for (const file of files) {
      scannedFiles += 1;
      const text = await readFile(file, 'utf8').catch(() => '');
      const lines = text.split(/\r?\n/);
      lines.forEach((line, index) => {
        for (const pattern of patterns) {
          if (line.includes(pattern)) issues.push({ code: 'release.blocker_marker', severity: gate.severity, source: `${relative(rootDir, file)}:${index + 1}`, message: `Найден marker "${pattern}"`, details: { line: line.trim().slice(0, 240) } });
        }
      });
    }
  }
  return result(gate, issues.length ? 'failed' : 'passed', { scannedFiles, markers: issues.length }, issues.slice(0, 500));
}

async function openApiParseable(rootDir: string, gate: ReleaseGateDefinition): Promise<ReleaseGateResult> {
  const sourcePath = String(gate.config?.sourcePath ?? 'contracts/openapi/cajeerbots.openapi.yaml');
  const minOperations = Number(gate.config?.minOperations ?? 1);
  const snapshot = buildOpenApiSnapshot(await readTextFile(join(rootDir, sourcePath)));
  const issues: ReleaseGateIssue[] = [];
  if (snapshot.operations.length < minOperations) issues.push({ code: 'openapi.too_few_operations', severity: gate.severity, source: sourcePath, message: `OpenAPI содержит ${snapshot.operations.length} operations, минимум ${minOperations}` });
  for (const duplicate of snapshot.duplicateOperationIds) issues.push({ code: 'openapi.duplicate_operation_id', severity: gate.severity, source: sourcePath, message: `Дублирующий operationId: ${duplicate}` });
  return result(gate, issues.length ? 'failed' : 'passed', { version: snapshot.version, operations: snapshot.operations.length, paths: snapshot.paths, tags: snapshot.tags.length }, issues);
}

async function docsWikiPresent(rootDir: string, gate: ReleaseGateDefinition): Promise<ReleaseGateResult> {
  const wikiRoot = join(rootDir, String(gate.config?.rootDir ?? 'wiki'));
  const minPages = Number(gate.config?.minPages ?? 1);
  const scanned = await scanWikiDirectory({ rootDir: wikiRoot, includeDrafts: false });
  const check = checkDocumentationPages(scanned.pages);
  const issues: ReleaseGateIssue[] = [];
  if (scanned.pages.length < minPages) issues.push({ code: 'docs.too_few_pages', severity: gate.severity, source: relative(rootDir, wikiRoot), message: `Wiki содержит ${scanned.pages.length} страниц, минимум ${minPages}` });
  issues.push(...check.issues.filter((issue) => issue.severity === 'error').map((issue) => ({ code: `docs.${issue.code}`, severity: gate.severity, source: issue.pageSlug, message: issue.message })));
  return result(gate, issues.length ? (gate.severity === 'warning' ? 'warning' : 'failed') : 'passed', { pages: scanned.pages.length, words: scanned.pages.reduce((sum, page) => sum + page.wordCount, 0), docsStatus: check.status }, issues.slice(0, 200));
}

function result(gate: ReleaseGateDefinition, status: ReleaseGateResult['status'], summary: Record<string, unknown>, issues: ReleaseGateIssue[]): ReleaseGateResult {
  return { code: gate.code, title: gate.title, category: gate.category, severity: gate.severity, status, summary, issues, durationMs: 0 };
}

function arrayOfStrings(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === 'string') : [];
}

async function expandPackageGlobs(rootDir: string, globs: string[]): Promise<string[]> {
  const result: string[] = [];
  for (const glob of globs) {
    const [root, star, file] = glob.split('/');
    if (!root || star !== '*' || file !== 'package.json') continue;
    const dir = join(rootDir, root);
    const entries = await import('node:fs/promises').then((fs) => fs.readdir(dir, { withFileTypes: true }).catch(() => []));
    for (const entry of entries) if (entry.isDirectory()) result.push(`${root}/${entry.name}/package.json`);
  }
  return result.sort();
}

function resolveSafe(rootDir: string, path: string): string {
  const absolute = resolve(rootDir, path.replace(/^\/+/, ''));
  if (!absolute.startsWith(rootDir)) throw new Error(`Недопустимый путь: ${path}`);
  return absolute;
}
