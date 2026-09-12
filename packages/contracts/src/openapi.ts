import { createHash } from 'node:crypto';

export type HttpMethod = 'get' | 'post' | 'put' | 'patch' | 'delete' | 'options' | 'head';

export interface OpenApiOperationSnapshot {
  method: HttpMethod;
  path: string;
  operationId: string;
  tags: string[];
  summary?: string | undefined;
  deprecated: boolean;
  secured: boolean;
}

export interface OpenApiContractSnapshot {
  format: 'cajeerbots.openapi.snapshot.v1';
  title: string;
  version: string;
  sourceSha256: string;
  generatedAt: string;
  paths: number;
  operations: OpenApiOperationSnapshot[];
  tags: string[];
  duplicateOperationIds: string[];
  unsecuredOperations: Array<{ method: HttpMethod; path: string; operationId: string }>;
  deprecatedOperations: Array<{ method: HttpMethod; path: string; operationId: string }>;
}

const methods = new Set<HttpMethod>(['get', 'post', 'put', 'patch', 'delete', 'options', 'head']);

export function sha256Hex(input: string | Uint8Array): string {
  return createHash('sha256').update(input).digest('hex');
}

export function buildOpenApiSnapshot(openApiText: string, generatedAt = new Date()): OpenApiContractSnapshot {
  const lines = openApiText.replace(/\r\n/g, '\n').split('\n');
  const title = firstYamlScalar(lines, /^\s*title:\s*(.+)$/) ?? 'CajeerBots API';
  const version = firstYamlScalar(lines, /^\s*version:\s*(.+)$/) ?? '0.0.0';
  const operations = parseOperations(lines);
  const tags = Array.from(new Set(operations.flatMap((operation) => operation.tags))).sort();
  const operationCounts = new Map<string, number>();
  for (const operation of operations) operationCounts.set(operation.operationId, (operationCounts.get(operation.operationId) ?? 0) + 1);
  const duplicateOperationIds = Array.from(operationCounts.entries()).filter(([, count]) => count > 1).map(([id]) => id).sort();
  return {
    format: 'cajeerbots.openapi.snapshot.v1',
    title,
    version,
    sourceSha256: sha256Hex(openApiText),
    generatedAt: generatedAt.toISOString(),
    paths: new Set(operations.map((operation) => operation.path)).size,
    operations,
    tags,
    duplicateOperationIds,
    unsecuredOperations: operations.filter((operation) => !operation.secured && !isPublicOperation(operation)).map(toOperationRef),
    deprecatedOperations: operations.filter((operation) => operation.deprecated).map(toOperationRef),
  };
}

export function assertOpenApiSnapshot(snapshot: OpenApiContractSnapshot): { ok: boolean; issues: string[] } {
  const issues: string[] = [];
  if (snapshot.operations.length === 0) issues.push('OpenAPI не содержит операций.');
  if (snapshot.duplicateOperationIds.length > 0) issues.push(`Дублирующиеся operationId: ${snapshot.duplicateOperationIds.join(', ')}`);
  if (snapshot.unsecuredOperations.length > 0) issues.push(`Операции без security: ${snapshot.unsecuredOperations.map((op) => `${op.method.toUpperCase()} ${op.path}`).join(', ')}`);
  const emptyIds = snapshot.operations.filter((operation) => !operation.operationId || operation.operationId.startsWith('generated_'));
  if (emptyIds.length > 0) issues.push(`Операции без явного operationId: ${emptyIds.map((op) => `${op.method.toUpperCase()} ${op.path}`).join(', ')}`);
  return { ok: issues.length === 0, issues };
}

function parseOperations(lines: string[]): OpenApiOperationSnapshot[] {
  const operations: OpenApiOperationSnapshot[] = [];
  let inPaths = false;
  let currentPath: string | undefined;
  let currentMethod: HttpMethod | undefined;
  let operationStart = 0;
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] ?? '';
    if (/^paths:\s*$/.test(line)) {
      inPaths = true;
      continue;
    }
    if (inPaths && /^\S/.test(line) && !line.startsWith('paths:')) break;
    if (!inPaths) continue;
    const pathMatch = line.match(/^\s{2}(\/[^:]+):\s*$/);
    if (pathMatch) {
      if (currentPath && currentMethod) operations.push(readOperation(lines.slice(operationStart, index), currentMethod, currentPath));
      currentPath = pathMatch[1];
      currentMethod = undefined;
      continue;
    }
    const methodMatch = line.match(/^\s{4}([a-z]+):\s*$/);
    if (methodMatch && currentPath && methods.has(methodMatch[1] as HttpMethod)) {
      if (currentMethod) operations.push(readOperation(lines.slice(operationStart, index), currentMethod, currentPath));
      currentMethod = methodMatch[1] as HttpMethod;
      operationStart = index + 1;
    }
  }
  if (currentPath && currentMethod) operations.push(readOperation(lines.slice(operationStart), currentMethod, currentPath));
  return operations.sort((a, b) => `${a.path}:${a.method}`.localeCompare(`${b.path}:${b.method}`));
}

function readOperation(block: string[], method: HttpMethod, path: string): OpenApiOperationSnapshot {
  const operationId = firstYamlScalar(block, /^\s{6}operationId:\s*(.+)$/) ?? generatedOperationId(method, path);
  const summary = firstYamlScalar(block, /^\s{6}summary:\s*(.+)$/);
  const tags = readTags(block);
  const deprecated = /\n\s{6}deprecated:\s*true\b/.test(`\n${block.join('\n')}`);
  const secured = /\n\s{6}security:\s*\n/.test(`\n${block.join('\n')}`) || /\n\s{8}-\s*BearerAuth:/.test(`\n${block.join('\n')}`) || /\n\s{8}-\s*ApiKeyAuth:/.test(`\n${block.join('\n')}`);
  return { method, path, operationId, tags, summary, deprecated, secured };
}

function readTags(block: string[]): string[] {
  const inline = firstYamlScalar(block, /^\s{6}tags:\s*\[(.+)\]\s*$/);
  if (inline) return inline.split(',').map((tag) => cleanYamlScalar(tag)).filter(Boolean);
  const tags: string[] = [];
  let inTags = false;
  for (const line of block) {
    if (/^\s{6}tags:\s*$/.test(line)) {
      inTags = true;
      continue;
    }
    if (inTags && /^\s{8}-\s+(.+)$/.test(line)) {
      tags.push(cleanYamlScalar(line.replace(/^\s{8}-\s+/, '')));
      continue;
    }
    if (inTags && /^\s{6}\S/.test(line)) break;
  }
  return tags;
}

function generatedOperationId(method: HttpMethod, path: string): string {
  return `generated_${method}_${path.replace(/[^a-zA-Z0-9]+/g, '_').replace(/^_|_$/g, '')}`;
}

function firstYamlScalar(lines: string[], pattern: RegExp): string | undefined {
  for (const line of lines) {
    const match = line.match(pattern);
    if (match?.[1]) return cleanYamlScalar(match[1]);
  }
  return undefined;
}

function cleanYamlScalar(value: string): string {
  return value.trim().replace(/^['"]|['"]$/g, '');
}

function toOperationRef(operation: OpenApiOperationSnapshot) {
  return { method: operation.method, path: operation.path, operationId: operation.operationId };
}

function isPublicOperation(operation: OpenApiOperationSnapshot): boolean {
  return operation.path === '/api/health' || operation.path === '/health' || operation.path === '/system/metrics' || operation.path === '/api/system/metrics' || operation.operationId.toLowerCase().includes('health');
}
