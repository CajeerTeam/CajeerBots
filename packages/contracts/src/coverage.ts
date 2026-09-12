import type { OpenApiContractSnapshot } from './openapi.js';

export interface SdkCoverageResult {
  totalOperations: number;
  covered: number;
  missing: Array<{ operationId: string; method: string; path: string }>;
  extraMethods: string[];
  percent: number;
}

export function evaluateSdkCoverage(snapshot: OpenApiContractSnapshot, sdkSource: string): SdkCoverageResult {
  const methodNames = extractSdkMethodNames(sdkSource);
  const missing = snapshot.operations
    .filter((operation) => !methodNames.has(operation.operationId) && !methodNames.has(toCamel(operation.operationId)) && !sdkSource.includes(operation.path) && !sdkSource.includes(`/api${operation.path}`))
    .map((operation) => ({ operationId: operation.operationId, method: operation.method, path: operation.path }));
  const operationIds = new Set(snapshot.operations.flatMap((operation) => [operation.operationId, toCamel(operation.operationId)]));
  const extraMethods = Array.from(methodNames).filter((name) => !operationIds.has(name) && !['request'].includes(name)).sort();
  const covered = snapshot.operations.length - missing.length;
  return { totalOperations: snapshot.operations.length, covered, missing, extraMethods, percent: snapshot.operations.length === 0 ? 100 : Math.round((covered / snapshot.operations.length) * 10000) / 100 };
}

export function extractSdkMethodNames(source: string): Set<string> {
  const names = new Set<string>();
  const pattern = /\n\s{2,}([a-zA-Z_$][\w$]*)\s*\([^)]*\)\s*:/g;
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(`\n${source}`))) {
    if (match[1]) names.add(match[1]);
  }
  return names;
}

function toCamel(value: string): string {
  return value.replace(/[-_\s]+([a-zA-Z0-9])/g, (_, letter: string) => letter.toUpperCase());
}
