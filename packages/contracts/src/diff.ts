import type { OpenApiContractSnapshot, OpenApiOperationSnapshot } from './openapi.js';

export interface ContractDiffResult {
  added: OpenApiOperationSnapshot[];
  removed: OpenApiOperationSnapshot[];
  changed: Array<{ before: OpenApiOperationSnapshot; after: OpenApiOperationSnapshot; changes: string[] }>;
  breaking: Array<{ method: string; path: string; operationId: string; reason: string }>;
  summary: { added: number; removed: number; changed: number; breaking: number };
}

export function diffSnapshots(before: OpenApiContractSnapshot, after: OpenApiContractSnapshot): ContractDiffResult {
  const beforeMap = byKey(before.operations);
  const afterMap = byKey(after.operations);
  const added = after.operations.filter((operation) => !beforeMap.has(keyOf(operation)));
  const removed = before.operations.filter((operation) => !afterMap.has(keyOf(operation)));
  const changed: ContractDiffResult['changed'] = [];
  for (const current of after.operations) {
    const previous = beforeMap.get(keyOf(current));
    if (!previous) continue;
    const changes = compareOperation(previous, current);
    if (changes.length > 0) changed.push({ before: previous, after: current, changes });
  }
  const breaking = [
    ...removed.map((operation) => ({ method: operation.method, path: operation.path, operationId: operation.operationId, reason: 'operation_removed' })),
    ...changed.filter((entry) => entry.changes.includes('operationId')).map((entry) => ({ method: entry.after.method, path: entry.after.path, operationId: entry.after.operationId, reason: 'operation_id_changed' })),
  ];
  return { added, removed, changed, breaking, summary: { added: added.length, removed: removed.length, changed: changed.length, breaking: breaking.length } };
}

function byKey(operations: OpenApiOperationSnapshot[]): Map<string, OpenApiOperationSnapshot> {
  return new Map(operations.map((operation) => [keyOf(operation), operation]));
}

function keyOf(operation: OpenApiOperationSnapshot): string {
  return `${operation.method.toUpperCase()} ${operation.path}`;
}

function compareOperation(before: OpenApiOperationSnapshot, after: OpenApiOperationSnapshot): string[] {
  const changes: string[] = [];
  if (before.operationId !== after.operationId) changes.push('operationId');
  if (before.deprecated !== after.deprecated) changes.push('deprecated');
  if (before.secured !== after.secured) changes.push('security');
  if (before.summary !== after.summary) changes.push('summary');
  if (before.tags.join(',') !== after.tags.join(',')) changes.push('tags');
  return changes;
}
