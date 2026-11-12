import { describe, expect, it } from 'vitest';
import { buildOpenApiSnapshot } from './openapi.js';
import { diffSnapshots } from './diff.js';
import { evaluateSdkCoverage } from './coverage.js';

const openApi = `openapi: 3.1.1
info:
  title: CajeerBots API
  version: 1.0.0-rc.6
paths:
  /api/health:
    get:
      operationId: health
      tags:
        - System
  /api/contracts/snapshots:
    post:
      operationId: createContractSnapshot
      tags:
        - Contracts
      security:
        - BearerAuth: []
`;

describe('contracts', () => {
  it('builds operation snapshot', () => {
    const snapshot = buildOpenApiSnapshot(openApi, new Date('2026-01-01T00:00:00Z'));
    expect(snapshot.operations).toHaveLength(2);
    expect(snapshot.unsecuredOperations).toHaveLength(0);
    expect(snapshot.tags).toContain('Contracts');
  });

  it('detects breaking removed operation', () => {
    const before = buildOpenApiSnapshot(openApi);
    const after = buildOpenApiSnapshot(openApi.replace(/  \/api\/health:[\s\S]*?  \/api\/contracts/, '  /api/contracts'));
    expect(diffSnapshots(before, after).summary.breaking).toBe(1);
  });

  it('evaluates sdk coverage by operationId', () => {
    const snapshot = buildOpenApiSnapshot(openApi);
    const coverage = evaluateSdkCoverage(snapshot, 'class Client {\n  health(): Promise<unknown> { return x }\n}\n');
    expect(coverage.covered).toBe(1);
    expect(coverage.missing[0]?.operationId).toBe('createContractSnapshot');
  });
});
