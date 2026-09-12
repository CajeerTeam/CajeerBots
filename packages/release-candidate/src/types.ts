export type ReleaseCandidateChannel = 'rc' | 'stable' | 'lts' | 'security';

export interface RcOperationRef {
  method: string;
  path: string;
  operationId: string;
  deprecated?: boolean;
  tags?: string[];
}

export interface ApiFreezeSnapshotPayload {
  format: 'cajeerbots.api-freeze.v1';
  version: string;
  channel: ReleaseCandidateChannel;
  sourcePath: string;
  sourceSha256: string;
  openApiVersion: string;
  operationCount: number;
  operations: RcOperationRef[];
  duplicateOperationIds: string[];
  unsecuredOperations: RcOperationRef[];
  deprecatedOperations: RcOperationRef[];
  generatedAt: string;
}

export interface ApiFreezeVerificationIssue {
  code: string;
  severity: 'info' | 'warning' | 'error' | 'blocker';
  message: string;
  source?: string;
  details?: Record<string, unknown>;
}

export interface ApiFreezeVerificationReport {
  status: 'passed' | 'warning' | 'failed';
  version: string;
  channel: ReleaseCandidateChannel;
  frozenOperations: number;
  currentOperations: number;
  removedOperations: RcOperationRef[];
  addedOperations: RcOperationRef[];
  changedOperationIds: Array<{ before: RcOperationRef; after: RcOperationRef }>;
  issues: ApiFreezeVerificationIssue[];
  generatedAt: string;
}

export interface CompatibilityTargetDefinition {
  code: string;
  kind: 'runtime' | 'database' | 'cache' | 'queue' | 'provider' | 'deployment' | 'tooling';
  name: string;
  versionRange: string;
  required: boolean;
  status?: 'supported' | 'experimental' | 'deprecated' | 'blocked';
  notes?: string;
  checks?: Array<{ code: string; title: string; command?: string; env?: string; severity?: string }>;
}

export interface CompatibilityMatrixReport {
  format: 'cajeerbots.compatibility-matrix.v1';
  version: string;
  channel: ReleaseCandidateChannel;
  status: 'passed' | 'warning' | 'failed';
  targets: CompatibilityTargetDefinition[];
  summary: {
    total: number;
    required: number;
    supported: number;
    experimental: number;
    deprecated: number;
    blocked: number;
  };
  generatedAt: string;
}

export interface UpgradePlanStep {
  code: string;
  title: string;
  phase: 'preflight' | 'backup' | 'migrate' | 'verify' | 'rollback';
  required: boolean;
  command?: string;
  description?: string;
}

export interface UpgradePlanPayload {
  format: 'cajeerbots.upgrade-plan.v1';
  fromVersion: string;
  toVersion: string;
  channel: ReleaseCandidateChannel;
  status: 'draft' | 'ready' | 'blocked';
  steps: UpgradePlanStep[];
  risks: Array<{ code: string; severity: string; message: string }>;
  checks: Array<{ code: string; status: string; message: string }>;
  generatedAt: string;
}

export interface ReadinessSection {
  code: string;
  status: 'passed' | 'warning' | 'failed';
  summary: Record<string, unknown>;
  issues: ApiFreezeVerificationIssue[];
}

export interface RcReadinessReport {
  format: 'cajeerbots.rc-readiness.v1';
  version: string;
  channel: ReleaseCandidateChannel;
  status: 'passed' | 'warning' | 'failed';
  sections: ReadinessSection[];
  summary: Record<string, unknown>;
  generatedAt: string;
}
