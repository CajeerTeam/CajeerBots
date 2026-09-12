export type ReleaseChannel = 'alpha' | 'beta' | 'rc' | 'stable' | 'lts' | 'security';
export type GateStatus = 'passed' | 'warning' | 'failed' | 'skipped';
export type GateSeverity = 'info' | 'warning' | 'error' | 'blocker';

export interface ReleaseGateDefinition {
  code: string;
  title: string;
  category: string;
  severity: GateSeverity;
  enabled: boolean;
  config?: Record<string, unknown>;
}

export interface ReleaseGateIssue {
  code: string;
  severity: GateSeverity;
  message: string;
  source?: string;
  details?: Record<string, unknown>;
}

export interface ReleaseGateResult {
  code: string;
  title: string;
  category: string;
  status: GateStatus;
  severity: GateSeverity;
  summary: Record<string, unknown>;
  issues: ReleaseGateIssue[];
  durationMs: number;
}

export interface ReleaseGateRunSummary {
  status: 'passed' | 'warning' | 'failed';
  total: number;
  passed: number;
  warning: number;
  failed: number;
  skipped: number;
  blockers: number;
  version: string;
  channel: ReleaseChannel;
}

export interface ReleaseGateRunReport {
  version: string;
  channel: ReleaseChannel;
  status: ReleaseGateRunSummary['status'];
  summary: ReleaseGateRunSummary;
  results: ReleaseGateResult[];
  generatedAt: string;
}

export interface ProjectFileCheck {
  path: string;
  required?: boolean;
  description?: string;
}

export interface ReleaseCandidateSummary {
  version: string;
  channel: ReleaseChannel;
  status: ReleaseGateRunSummary['status'];
  gateRunId?: string;
  generatedAt: string;
  highlights: string[];
  blockers: ReleaseGateIssue[];
}
