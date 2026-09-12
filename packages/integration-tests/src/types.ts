export type IntegrationStepKind =
  | 'api_health'
  | 'http_request'
  | 'database_count'
  | 'event_publish'
  | 'outbox_probe'
  | 'bot_scenario'
  | 'release_gate_summary';

export interface IntegrationTestStepDefinition {
  code?: string;
  title?: string;
  kind: IntegrationStepKind;
  request?: Record<string, unknown>;
  expect?: Record<string, unknown>;
  timeoutMs?: number;
}

export interface IntegrationTestDefinition {
  version?: string;
  description?: string;
  steps: IntegrationTestStepDefinition[];
}

export interface StepExecutionResult {
  code: string;
  kind: string;
  status: 'passed' | 'failed' | 'skipped';
  request: Record<string, unknown>;
  response: Record<string, unknown>;
  assertion: Record<string, unknown>;
  error?: string;
  durationMs: number;
}

export interface IntegrationRunSummary {
  status: 'passed' | 'failed' | 'skipped';
  total: number;
  passed: number;
  failed: number;
  skipped: number;
  durationMs: number;
}

export interface IntegrationRunOptions {
  prisma: any;
  baseUrl?: string;
  apiToken?: string;
  dryRun?: boolean;
  rootDir?: string;
  runId?: string;
  triggeredBy?: string;
  version?: string;
  channel?: string;
}

export interface CoverageAreaDefinition {
  code: string;
  title: string;
  requiredTargets: string[];
}
