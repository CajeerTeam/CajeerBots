export type SecurityAuditSeverity = 'info' | 'warning' | 'error' | 'critical';
export type SecurityAuditStatus = 'passed' | 'warning' | 'failed' | 'skipped';

export interface SecurityAuditPolicyDefinition {
  code: string;
  title: string;
  category: string;
  severity: SecurityAuditSeverity;
  enabled?: boolean;
  config?: Record<string, unknown>;
}

export interface SecurityAuditFindingInput {
  code: string;
  category: string;
  severity: SecurityAuditSeverity;
  source?: string;
  subjectType?: string;
  subjectId?: string;
  message: string;
  evidence?: Record<string, unknown>;
  remediation?: string;
}

export interface SecurityAuditPolicyResult {
  code: string;
  title: string;
  category: string;
  severity: SecurityAuditSeverity;
  status: SecurityAuditStatus;
  summary: Record<string, unknown>;
  findings: SecurityAuditFindingInput[];
  durationMs: number;
}

export interface SecurityAuditSummary {
  version: string;
  channel: string;
  status: SecurityAuditStatus;
  totalPolicies: number;
  passed: number;
  warning: number;
  failed: number;
  skipped: number;
  findings: number;
  critical: number;
  errors: number;
  warnings: number;
  riskScore: number;
}

export interface SecurityAuditReport {
  version: string;
  channel: string;
  status: SecurityAuditStatus;
  summary: SecurityAuditSummary;
  results: SecurityAuditPolicyResult[];
  generatedAt: string;
}

export interface RunSecurityAuditOptions {
  prisma: any;
  policies?: SecurityAuditPolicyDefinition[];
  version?: string;
  channel?: string;
  trigger?: string;
  createdBy?: string;
  persist?: boolean;
  now?: Date;
}
