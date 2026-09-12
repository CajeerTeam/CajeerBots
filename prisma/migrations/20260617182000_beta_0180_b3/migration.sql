-- CajeerBots 0.18.0-beta.3: Security Beta Audit, Risk Snapshots and Hardening Evidence
CREATE TABLE IF NOT EXISTS "security_audit_policies" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "category" TEXT NOT NULL,
  "severity" TEXT NOT NULL DEFAULT 'error',
  "isEnabled" BOOLEAN NOT NULL DEFAULT true,
  "config" JSONB NOT NULL DEFAULT '{}',
  "lastStatus" TEXT,
  "lastRunAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS "security_audit_policies_code_key" ON "security_audit_policies"("code");
CREATE INDEX IF NOT EXISTS "security_audit_policies_category_isEnabled_idx" ON "security_audit_policies"("category", "isEnabled");

CREATE TABLE IF NOT EXISTS "security_audit_runs" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "version" TEXT NOT NULL DEFAULT '0.18.0-beta.3',
  "channel" TEXT NOT NULL DEFAULT 'beta',
  "trigger" TEXT NOT NULL DEFAULT 'manual',
  "status" TEXT NOT NULL DEFAULT 'running',
  "summary" JSONB NOT NULL DEFAULT '{}',
  "results" JSONB NOT NULL DEFAULT '[]',
  "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "finishedAt" TIMESTAMP(3),
  "durationMs" INTEGER,
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS "security_audit_runs_version_channel_status_idx" ON "security_audit_runs"("version", "channel", "status");
CREATE INDEX IF NOT EXISTS "security_audit_runs_startedAt_idx" ON "security_audit_runs"("startedAt");

CREATE TABLE IF NOT EXISTS "security_audit_findings" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "runId" TEXT REFERENCES "security_audit_runs"("id") ON DELETE SET NULL ON UPDATE CASCADE,
  "code" TEXT NOT NULL,
  "category" TEXT NOT NULL,
  "severity" TEXT NOT NULL DEFAULT 'error',
  "status" TEXT NOT NULL DEFAULT 'open',
  "source" TEXT,
  "subjectType" TEXT,
  "subjectId" TEXT,
  "message" TEXT NOT NULL,
  "evidence" JSONB NOT NULL DEFAULT '{}',
  "remediation" TEXT,
  "resolvedAt" TIMESTAMP(3),
  "resolvedBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS "security_audit_findings_runId_idx" ON "security_audit_findings"("runId");
CREATE INDEX IF NOT EXISTS "security_audit_findings_status_severity_idx" ON "security_audit_findings"("status", "severity");
CREATE INDEX IF NOT EXISTS "security_audit_findings_category_status_idx" ON "security_audit_findings"("category", "status");
CREATE INDEX IF NOT EXISTS "security_audit_findings_subjectType_subjectId_idx" ON "security_audit_findings"("subjectType", "subjectId");

CREATE TABLE IF NOT EXISTS "security_risk_snapshots" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "version" TEXT NOT NULL DEFAULT '0.18.0-beta.3',
  "channel" TEXT NOT NULL DEFAULT 'beta',
  "status" TEXT NOT NULL DEFAULT 'unknown',
  "score" INTEGER NOT NULL DEFAULT 0,
  "summary" JSONB NOT NULL DEFAULT '{}',
  "findingsBySeverity" JSONB NOT NULL DEFAULT '{}',
  "recommendations" JSONB NOT NULL DEFAULT '[]',
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS "security_risk_snapshots_version_channel_status_idx" ON "security_risk_snapshots"("version", "channel", "status");
CREATE INDEX IF NOT EXISTS "security_risk_snapshots_score_createdAt_idx" ON "security_risk_snapshots"("score", "createdAt");
