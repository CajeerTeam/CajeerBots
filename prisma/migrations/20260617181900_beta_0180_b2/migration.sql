-- CajeerBots 0.18.0-beta.2: Integration Testing & MVP Regression Runs
CREATE TABLE IF NOT EXISTS "integration_test_suites" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "category" TEXT NOT NULL DEFAULT 'mvp',
  "description" TEXT,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "metadata" JSONB NOT NULL DEFAULT '{}',
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS "integration_test_suites_code_key" ON "integration_test_suites"("code");
CREATE INDEX IF NOT EXISTS "integration_test_suites_category_isActive_idx" ON "integration_test_suites"("category", "isActive");

CREATE TABLE IF NOT EXISTS "integration_test_cases" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "suiteId" TEXT NOT NULL REFERENCES "integration_test_suites"("id") ON DELETE CASCADE ON UPDATE CASCADE,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "target" TEXT NOT NULL DEFAULT 'api',
  "kind" TEXT NOT NULL DEFAULT 'multi_step',
  "provider" TEXT,
  "priority" INTEGER NOT NULL DEFAULT 100,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "timeoutMs" INTEGER NOT NULL DEFAULT 30000,
  "definition" JSONB NOT NULL DEFAULT '{}',
  "expected" JSONB NOT NULL DEFAULT '{}',
  "lastStatus" TEXT,
  "lastRunAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS "integration_test_cases_suiteId_code_key" ON "integration_test_cases"("suiteId", "code");
CREATE INDEX IF NOT EXISTS "integration_test_cases_suiteId_isActive_priority_idx" ON "integration_test_cases"("suiteId", "isActive", "priority");
CREATE INDEX IF NOT EXISTS "integration_test_cases_target_kind_provider_idx" ON "integration_test_cases"("target", "kind", "provider");

CREATE TABLE IF NOT EXISTS "integration_test_runs" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "suiteId" TEXT REFERENCES "integration_test_suites"("id") ON DELETE SET NULL ON UPDATE CASCADE,
  "caseId" TEXT REFERENCES "integration_test_cases"("id") ON DELETE SET NULL ON UPDATE CASCADE,
  "version" TEXT NOT NULL DEFAULT '0.18.0-beta.2',
  "channel" TEXT NOT NULL DEFAULT 'beta',
  "trigger" TEXT NOT NULL DEFAULT 'manual',
  "status" TEXT NOT NULL DEFAULT 'queued',
  "rootDir" TEXT,
  "startedAt" TIMESTAMP(3),
  "finishedAt" TIMESTAMP(3),
  "durationMs" INTEGER,
  "summary" JSONB NOT NULL DEFAULT '{}',
  "environment" JSONB NOT NULL DEFAULT '{}',
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS "integration_test_runs_version_channel_status_idx" ON "integration_test_runs"("version", "channel", "status");
CREATE INDEX IF NOT EXISTS "integration_test_runs_suiteId_status_createdAt_idx" ON "integration_test_runs"("suiteId", "status", "createdAt");
CREATE INDEX IF NOT EXISTS "integration_test_runs_caseId_status_createdAt_idx" ON "integration_test_runs"("caseId", "status", "createdAt");

CREATE TABLE IF NOT EXISTS "integration_test_step_runs" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "runId" TEXT NOT NULL REFERENCES "integration_test_runs"("id") ON DELETE CASCADE ON UPDATE CASCADE,
  "caseId" TEXT REFERENCES "integration_test_cases"("id") ON DELETE SET NULL ON UPDATE CASCADE,
  "stepIndex" INTEGER NOT NULL,
  "code" TEXT NOT NULL,
  "kind" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'running',
  "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "finishedAt" TIMESTAMP(3),
  "durationMs" INTEGER,
  "request" JSONB NOT NULL DEFAULT '{}',
  "response" JSONB NOT NULL DEFAULT '{}',
  "assertion" JSONB NOT NULL DEFAULT '{}',
  "error" TEXT
);
CREATE INDEX IF NOT EXISTS "integration_test_step_runs_runId_stepIndex_idx" ON "integration_test_step_runs"("runId", "stepIndex");
CREATE INDEX IF NOT EXISTS "integration_test_step_runs_caseId_status_idx" ON "integration_test_step_runs"("caseId", "status");
CREATE INDEX IF NOT EXISTS "integration_test_step_runs_kind_status_idx" ON "integration_test_step_runs"("kind", "status");

CREATE TABLE IF NOT EXISTS "integration_test_coverage" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "version" TEXT NOT NULL DEFAULT '0.18.0-beta.2',
  "channel" TEXT NOT NULL DEFAULT 'beta',
  "status" TEXT NOT NULL DEFAULT 'unknown',
  "source" TEXT NOT NULL DEFAULT 'database',
  "summary" JSONB NOT NULL DEFAULT '{}',
  "areas" JSONB NOT NULL DEFAULT '[]',
  "missing" JSONB NOT NULL DEFAULT '[]',
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS "integration_test_coverage_version_channel_status_idx" ON "integration_test_coverage"("version", "channel", "status");
CREATE INDEX IF NOT EXISTS "integration_test_coverage_source_createdAt_idx" ON "integration_test_coverage"("source", "createdAt");
