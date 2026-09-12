CREATE TABLE "release_api_freezes" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "version" TEXT NOT NULL,
  "channel" TEXT NOT NULL DEFAULT 'rc',
  "status" TEXT NOT NULL DEFAULT 'frozen',
  "sourcePath" TEXT NOT NULL,
  "sourceSha256" TEXT NOT NULL,
  "openApiVersion" TEXT NOT NULL,
  "operationCount" INTEGER NOT NULL DEFAULT 0,
  "operations" JSONB NOT NULL DEFAULT '[]',
  "duplicateOperationIds" JSONB NOT NULL DEFAULT '[]',
  "unsecuredOperations" JSONB NOT NULL DEFAULT '[]',
  "deprecatedOperations" JSONB NOT NULL DEFAULT '[]',
  "payload" JSONB NOT NULL DEFAULT '{}',
  "notes" TEXT,
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX "release_api_freezes_version_channel_status_idx" ON "release_api_freezes"("version", "channel", "status");
CREATE INDEX "release_api_freezes_sourceSha256_idx" ON "release_api_freezes"("sourceSha256");

CREATE TABLE "release_compatibility_targets" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "code" TEXT NOT NULL UNIQUE,
  "kind" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "versionRange" TEXT NOT NULL,
  "required" BOOLEAN NOT NULL DEFAULT true,
  "status" TEXT NOT NULL DEFAULT 'supported',
  "notes" TEXT,
  "checks" JSONB NOT NULL DEFAULT '[]',
  "lastCheckedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX "release_compatibility_targets_kind_status_idx" ON "release_compatibility_targets"("kind", "status");
CREATE INDEX "release_compatibility_targets_required_status_idx" ON "release_compatibility_targets"("required", "status");

CREATE TABLE "release_compatibility_matrices" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "version" TEXT NOT NULL,
  "channel" TEXT NOT NULL DEFAULT 'rc',
  "status" TEXT NOT NULL DEFAULT 'unknown',
  "summary" JSONB NOT NULL DEFAULT '{}',
  "targets" JSONB NOT NULL DEFAULT '[]',
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX "release_compatibility_matrices_version_channel_status_idx" ON "release_compatibility_matrices"("version", "channel", "status");
CREATE INDEX "release_compatibility_matrices_createdAt_idx" ON "release_compatibility_matrices"("createdAt");

CREATE TABLE "release_upgrade_plans" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "fromVersion" TEXT NOT NULL,
  "toVersion" TEXT NOT NULL,
  "channel" TEXT NOT NULL DEFAULT 'rc',
  "status" TEXT NOT NULL DEFAULT 'draft',
  "steps" JSONB NOT NULL DEFAULT '[]',
  "risks" JSONB NOT NULL DEFAULT '[]',
  "checks" JSONB NOT NULL DEFAULT '[]',
  "payload" JSONB NOT NULL DEFAULT '{}',
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX "release_upgrade_plans_fromVersion_toVersion_channel_idx" ON "release_upgrade_plans"("fromVersion", "toVersion", "channel");
CREATE INDEX "release_upgrade_plans_status_createdAt_idx" ON "release_upgrade_plans"("status", "createdAt");

CREATE TABLE "release_upgrade_runs" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "planId" TEXT NOT NULL,
  "mode" TEXT NOT NULL DEFAULT 'dry-run',
  "status" TEXT NOT NULL DEFAULT 'running',
  "rootDir" TEXT,
  "results" JSONB NOT NULL DEFAULT '[]',
  "logs" JSONB NOT NULL DEFAULT '[]',
  "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "finishedAt" TIMESTAMP(3),
  "durationMs" INTEGER,
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "release_upgrade_runs_planId_fkey" FOREIGN KEY ("planId") REFERENCES "release_upgrade_plans"("id") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX "release_upgrade_runs_planId_status_idx" ON "release_upgrade_runs"("planId", "status");
CREATE INDEX "release_upgrade_runs_mode_createdAt_idx" ON "release_upgrade_runs"("mode", "createdAt");

CREATE TABLE "release_upgrade_checks" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "version" TEXT NOT NULL,
  "channel" TEXT NOT NULL DEFAULT 'rc',
  "code" TEXT NOT NULL,
  "status" TEXT NOT NULL,
  "severity" TEXT NOT NULL DEFAULT 'info',
  "message" TEXT NOT NULL,
  "details" JSONB NOT NULL DEFAULT '{}',
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX "release_upgrade_checks_version_channel_status_idx" ON "release_upgrade_checks"("version", "channel", "status");
CREATE INDEX "release_upgrade_checks_code_createdAt_idx" ON "release_upgrade_checks"("code", "createdAt");

CREATE TABLE "release_candidate_readiness_runs" (
  "id" TEXT NOT NULL PRIMARY KEY,
  "version" TEXT NOT NULL,
  "channel" TEXT NOT NULL DEFAULT 'rc',
  "status" TEXT NOT NULL DEFAULT 'running',
  "rootDir" TEXT,
  "summary" JSONB NOT NULL DEFAULT '{}',
  "sections" JSONB NOT NULL DEFAULT '[]',
  "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "finishedAt" TIMESTAMP(3),
  "durationMs" INTEGER,
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX "release_candidate_readiness_runs_version_channel_status_idx" ON "release_candidate_readiness_runs"("version", "channel", "status");
CREATE INDEX "release_candidate_readiness_runs_startedAt_idx" ON "release_candidate_readiness_runs"("startedAt");
