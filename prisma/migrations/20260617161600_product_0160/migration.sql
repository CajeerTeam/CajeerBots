-- CajeerBots 0.16.0: Contract-first API governance and SDK generation history
CREATE TABLE IF NOT EXISTS "api_contract_snapshots" (
  "id" TEXT PRIMARY KEY,
  "version" TEXT NOT NULL,
  "sourcePath" TEXT NOT NULL DEFAULT 'contracts/openapi/cajeerbots.openapi.yaml',
  "sourceSha256" TEXT NOT NULL,
  "operationCount" INTEGER NOT NULL,
  "pathCount" INTEGER NOT NULL,
  "tagCount" INTEGER NOT NULL,
  "duplicateOperationIds" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  "unsecuredOperations" JSONB,
  "deprecatedOperations" JSONB,
  "snapshot" JSONB NOT NULL,
  "isBaseline" BOOLEAN NOT NULL DEFAULT false,
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS "api_contract_snapshots_version_createdAt_idx" ON "api_contract_snapshots"("version", "createdAt");
CREATE INDEX IF NOT EXISTS "api_contract_snapshots_isBaseline_createdAt_idx" ON "api_contract_snapshots"("isBaseline", "createdAt");
CREATE INDEX IF NOT EXISTS "api_contract_snapshots_sourceSha256_idx" ON "api_contract_snapshots"("sourceSha256");

CREATE TABLE IF NOT EXISTS "api_contract_checks" (
  "id" TEXT PRIMARY KEY,
  "snapshotId" TEXT,
  "baselineId" TEXT,
  "status" TEXT NOT NULL DEFAULT 'passed',
  "checkType" TEXT NOT NULL DEFAULT 'contract',
  "summary" JSONB,
  "issues" JSONB,
  "breakingCount" INTEGER NOT NULL DEFAULT 0,
  "coveragePercent" DOUBLE PRECISION,
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "api_contract_checks_snapshotId_fkey" FOREIGN KEY ("snapshotId") REFERENCES "api_contract_snapshots"("id") ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS "api_contract_checks_status_checkType_createdAt_idx" ON "api_contract_checks"("status", "checkType", "createdAt");
CREATE INDEX IF NOT EXISTS "api_contract_checks_snapshotId_createdAt_idx" ON "api_contract_checks"("snapshotId", "createdAt");
CREATE INDEX IF NOT EXISTS "api_contract_checks_baselineId_createdAt_idx" ON "api_contract_checks"("baselineId", "createdAt");

CREATE TABLE IF NOT EXISTS "sdk_generation_runs" (
  "id" TEXT PRIMARY KEY,
  "snapshotId" TEXT,
  "status" TEXT NOT NULL DEFAULT 'generated',
  "generatorVersion" TEXT NOT NULL DEFAULT 'cajeerbots.contracts.v1',
  "outputMode" TEXT NOT NULL DEFAULT 'database',
  "artifactName" TEXT,
  "artifactSha256" TEXT,
  "artifactSizeBytes" INTEGER,
  "operationCount" INTEGER NOT NULL DEFAULT 0,
  "coveragePercent" DOUBLE PRECISION,
  "missingOperations" JSONB,
  "generatedFiles" JSONB,
  "error" TEXT,
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "sdk_generation_runs_snapshotId_fkey" FOREIGN KEY ("snapshotId") REFERENCES "api_contract_snapshots"("id") ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS "sdk_generation_runs_status_createdAt_idx" ON "sdk_generation_runs"("status", "createdAt");
CREATE INDEX IF NOT EXISTS "sdk_generation_runs_snapshotId_createdAt_idx" ON "sdk_generation_runs"("snapshotId", "createdAt");

CREATE TABLE IF NOT EXISTS "api_deprecations" (
  "id" TEXT PRIMARY KEY,
  "operationId" TEXT NOT NULL UNIQUE,
  "method" TEXT NOT NULL,
  "path" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'planned',
  "reason" TEXT,
  "replacement" TEXT,
  "deprecatedSince" TEXT,
  "removeAfter" TEXT,
  "metadata" JSONB,
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS "api_deprecations_status_removeAfter_idx" ON "api_deprecations"("status", "removeAfter");
CREATE INDEX IF NOT EXISTS "api_deprecations_method_path_idx" ON "api_deprecations"("method", "path");
