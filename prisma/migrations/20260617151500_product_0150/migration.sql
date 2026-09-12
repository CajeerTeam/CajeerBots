CREATE TABLE "deployment_targets" (
  "id" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "kind" TEXT NOT NULL DEFAULT 'docker_compose',
  "environment" TEXT NOT NULL DEFAULT 'production',
  "baseUrl" TEXT,
  "composeFiles" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  "envFile" TEXT,
  "status" TEXT NOT NULL DEFAULT 'unknown',
  "lastCheckedAt" TIMESTAMP(3),
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "deployment_targets_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "deployment_targets_code_key" ON "deployment_targets"("code");
CREATE INDEX "deployment_targets_kind_environment_idx" ON "deployment_targets"("kind", "environment");
CREATE INDEX "deployment_targets_status_lastCheckedAt_idx" ON "deployment_targets"("status", "lastCheckedAt");

CREATE TABLE "deployment_checks" (
  "id" TEXT NOT NULL,
  "targetId" TEXT,
  "code" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'pass',
  "severity" TEXT NOT NULL DEFAULT 'info',
  "message" TEXT NOT NULL,
  "details" JSONB,
  "checkedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "deployment_checks_pkey" PRIMARY KEY ("id")
);
CREATE INDEX "deployment_checks_targetId_checkedAt_idx" ON "deployment_checks"("targetId", "checkedAt");
CREATE INDEX "deployment_checks_status_severity_checkedAt_idx" ON "deployment_checks"("status", "severity", "checkedAt");
CREATE INDEX "deployment_checks_code_checkedAt_idx" ON "deployment_checks"("code", "checkedAt");
ALTER TABLE "deployment_checks" ADD CONSTRAINT "deployment_checks_targetId_fkey" FOREIGN KEY ("targetId") REFERENCES "deployment_targets"("id") ON DELETE SET NULL ON UPDATE CASCADE;

CREATE TABLE "deployment_runs" (
  "id" TEXT NOT NULL,
  "targetId" TEXT,
  "action" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'running',
  "actorId" TEXT,
  "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "finishedAt" TIMESTAMP(3),
  "durationMs" INTEGER,
  "logs" JSONB,
  "metadata" JSONB,
  "error" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "deployment_runs_pkey" PRIMARY KEY ("id")
);
CREATE INDEX "deployment_runs_targetId_startedAt_idx" ON "deployment_runs"("targetId", "startedAt");
CREATE INDEX "deployment_runs_action_status_startedAt_idx" ON "deployment_runs"("action", "status", "startedAt");
ALTER TABLE "deployment_runs" ADD CONSTRAINT "deployment_runs_targetId_fkey" FOREIGN KEY ("targetId") REFERENCES "deployment_targets"("id") ON DELETE SET NULL ON UPDATE CASCADE;

CREATE TABLE "release_artifacts" (
  "id" TEXT NOT NULL,
  "version" TEXT NOT NULL,
  "channel" TEXT NOT NULL DEFAULT 'alpha',
  "artifactType" TEXT NOT NULL DEFAULT 'dist',
  "filename" TEXT NOT NULL,
  "storageObjectId" TEXT,
  "sha256" TEXT NOT NULL,
  "sizeBytes" INTEGER NOT NULL,
  "manifest" JSONB,
  "status" TEXT NOT NULL DEFAULT 'created',
  "signature" TEXT,
  "signedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "release_artifacts_pkey" PRIMARY KEY ("id")
);
CREATE INDEX "release_artifacts_version_channel_idx" ON "release_artifacts"("version", "channel");
CREATE INDEX "release_artifacts_status_createdAt_idx" ON "release_artifacts"("status", "createdAt");
CREATE INDEX "release_artifacts_storageObjectId_idx" ON "release_artifacts"("storageObjectId");
