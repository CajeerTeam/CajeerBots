CREATE TABLE IF NOT EXISTS "storage_objects" (
  "id" TEXT NOT NULL,
  "key" TEXT NOT NULL,
  "driver" TEXT NOT NULL DEFAULT 'local',
  "bucket" TEXT,
  "contentType" TEXT NOT NULL DEFAULT 'application/octet-stream',
  "sizeBytes" INTEGER NOT NULL,
  "sha256" TEXT NOT NULL,
  "metadata" JSONB,
  "refType" TEXT,
  "refId" TEXT,
  "status" TEXT NOT NULL DEFAULT 'active',
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  "deletedAt" TIMESTAMP(3),
  CONSTRAINT "storage_objects_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX IF NOT EXISTS "storage_objects_key_key" ON "storage_objects"("key");
CREATE INDEX IF NOT EXISTS "storage_objects_driver_status_idx" ON "storage_objects"("driver", "status");
CREATE INDEX IF NOT EXISTS "storage_objects_refType_refId_idx" ON "storage_objects"("refType", "refId");
CREATE INDEX IF NOT EXISTS "storage_objects_status_deletedAt_idx" ON "storage_objects"("status", "deletedAt");

CREATE TABLE IF NOT EXISTS "backup_artifacts" (
  "id" TEXT NOT NULL,
  "scope" TEXT NOT NULL DEFAULT 'community',
  "communityId" TEXT,
  "title" TEXT NOT NULL,
  "storageObjectId" TEXT,
  "storageKey" TEXT NOT NULL,
  "format" TEXT NOT NULL DEFAULT 'cajeerbots.backup.bundle.v1',
  "sizeBytes" INTEGER NOT NULL,
  "sha256" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'created',
  "includedResources" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  "encryptionRef" TEXT,
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "completedAt" TIMESTAMP(3),
  "error" TEXT,
  CONSTRAINT "backup_artifacts_pkey" PRIMARY KEY ("id")
);
CREATE INDEX IF NOT EXISTS "backup_artifacts_scope_communityId_createdAt_idx" ON "backup_artifacts"("scope", "communityId", "createdAt");
CREATE INDEX IF NOT EXISTS "backup_artifacts_status_createdAt_idx" ON "backup_artifacts"("status", "createdAt");
CREATE INDEX IF NOT EXISTS "backup_artifacts_storageObjectId_idx" ON "backup_artifacts"("storageObjectId");

CREATE TABLE IF NOT EXISTS "backup_restore_runs" (
  "id" TEXT NOT NULL,
  "artifactId" TEXT NOT NULL,
  "communityId" TEXT,
  "mode" TEXT NOT NULL DEFAULT 'dry-run',
  "strategy" TEXT NOT NULL DEFAULT 'merge',
  "status" TEXT NOT NULL DEFAULT 'running',
  "actorId" TEXT,
  "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "finishedAt" TIMESTAMP(3),
  "durationMs" INTEGER,
  "result" JSONB,
  "error" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "backup_restore_runs_pkey" PRIMARY KEY ("id")
);
CREATE INDEX IF NOT EXISTS "backup_restore_runs_artifactId_createdAt_idx" ON "backup_restore_runs"("artifactId", "createdAt");
CREATE INDEX IF NOT EXISTS "backup_restore_runs_communityId_status_createdAt_idx" ON "backup_restore_runs"("communityId", "status", "createdAt");
