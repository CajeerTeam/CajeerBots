-- CajeerBots 0.10.0: Registry/Updates product layer

ALTER TABLE "extensions"
  ADD COLUMN IF NOT EXISTS "registrySourceId" TEXT,
  ADD COLUMN IF NOT EXISTS "registryPackageName" TEXT,
  ADD COLUMN IF NOT EXISTS "registryVersionId" TEXT;

CREATE TABLE IF NOT EXISTS "registry_sources" (
  "id" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "url" TEXT NOT NULL,
  "type" TEXT NOT NULL DEFAULT 'gitflic_registry',
  "authSecretRef" TEXT,
  "publicKey" TEXT,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "lastSyncAt" TIMESTAMP(3),
  "syncStatus" TEXT NOT NULL DEFAULT 'never',
  "syncError" TEXT,
  "packageCount" INTEGER NOT NULL DEFAULT 0,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "registry_sources_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "registry_sources_name_key" ON "registry_sources"("name");
CREATE INDEX IF NOT EXISTS "registry_sources_isActive_lastSyncAt_idx" ON "registry_sources"("isActive", "lastSyncAt");

CREATE TABLE IF NOT EXISTS "registry_packages" (
  "id" TEXT NOT NULL,
  "sourceId" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "displayName" TEXT,
  "description" TEXT,
  "kind" TEXT NOT NULL DEFAULT 'plugin',
  "publisher" TEXT,
  "homepage" TEXT,
  "repository" TEXT,
  "license" TEXT,
  "isOfficial" BOOLEAN NOT NULL DEFAULT false,
  "latestVersion" TEXT,
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "registry_packages_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "registry_packages_sourceId_name_key" ON "registry_packages"("sourceId", "name");
CREATE INDEX IF NOT EXISTS "registry_packages_name_kind_idx" ON "registry_packages"("name", "kind");
ALTER TABLE "registry_packages" ADD CONSTRAINT "registry_packages_sourceId_fkey" FOREIGN KEY ("sourceId") REFERENCES "registry_sources"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "registry_package_versions" (
  "id" TEXT NOT NULL,
  "packageId" TEXT NOT NULL,
  "version" TEXT NOT NULL,
  "engineRange" TEXT NOT NULL DEFAULT '*',
  "channel" TEXT NOT NULL DEFAULT 'stable',
  "manifest" JSONB,
  "artifactUrl" TEXT NOT NULL,
  "artifactSha256" TEXT NOT NULL,
  "artifactSize" INTEGER,
  "signature" TEXT,
  "signatureAlgorithm" TEXT NOT NULL DEFAULT 'none',
  "releaseNotes" TEXT,
  "isYanked" BOOLEAN NOT NULL DEFAULT false,
  "metadata" JSONB,
  "publishedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "registry_package_versions_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "registry_package_versions_packageId_version_key" ON "registry_package_versions"("packageId", "version");
CREATE INDEX IF NOT EXISTS "registry_package_versions_channel_isYanked_version_idx" ON "registry_package_versions"("channel", "isYanked", "version");
ALTER TABLE "registry_package_versions" ADD CONSTRAINT "registry_package_versions_packageId_fkey" FOREIGN KEY ("packageId") REFERENCES "registry_packages"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "update_plans" (
  "id" TEXT NOT NULL,
  "targetType" TEXT NOT NULL,
  "targetRef" TEXT,
  "packageName" TEXT NOT NULL,
  "currentVersion" TEXT NOT NULL,
  "targetVersion" TEXT NOT NULL,
  "sourceId" TEXT,
  "versionId" TEXT,
  "status" TEXT NOT NULL DEFAULT 'planned',
  "steps" JSONB NOT NULL,
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "appliedAt" TIMESTAMP(3),
  "error" TEXT,
  CONSTRAINT "update_plans_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "update_plans_targetType_targetRef_status_idx" ON "update_plans"("targetType", "targetRef", "status");
CREATE INDEX IF NOT EXISTS "update_plans_packageName_targetVersion_idx" ON "update_plans"("packageName", "targetVersion");
ALTER TABLE "update_plans" ADD CONSTRAINT "update_plans_sourceId_fkey" FOREIGN KEY ("sourceId") REFERENCES "registry_sources"("id") ON DELETE SET NULL ON UPDATE CASCADE;
ALTER TABLE "update_plans" ADD CONSTRAINT "update_plans_versionId_fkey" FOREIGN KEY ("versionId") REFERENCES "registry_package_versions"("id") ON DELETE SET NULL ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "update_history" (
  "id" TEXT NOT NULL,
  "planId" TEXT,
  "targetType" TEXT NOT NULL,
  "targetRef" TEXT,
  "packageName" TEXT NOT NULL,
  "fromVersion" TEXT NOT NULL,
  "toVersion" TEXT NOT NULL,
  "status" TEXT NOT NULL,
  "steps" JSONB,
  "metadata" JSONB,
  "error" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "completedAt" TIMESTAMP(3),
  CONSTRAINT "update_history_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "update_history_targetType_targetRef_createdAt_idx" ON "update_history"("targetType", "targetRef", "createdAt");
CREATE INDEX IF NOT EXISTS "update_history_packageName_createdAt_idx" ON "update_history"("packageName", "createdAt");
ALTER TABLE "update_history" ADD CONSTRAINT "update_history_planId_fkey" FOREIGN KEY ("planId") REFERENCES "update_plans"("id") ON DELETE SET NULL ON UPDATE CASCADE;

ALTER TABLE "bot_runtime_statuses" ALTER COLUMN "version" SET DEFAULT '0.10.0';
