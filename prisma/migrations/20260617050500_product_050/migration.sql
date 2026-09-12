-- CajeerBots 0.5.0: runtime-ready extension system.
CREATE TABLE "extension_permission_grants" (
  "id" TEXT NOT NULL,
  "extensionId" TEXT NOT NULL,
  "permission" TEXT NOT NULL,
  "grantedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "revokedAt" TIMESTAMP(3),
  "actorId" TEXT,
  "reason" TEXT,
  CONSTRAINT "extension_permission_grants_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "extension_permission_grants_extensionId_permission_key"
  ON "extension_permission_grants"("extensionId", "permission");
CREATE INDEX "extension_permission_grants_permission_revokedAt_idx"
  ON "extension_permission_grants"("permission", "revokedAt");
ALTER TABLE "extension_permission_grants"
  ADD CONSTRAINT "extension_permission_grants_extensionId_fkey"
  FOREIGN KEY ("extensionId") REFERENCES "extensions"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "extension_runtime_events" (
  "id" TEXT NOT NULL,
  "extensionId" TEXT,
  "hook" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'pending',
  "payload" JSONB NOT NULL,
  "result" JSONB,
  "error" TEXT,
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "availableAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "startedAt" TIMESTAMP(3),
  "completedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "extension_runtime_events_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "extension_runtime_events_status_availableAt_idx"
  ON "extension_runtime_events"("status", "availableAt");
CREATE INDEX "extension_runtime_events_hook_createdAt_idx"
  ON "extension_runtime_events"("hook", "createdAt");
ALTER TABLE "extension_runtime_events"
  ADD CONSTRAINT "extension_runtime_events_extensionId_fkey"
  FOREIGN KEY ("extensionId") REFERENCES "extensions"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "extension_storage_items" (
  "id" TEXT NOT NULL,
  "extensionId" TEXT NOT NULL,
  "namespace" TEXT NOT NULL DEFAULT 'default',
  "key" TEXT NOT NULL,
  "value" JSONB NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "extension_storage_items_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "extension_storage_items_extensionId_namespace_key_key"
  ON "extension_storage_items"("extensionId", "namespace", "key");
CREATE INDEX "extension_storage_items_extensionId_namespace_idx"
  ON "extension_storage_items"("extensionId", "namespace");
ALTER TABLE "extension_storage_items"
  ADD CONSTRAINT "extension_storage_items_extensionId_fkey"
  FOREIGN KEY ("extensionId") REFERENCES "extensions"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "extension_runtime_logs" (
  "id" TEXT NOT NULL,
  "extensionId" TEXT NOT NULL,
  "level" TEXT NOT NULL,
  "message" TEXT NOT NULL,
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "extension_runtime_logs_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "extension_runtime_logs_extensionId_createdAt_idx"
  ON "extension_runtime_logs"("extensionId", "createdAt");
CREATE INDEX "extension_runtime_logs_level_createdAt_idx"
  ON "extension_runtime_logs"("level", "createdAt");
ALTER TABLE "extension_runtime_logs"
  ADD CONSTRAINT "extension_runtime_logs_extensionId_fkey"
  FOREIGN KEY ("extensionId") REFERENCES "extensions"("id") ON DELETE CASCADE ON UPDATE CASCADE;
