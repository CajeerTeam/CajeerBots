CREATE TYPE "DeliveryStatus" AS ENUM ('pending', 'processing', 'delivered', 'failed', 'dead');

CREATE INDEX IF NOT EXISTS "bot_accounts_provider_isActive_idx" ON "bot_accounts"("provider", "isActive");
CREATE INDEX IF NOT EXISTS "users_communityId_provider_idx" ON "users"("communityId", "provider");
CREATE INDEX IF NOT EXISTS "api_keys_revokedAt_expiresAt_idx" ON "api_keys"("revokedAt", "expiresAt");
CREATE INDEX IF NOT EXISTS "setting_history_settingId_createdAt_idx" ON "setting_history"("settingId", "createdAt");
CREATE INDEX IF NOT EXISTS "extensions_status_installedAt_idx" ON "extensions"("status", "installedAt");
CREATE INDEX IF NOT EXISTS "extension_config_history_extensionId_createdAt_idx" ON "extension_config_history"("extensionId", "createdAt");
CREATE INDEX IF NOT EXISTS "audit_logs_resource_resourceId_idx" ON "audit_logs"("resource", "resourceId");
CREATE INDEX IF NOT EXISTS "webhook_endpoints_isActive_idx" ON "webhook_endpoints"("isActive");

DROP INDEX IF EXISTS "message_templates_key_key";
CREATE UNIQUE INDEX IF NOT EXISTS "message_templates_key_locale_key" ON "message_templates"("key", "locale");

CREATE TABLE "webhook_deliveries" (
  "id" TEXT NOT NULL,
  "outboxMessageId" TEXT NOT NULL,
  "endpointId" TEXT NOT NULL,
  "status" "DeliveryStatus" NOT NULL DEFAULT 'pending',
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "responseStatus" INTEGER,
  "responseBody" TEXT,
  "lastError" TEXT,
  "availableAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "deliveredAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "webhook_deliveries_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "webhook_deliveries_outboxMessageId_endpointId_key" ON "webhook_deliveries"("outboxMessageId", "endpointId");
CREATE INDEX "webhook_deliveries_status_availableAt_idx" ON "webhook_deliveries"("status", "availableAt");
ALTER TABLE "webhook_deliveries" ADD CONSTRAINT "webhook_deliveries_outboxMessageId_fkey" FOREIGN KEY ("outboxMessageId") REFERENCES "outbox_messages"("id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "webhook_deliveries" ADD CONSTRAINT "webhook_deliveries_endpointId_fkey" FOREIGN KEY ("endpointId") REFERENCES "webhook_endpoints"("id") ON DELETE CASCADE ON UPDATE CASCADE;
