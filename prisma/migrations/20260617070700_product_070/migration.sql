-- CajeerBots 0.7.0: рабочий Bot Runtime MVP, provider-update journal и outbound message journal.
CREATE TYPE "BotOutboundStatus" AS ENUM ('pending', 'sending', 'sent', 'failed', 'dead');

CREATE TABLE "bot_runtime_statuses" (
  "id" TEXT NOT NULL,
  "accountId" TEXT NOT NULL,
  "provider" "BotProvider" NOT NULL,
  "mode" "BotMode" NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'starting',
  "pid" INTEGER,
  "hostname" TEXT,
  "version" TEXT NOT NULL DEFAULT '0.7.0',
  "lastBeatAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "stoppedAt" TIMESTAMP(3),
  "error" TEXT,
  "metadata" JSONB,
  CONSTRAINT "bot_runtime_statuses_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "bot_inbound_updates" (
  "id" TEXT NOT NULL,
  "accountId" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "provider" "BotProvider" NOT NULL,
  "providerUpdateId" TEXT,
  "type" TEXT NOT NULL,
  "userExternalId" TEXT,
  "chatExternalId" TEXT,
  "payload" JSONB NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'received',
  "receivedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "handledAt" TIMESTAMP(3),
  "error" TEXT,
  CONSTRAINT "bot_inbound_updates_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "bot_outbound_messages" (
  "id" TEXT NOT NULL,
  "accountId" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "inboundUpdateId" TEXT,
  "provider" "BotProvider" NOT NULL,
  "targetType" TEXT NOT NULL DEFAULT 'chat',
  "targetId" TEXT NOT NULL,
  "text" TEXT NOT NULL,
  "buttons" JSONB,
  "status" "BotOutboundStatus" NOT NULL DEFAULT 'pending',
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "providerMessageId" TEXT,
  "lastError" TEXT,
  "availableAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "sentAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "bot_outbound_messages_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "bot_inbound_updates_provider_account_update_key" ON "bot_inbound_updates"("provider", "accountId", "providerUpdateId");
CREATE INDEX "bot_runtime_statuses_account_lastbeat_idx" ON "bot_runtime_statuses"("accountId", "lastBeatAt");
CREATE INDEX "bot_runtime_statuses_status_lastbeat_idx" ON "bot_runtime_statuses"("status", "lastBeatAt");
CREATE INDEX "bot_inbound_updates_community_provider_received_idx" ON "bot_inbound_updates"("communityId", "provider", "receivedAt");
CREATE INDEX "bot_inbound_updates_status_received_idx" ON "bot_inbound_updates"("status", "receivedAt");
CREATE INDEX "bot_outbound_messages_community_provider_created_idx" ON "bot_outbound_messages"("communityId", "provider", "createdAt");
CREATE INDEX "bot_outbound_messages_status_available_idx" ON "bot_outbound_messages"("status", "availableAt");

ALTER TABLE "bot_runtime_statuses" ADD CONSTRAINT "bot_runtime_statuses_accountId_fkey" FOREIGN KEY ("accountId") REFERENCES "bot_accounts"("id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "bot_inbound_updates" ADD CONSTRAINT "bot_inbound_updates_accountId_fkey" FOREIGN KEY ("accountId") REFERENCES "bot_accounts"("id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "bot_outbound_messages" ADD CONSTRAINT "bot_outbound_messages_accountId_fkey" FOREIGN KEY ("accountId") REFERENCES "bot_accounts"("id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "bot_outbound_messages" ADD CONSTRAINT "bot_outbound_messages_inboundUpdateId_fkey" FOREIGN KEY ("inboundUpdateId") REFERENCES "bot_inbound_updates"("id") ON DELETE SET NULL ON UPDATE CASCADE;
