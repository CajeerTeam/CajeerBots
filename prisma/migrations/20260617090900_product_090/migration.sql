-- CajeerBots 0.9.0: Durable Event API, subscriptions, replay and dead-letter queue.

CREATE TABLE IF NOT EXISTS "event_topics" (
  "id" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "description" TEXT,
  "schema" JSONB,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "retentionDays" INTEGER NOT NULL DEFAULT 30,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "event_topics_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "event_topics_name_key" ON "event_topics"("name");
CREATE INDEX IF NOT EXISTS "event_topics_isActive_updatedAt_idx" ON "event_topics"("isActive", "updatedAt");

CREATE TABLE IF NOT EXISTS "event_records" (
  "id" TEXT NOT NULL,
  "eventId" TEXT NOT NULL,
  "topicId" TEXT,
  "topicName" TEXT NOT NULL,
  "type" TEXT NOT NULL,
  "source" TEXT NOT NULL DEFAULT 'api',
  "communityId" TEXT,
  "aggregateType" TEXT,
  "aggregateId" TEXT,
  "payload" JSONB NOT NULL,
  "metadata" JSONB,
  "headers" JSONB,
  "outboxMessageId" TEXT,
  "replayCount" INTEGER NOT NULL DEFAULT 0,
  "replayedAt" TIMESTAMP(3),
  "publishedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "event_records_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "event_records_eventId_key" ON "event_records"("eventId");
CREATE INDEX IF NOT EXISTS "event_records_topicName_publishedAt_idx" ON "event_records"("topicName", "publishedAt");
CREATE INDEX IF NOT EXISTS "event_records_type_publishedAt_idx" ON "event_records"("type", "publishedAt");
CREATE INDEX IF NOT EXISTS "event_records_communityId_publishedAt_idx" ON "event_records"("communityId", "publishedAt");
CREATE INDEX IF NOT EXISTS "event_records_aggregateType_aggregateId_idx" ON "event_records"("aggregateType", "aggregateId");
ALTER TABLE "event_records" ADD CONSTRAINT "event_records_topicId_fkey" FOREIGN KEY ("topicId") REFERENCES "event_topics"("id") ON DELETE SET NULL ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "event_subscriptions" (
  "id" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "topicId" TEXT,
  "topicPattern" TEXT NOT NULL,
  "targetType" TEXT NOT NULL,
  "targetRef" TEXT NOT NULL,
  "filters" JSONB,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "maxAttempts" INTEGER NOT NULL DEFAULT 8,
  "lastDeliveredAt" TIMESTAMP(3),
  "disabledAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "event_subscriptions_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "event_subscriptions_name_key" ON "event_subscriptions"("name");
CREATE INDEX IF NOT EXISTS "event_subscriptions_isActive_topicPattern_idx" ON "event_subscriptions"("isActive", "topicPattern");
CREATE INDEX IF NOT EXISTS "event_subscriptions_targetType_targetRef_idx" ON "event_subscriptions"("targetType", "targetRef");
ALTER TABLE "event_subscriptions" ADD CONSTRAINT "event_subscriptions_topicId_fkey" FOREIGN KEY ("topicId") REFERENCES "event_topics"("id") ON DELETE SET NULL ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "event_subscription_deliveries" (
  "id" TEXT NOT NULL,
  "subscriptionId" TEXT NOT NULL,
  "eventRecordId" TEXT NOT NULL,
  "status" "DeliveryStatus" NOT NULL DEFAULT 'pending',
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "availableAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "lockedAt" TIMESTAMP(3),
  "responseStatus" INTEGER,
  "responseBody" TEXT,
  "result" JSONB,
  "lastError" TEXT,
  "deliveredAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "event_subscription_deliveries_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "event_subscription_deliveries_subscriptionId_eventRecordId_key" ON "event_subscription_deliveries"("subscriptionId", "eventRecordId");
CREATE INDEX IF NOT EXISTS "event_subscription_deliveries_status_availableAt_idx" ON "event_subscription_deliveries"("status", "availableAt");
CREATE INDEX IF NOT EXISTS "event_subscription_deliveries_eventRecordId_createdAt_idx" ON "event_subscription_deliveries"("eventRecordId", "createdAt");
ALTER TABLE "event_subscription_deliveries" ADD CONSTRAINT "event_subscription_deliveries_subscriptionId_fkey" FOREIGN KEY ("subscriptionId") REFERENCES "event_subscriptions"("id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "event_subscription_deliveries" ADD CONSTRAINT "event_subscription_deliveries_eventRecordId_fkey" FOREIGN KEY ("eventRecordId") REFERENCES "event_records"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "event_dead_letters" (
  "id" TEXT NOT NULL,
  "sourceType" TEXT NOT NULL,
  "sourceId" TEXT NOT NULL,
  "reason" TEXT NOT NULL,
  "payload" JSONB NOT NULL,
  "metadata" JSONB,
  "status" TEXT NOT NULL DEFAULT 'open',
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "resolvedAt" TIMESTAMP(3),
  "resolvedBy" TEXT,
  CONSTRAINT "event_dead_letters_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "event_dead_letters_status_createdAt_idx" ON "event_dead_letters"("status", "createdAt");
CREATE INDEX IF NOT EXISTS "event_dead_letters_sourceType_sourceId_idx" ON "event_dead_letters"("sourceType", "sourceId");

ALTER TABLE "bot_runtime_statuses" ALTER COLUMN "version" SET DEFAULT '0.9.0';
