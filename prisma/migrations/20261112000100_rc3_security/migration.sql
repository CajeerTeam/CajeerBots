ALTER TABLE "outbox_messages" ADD COLUMN "communityId" TEXT;
ALTER TABLE "webhook_endpoints" ADD COLUMN "communityId" TEXT;
UPDATE "outbox_messages" o SET "communityId" = e."communityId" FROM "event_records" e WHERE e."outboxMessageId" = o.id AND e."communityId" IS NOT NULL;
UPDATE "outbox_messages" SET "communityId" = COALESCE(payload->'metadata'->>'communityId', payload->'payload'->'metadata'->>'communityId') WHERE "communityId" IS NULL;
-- Prevent already materialized cross-community deliveries from being retried.
UPDATE "webhook_deliveries" d SET status = 'dead', "lastError" = 'Community scope mismatch after rc3 upgrade' FROM "outbox_messages" o, "webhook_endpoints" w WHERE d."outboxMessageId" = o.id AND d."endpointId" = w.id AND o."communityId" IS DISTINCT FROM w."communityId" AND d.status <> 'delivered';
UPDATE "bot_outbound_messages" SET status = 'dead', "lastError" = 'Legacy Discord interaction cannot be replayed safely' WHERE provider = 'discord' AND status IN ('pending', 'sending', 'failed');
