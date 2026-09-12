-- CajeerBots 0.12.0: DX devtools, debug sessions and bot scenario runner.
CREATE TABLE "debug_sessions" (
  "id" TEXT NOT NULL,
  "communityId" TEXT,
  "title" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'active',
  "actorId" TEXT,
  "actorType" TEXT,
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  "endedAt" TIMESTAMP(3),
  CONSTRAINT "debug_sessions_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "debug_sessions_communityId_status_createdAt_idx" ON "debug_sessions"("communityId", "status", "createdAt");
CREATE INDEX "debug_sessions_status_updatedAt_idx" ON "debug_sessions"("status", "updatedAt");
ALTER TABLE "debug_sessions" ADD CONSTRAINT "debug_sessions_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE SET NULL ON UPDATE CASCADE;

CREATE TABLE "debug_events" (
  "id" TEXT NOT NULL,
  "sessionId" TEXT,
  "communityId" TEXT,
  "level" TEXT NOT NULL DEFAULT 'info',
  "category" TEXT NOT NULL,
  "message" TEXT NOT NULL,
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "debug_events_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "debug_events_sessionId_createdAt_idx" ON "debug_events"("sessionId", "createdAt");
CREATE INDEX "debug_events_communityId_createdAt_idx" ON "debug_events"("communityId", "createdAt");
CREATE INDEX "debug_events_level_createdAt_idx" ON "debug_events"("level", "createdAt");
CREATE INDEX "debug_events_category_createdAt_idx" ON "debug_events"("category", "createdAt");
ALTER TABLE "debug_events" ADD CONSTRAINT "debug_events_sessionId_fkey" FOREIGN KEY ("sessionId") REFERENCES "debug_sessions"("id") ON DELETE SET NULL ON UPDATE CASCADE;

CREATE TABLE "bot_scenarios" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "provider" "BotProvider",
  "accountId" TEXT,
  "definition" JSONB NOT NULL,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "bot_scenarios_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "bot_scenarios_communityId_code_key" ON "bot_scenarios"("communityId", "code");
CREATE INDEX "bot_scenarios_communityId_isActive_idx" ON "bot_scenarios"("communityId", "isActive");
CREATE INDEX "bot_scenarios_provider_updatedAt_idx" ON "bot_scenarios"("provider", "updatedAt");
ALTER TABLE "bot_scenarios" ADD CONSTRAINT "bot_scenarios_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "bot_scenario_runs" (
  "id" TEXT NOT NULL,
  "scenarioId" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'running',
  "provider" "BotProvider",
  "accountId" TEXT,
  "triggeredBy" TEXT NOT NULL DEFAULT 'api',
  "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "finishedAt" TIMESTAMP(3),
  "durationMs" INTEGER,
  "stepsResult" JSONB,
  "error" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "bot_scenario_runs_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "bot_scenario_runs_scenarioId_createdAt_idx" ON "bot_scenario_runs"("scenarioId", "createdAt");
CREATE INDEX "bot_scenario_runs_communityId_status_createdAt_idx" ON "bot_scenario_runs"("communityId", "status", "createdAt");
ALTER TABLE "bot_scenario_runs" ADD CONSTRAINT "bot_scenario_runs_scenarioId_fkey" FOREIGN KEY ("scenarioId") REFERENCES "bot_scenarios"("id") ON DELETE CASCADE ON UPDATE CASCADE;
