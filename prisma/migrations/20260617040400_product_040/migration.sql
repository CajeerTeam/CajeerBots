CREATE TABLE "interaction_commands" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "description" TEXT NOT NULL,
  "provider" "BotProvider",
  "type" TEXT NOT NULL DEFAULT 'command',
  "aliases" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  "scopes" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  "handler" TEXT NOT NULL DEFAULT 'template',
  "templateKey" TEXT,
  "conversationCode" TEXT,
  "cooldownSeconds" INTEGER NOT NULL DEFAULT 0,
  "isEnabled" BOOLEAN NOT NULL DEFAULT true,
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "interaction_commands_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "interaction_commands_communityId_provider_name_key" ON "interaction_commands"("communityId", "provider", "name");
CREATE INDEX "interaction_commands_communityId_isEnabled_idx" ON "interaction_commands"("communityId", "isEnabled");
ALTER TABLE "interaction_commands" ADD CONSTRAINT "interaction_commands_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "interaction_menus" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "locale" TEXT NOT NULL DEFAULT 'ru',
  "templateKey" TEXT,
  "buttons" JSONB NOT NULL,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "interaction_menus_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "interaction_menus_communityId_code_locale_key" ON "interaction_menus"("communityId", "code", "locale");
CREATE INDEX "interaction_menus_communityId_isActive_idx" ON "interaction_menus"("communityId", "isActive");
ALTER TABLE "interaction_menus" ADD CONSTRAINT "interaction_menus_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "interaction_callback_actions" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "handler" TEXT NOT NULL DEFAULT 'menu.open',
  "scopes" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  "payload" JSONB,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "interaction_callback_actions_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "interaction_callback_actions_communityId_code_key" ON "interaction_callback_actions"("communityId", "code");
CREATE INDEX "interaction_callback_actions_communityId_isActive_idx" ON "interaction_callback_actions"("communityId", "isActive");
ALTER TABLE "interaction_callback_actions" ADD CONSTRAINT "interaction_callback_actions_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "conversation_flows" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "entryCommand" TEXT,
  "steps" JSONB NOT NULL,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "conversation_flows_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "conversation_flows_communityId_code_key" ON "conversation_flows"("communityId", "code");
CREATE INDEX "conversation_flows_communityId_isActive_idx" ON "conversation_flows"("communityId", "isActive");
ALTER TABLE "conversation_flows" ADD CONSTRAINT "conversation_flows_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "interaction_events" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "accountId" TEXT,
  "provider" "BotProvider" NOT NULL,
  "type" TEXT NOT NULL,
  "name" TEXT,
  "userExternalId" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'received',
  "payload" JSONB,
  "response" JSONB,
  "error" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "interaction_events_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "interaction_events_communityId_createdAt_idx" ON "interaction_events"("communityId", "createdAt");
CREATE INDEX "interaction_events_provider_type_name_idx" ON "interaction_events"("provider", "type", "name");
ALTER TABLE "interaction_events" ADD CONSTRAINT "interaction_events_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;
