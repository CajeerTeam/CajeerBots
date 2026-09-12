-- CajeerBots 0.8.0: Message Core production layer

CREATE TABLE IF NOT EXISTS "locale_catalogs" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "namespace" TEXT NOT NULL DEFAULT 'default',
  "locale" TEXT NOT NULL,
  "fallbackLocale" TEXT,
  "messages" JSONB NOT NULL,
  "version" INTEGER NOT NULL DEFAULT 1,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "locale_catalogs_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "locale_catalogs_communityId_namespace_locale_key" ON "locale_catalogs" ("communityId", "namespace", "locale");
CREATE INDEX IF NOT EXISTS "locale_catalogs_communityId_locale_idx" ON "locale_catalogs" ("communityId", "locale");
ALTER TABLE "locale_catalogs" ADD CONSTRAINT "locale_catalogs_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "message_button_layouts" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "provider" "BotProvider",
  "locale" TEXT NOT NULL DEFAULT 'ru',
  "rows" JSONB NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "message_button_layouts_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "message_button_layouts_communityId_code_provider_locale_key" ON "message_button_layouts" ("communityId", "code", "provider", "locale");
CREATE INDEX IF NOT EXISTS "message_button_layouts_communityId_code_idx" ON "message_button_layouts" ("communityId", "code");
ALTER TABLE "message_button_layouts" ADD CONSTRAINT "message_button_layouts_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "message_form_definitions" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "locale" TEXT NOT NULL DEFAULT 'ru',
  "fields" JSONB NOT NULL,
  "submitTemplateKey" TEXT,
  "successTemplateKey" TEXT,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "message_form_definitions_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "message_form_definitions_communityId_code_locale_key" ON "message_form_definitions" ("communityId", "code", "locale");
CREATE INDEX IF NOT EXISTS "message_form_definitions_communityId_isActive_idx" ON "message_form_definitions" ("communityId", "isActive");
ALTER TABLE "message_form_definitions" ADD CONSTRAINT "message_form_definitions_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "message_form_submissions" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "formId" TEXT NOT NULL,
  "provider" "BotProvider",
  "userExternalId" TEXT,
  "payload" JSONB NOT NULL,
  "normalized" JSONB,
  "errors" JSONB,
  "status" TEXT NOT NULL DEFAULT 'accepted',
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "message_form_submissions_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "message_form_submissions_communityId_createdAt_idx" ON "message_form_submissions" ("communityId", "createdAt");
CREATE INDEX IF NOT EXISTS "message_form_submissions_formId_status_createdAt_idx" ON "message_form_submissions" ("formId", "status", "createdAt");
ALTER TABLE "message_form_submissions" ADD CONSTRAINT "message_form_submissions_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "message_form_submissions" ADD CONSTRAINT "message_form_submissions_formId_fkey" FOREIGN KEY ("formId") REFERENCES "message_form_definitions"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "message_render_logs" (
  "id" TEXT NOT NULL,
  "communityId" TEXT,
  "templateKey" TEXT,
  "locale" TEXT NOT NULL,
  "provider" "BotProvider",
  "variables" JSONB,
  "text" TEXT NOT NULL,
  "buttons" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "message_render_logs_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "message_render_logs_communityId_createdAt_idx" ON "message_render_logs" ("communityId", "createdAt");
CREATE INDEX IF NOT EXISTS "message_render_logs_templateKey_locale_idx" ON "message_render_logs" ("templateKey", "locale");
ALTER TABLE "message_render_logs" ADD CONSTRAINT "message_render_logs_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE SET NULL ON UPDATE CASCADE;
