CREATE TYPE "BotProvider" AS ENUM ('telegram', 'vk', 'discord');
CREATE TYPE "BotMode" AS ENUM ('polling', 'webhook');
CREATE TYPE "ExtensionStatus" AS ENUM ('installed', 'enabled', 'disabled', 'failed');
CREATE TYPE "OutboxStatus" AS ENUM ('pending', 'processing', 'delivered', 'failed', 'dead');

CREATE TABLE "communities" (
  "id" TEXT NOT NULL,
  "slug" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "language" TEXT NOT NULL DEFAULT 'ru',
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "communities_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "communities_slug_key" ON "communities"("slug");

CREATE TABLE "bot_accounts" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "provider" "BotProvider" NOT NULL,
  "mode" "BotMode" NOT NULL DEFAULT 'polling',
  "externalId" TEXT,
  "username" TEXT,
  "tokenRef" TEXT,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "bot_accounts_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "bot_accounts_communityId_provider_externalId_key" ON "bot_accounts"("communityId", "provider", "externalId");
ALTER TABLE "bot_accounts" ADD CONSTRAINT "bot_accounts_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "users" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "externalId" TEXT NOT NULL,
  "provider" "BotProvider" NOT NULL,
  "username" TEXT,
  "displayName" TEXT,
  "locale" TEXT NOT NULL DEFAULT 'ru',
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "users_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "users_communityId_provider_externalId_key" ON "users"("communityId", "provider", "externalId");
ALTER TABLE "users" ADD CONSTRAINT "users_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "roles" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "priority" INTEGER NOT NULL DEFAULT 100,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "roles_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "roles_communityId_code_key" ON "roles"("communityId", "code");
ALTER TABLE "roles" ADD CONSTRAINT "roles_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "permissions" (
  "id" TEXT NOT NULL,
  "scope" TEXT NOT NULL,
  "description" TEXT,
  CONSTRAINT "permissions_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "permissions_scope_key" ON "permissions"("scope");

CREATE TABLE "role_permissions" (
  "roleId" TEXT NOT NULL,
  "permissionId" TEXT NOT NULL,
  CONSTRAINT "role_permissions_pkey" PRIMARY KEY ("roleId", "permissionId")
);
ALTER TABLE "role_permissions" ADD CONSTRAINT "role_permissions_roleId_fkey" FOREIGN KEY ("roleId") REFERENCES "roles"("id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "role_permissions" ADD CONSTRAINT "role_permissions_permissionId_fkey" FOREIGN KEY ("permissionId") REFERENCES "permissions"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "user_roles" (
  "userId" TEXT NOT NULL,
  "roleId" TEXT NOT NULL,
  CONSTRAINT "user_roles_pkey" PRIMARY KEY ("userId", "roleId")
);
ALTER TABLE "user_roles" ADD CONSTRAINT "user_roles_userId_fkey" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE "user_roles" ADD CONSTRAINT "user_roles_roleId_fkey" FOREIGN KEY ("roleId") REFERENCES "roles"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "api_keys" (
  "id" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "keyHash" TEXT NOT NULL,
  "scopes" TEXT[] NOT NULL,
  "expiresAt" TIMESTAMP(3),
  "lastUsedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "revokedAt" TIMESTAMP(3),
  CONSTRAINT "api_keys_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "api_keys_keyHash_key" ON "api_keys"("keyHash");

CREATE TABLE "settings" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "key" TEXT NOT NULL,
  "value" JSONB NOT NULL,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "settings_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "settings_communityId_key_key" ON "settings"("communityId", "key");
ALTER TABLE "settings" ADD CONSTRAINT "settings_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "setting_history" (
  "id" TEXT NOT NULL,
  "settingId" TEXT NOT NULL,
  "oldValue" JSONB,
  "newValue" JSONB NOT NULL,
  "actorId" TEXT,
  "reason" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "setting_history_pkey" PRIMARY KEY ("id")
);
ALTER TABLE "setting_history" ADD CONSTRAINT "setting_history_settingId_fkey" FOREIGN KEY ("settingId") REFERENCES "settings"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "extensions" (
  "id" TEXT NOT NULL,
  "name" TEXT NOT NULL,
  "version" TEXT NOT NULL,
  "engineRange" TEXT NOT NULL,
  "status" "ExtensionStatus" NOT NULL DEFAULT 'installed',
  "manifest" JSONB NOT NULL,
  "signature" TEXT,
  "installedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "extensions_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "extensions_name_version_key" ON "extensions"("name", "version");

CREATE TABLE "extension_config_history" (
  "id" TEXT NOT NULL,
  "extensionId" TEXT NOT NULL,
  "oldValue" JSONB,
  "newValue" JSONB NOT NULL,
  "actorId" TEXT,
  "reason" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "extension_config_history_pkey" PRIMARY KEY ("id")
);
ALTER TABLE "extension_config_history" ADD CONSTRAINT "extension_config_history_extensionId_fkey" FOREIGN KEY ("extensionId") REFERENCES "extensions"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "audit_logs" (
  "id" TEXT NOT NULL,
  "actorId" TEXT,
  "actorType" TEXT NOT NULL,
  "action" TEXT NOT NULL,
  "resource" TEXT NOT NULL,
  "resourceId" TEXT,
  "ip" TEXT,
  "userAgent" TEXT,
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "audit_logs_pkey" PRIMARY KEY ("id")
);
CREATE INDEX "audit_logs_action_createdAt_idx" ON "audit_logs"("action", "createdAt");

CREATE TABLE "idempotency_keys" (
  "id" TEXT NOT NULL,
  "key" TEXT NOT NULL,
  "scope" TEXT NOT NULL,
  "requestHash" TEXT NOT NULL,
  "responseBody" JSONB,
  "expiresAt" TIMESTAMP(3) NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "idempotency_keys_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "idempotency_keys_key_key" ON "idempotency_keys"("key");
CREATE INDEX "idempotency_keys_scope_expiresAt_idx" ON "idempotency_keys"("scope", "expiresAt");

CREATE TABLE "outbox_messages" (
  "id" TEXT NOT NULL,
  "topic" TEXT NOT NULL,
  "payload" JSONB NOT NULL,
  "headers" JSONB,
  "status" "OutboxStatus" NOT NULL DEFAULT 'pending',
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "availableAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "deliveredAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "outbox_messages_pkey" PRIMARY KEY ("id")
);
CREATE INDEX "outbox_messages_status_availableAt_idx" ON "outbox_messages"("status", "availableAt");

CREATE TABLE "webhook_endpoints" (
  "id" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "url" TEXT NOT NULL,
  "events" TEXT[] NOT NULL,
  "secretRef" TEXT,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "webhook_endpoints_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "message_templates" (
  "id" TEXT NOT NULL,
  "key" TEXT NOT NULL,
  "locale" TEXT NOT NULL DEFAULT 'ru',
  "body" TEXT NOT NULL,
  "variables" JSONB,
  "buttons" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "message_templates_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "message_templates_key_key" ON "message_templates"("key");

CREATE TABLE "conversation_states" (
  "id" TEXT NOT NULL,
  "communityId" TEXT NOT NULL,
  "provider" "BotProvider" NOT NULL,
  "userId" TEXT NOT NULL,
  "flow" TEXT NOT NULL,
  "step" TEXT NOT NULL,
  "data" JSONB NOT NULL,
  "expiresAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "conversation_states_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "conversation_states_communityId_provider_userId_flow_key" ON "conversation_states"("communityId", "provider", "userId", "flow");
