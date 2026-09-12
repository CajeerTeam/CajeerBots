-- CajeerBots 0.6.0 security hardening: admin accounts, sessions, TOTP 2FA, recovery codes and rate-limit telemetry.

CREATE TABLE IF NOT EXISTS "admin_accounts" (
  "id" TEXT NOT NULL,
  "email" TEXT NOT NULL,
  "displayName" TEXT,
  "passwordHash" TEXT NOT NULL,
  "scopes" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "lastLoginAt" TIMESTAMP(3),
  "twoFactorEnabled" BOOLEAN NOT NULL DEFAULT false,
  "twoFactorConfirmedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "admin_accounts_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "admin_accounts_email_key" ON "admin_accounts"("email");
CREATE INDEX IF NOT EXISTS "admin_accounts_isActive_lastLoginAt_idx" ON "admin_accounts"("isActive", "lastLoginAt");

CREATE TABLE IF NOT EXISTS "admin_sessions" (
  "id" TEXT NOT NULL,
  "accountId" TEXT NOT NULL,
  "refreshTokenHash" TEXT NOT NULL,
  "userAgent" TEXT,
  "ip" TEXT,
  "expiresAt" TIMESTAMP(3) NOT NULL,
  "lastUsedAt" TIMESTAMP(3),
  "revokedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "admin_sessions_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "admin_sessions_refreshTokenHash_key" ON "admin_sessions"("refreshTokenHash");
CREATE INDEX IF NOT EXISTS "admin_sessions_accountId_revokedAt_expiresAt_idx" ON "admin_sessions"("accountId", "revokedAt", "expiresAt");
ALTER TABLE "admin_sessions" ADD CONSTRAINT "admin_sessions_accountId_fkey" FOREIGN KEY ("accountId") REFERENCES "admin_accounts"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "two_factor_secrets" (
  "id" TEXT NOT NULL,
  "accountId" TEXT NOT NULL,
  "algorithm" TEXT NOT NULL DEFAULT 'totp-sha1',
  "secret" TEXT NOT NULL,
  "isActive" BOOLEAN NOT NULL DEFAULT false,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "confirmedAt" TIMESTAMP(3),
  "disabledAt" TIMESTAMP(3),
  CONSTRAINT "two_factor_secrets_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "two_factor_secrets_accountId_isActive_idx" ON "two_factor_secrets"("accountId", "isActive");
ALTER TABLE "two_factor_secrets" ADD CONSTRAINT "two_factor_secrets_accountId_fkey" FOREIGN KEY ("accountId") REFERENCES "admin_accounts"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "recovery_codes" (
  "id" TEXT NOT NULL,
  "accountId" TEXT NOT NULL,
  "codeHash" TEXT NOT NULL,
  "usedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "recovery_codes_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "recovery_codes_codeHash_key" ON "recovery_codes"("codeHash");
CREATE INDEX IF NOT EXISTS "recovery_codes_accountId_usedAt_idx" ON "recovery_codes"("accountId", "usedAt");
ALTER TABLE "recovery_codes" ADD CONSTRAINT "recovery_codes_accountId_fkey" FOREIGN KEY ("accountId") REFERENCES "admin_accounts"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "rate_limit_violations" (
  "id" TEXT NOT NULL,
  "bucket" TEXT NOT NULL,
  "identifier" TEXT NOT NULL,
  "route" TEXT,
  "limit" INTEGER NOT NULL,
  "count" INTEGER NOT NULL,
  "windowMs" INTEGER NOT NULL,
  "ip" TEXT,
  "userAgent" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "rate_limit_violations_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "rate_limit_violations_bucket_identifier_createdAt_idx" ON "rate_limit_violations"("bucket", "identifier", "createdAt");

INSERT INTO "permissions" ("id", "scope", "description")
VALUES
  ('perm_auth_accounts_manage', 'auth:accounts:manage', 'Создание admin accounts, смена паролей и управление доступом администраторов'),
  ('perm_auth_sessions_manage', 'auth:sessions:manage', 'Просмотр и отзыв admin sessions'),
  ('perm_auth_2fa_manage', 'auth:2fa:manage', 'Настройка TOTP 2FA и recovery codes'),
  ('perm_security_read', 'security:read', 'Чтение security hardening status и rate-limit violations')
ON CONFLICT ("scope") DO UPDATE SET "description" = EXCLUDED."description";
