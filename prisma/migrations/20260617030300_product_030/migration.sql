CREATE TABLE "secrets" (
  "id" TEXT NOT NULL,
  "key" TEXT NOT NULL,
  "description" TEXT,
  "algorithm" TEXT NOT NULL DEFAULT 'aes-256-gcm',
  "ciphertext" TEXT NOT NULL,
  "iv" TEXT NOT NULL,
  "authTag" TEXT NOT NULL,
  "version" INTEGER NOT NULL DEFAULT 1,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  "rotatedAt" TIMESTAMP(3),
  CONSTRAINT "secrets_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "secrets_key_key" ON "secrets"("key");
CREATE INDEX "secrets_updatedAt_idx" ON "secrets"("updatedAt");

CREATE TABLE "secret_history" (
  "id" TEXT NOT NULL,
  "secretId" TEXT NOT NULL,
  "version" INTEGER NOT NULL,
  "algorithm" TEXT NOT NULL,
  "ciphertext" TEXT NOT NULL,
  "iv" TEXT NOT NULL,
  "authTag" TEXT NOT NULL,
  "actorId" TEXT,
  "reason" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "secret_history_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "secret_history_secretId_createdAt_idx" ON "secret_history"("secretId", "createdAt");
ALTER TABLE "secret_history" ADD CONSTRAINT "secret_history_secretId_fkey" FOREIGN KEY ("secretId") REFERENCES "secrets"("id") ON DELETE CASCADE ON UPDATE CASCADE;
