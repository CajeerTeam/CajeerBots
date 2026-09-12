-- CajeerBots 0.17.0: GitFlic Wiki documentation operations and knowledge-base runtime
CREATE TABLE IF NOT EXISTS "documentation_collections" (
  "id" TEXT PRIMARY KEY,
  "code" TEXT NOT NULL UNIQUE,
  "title" TEXT NOT NULL,
  "locale" TEXT NOT NULL DEFAULT 'ru',
  "sourceType" TEXT NOT NULL DEFAULT 'local_wiki',
  "sourcePath" TEXT NOT NULL DEFAULT 'wiki',
  "sourceUrl" TEXT,
  "isActive" BOOLEAN NOT NULL DEFAULT true,
  "version" INTEGER NOT NULL DEFAULT 1,
  "metadata" JSONB,
  "lastSyncedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS "documentation_collections_sourceType_isActive_idx" ON "documentation_collections"("sourceType", "isActive");
CREATE INDEX IF NOT EXISTS "documentation_collections_locale_isActive_idx" ON "documentation_collections"("locale", "isActive");

CREATE TABLE IF NOT EXISTS "documentation_pages" (
  "id" TEXT PRIMARY KEY,
  "collectionId" TEXT NOT NULL,
  "slug" TEXT NOT NULL,
  "path" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "locale" TEXT NOT NULL DEFAULT 'ru',
  "status" TEXT NOT NULL DEFAULT 'published',
  "body" TEXT NOT NULL,
  "excerpt" TEXT,
  "headings" JSONB,
  "links" JSONB,
  "frontmatter" JSONB,
  "tags" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  "wordCount" INTEGER NOT NULL DEFAULT 0,
  "sizeBytes" INTEGER NOT NULL DEFAULT 0,
  "sha256" TEXT NOT NULL,
  "sourceMtime" TIMESTAMP(3),
  "syncedAt" TIMESTAMP(3),
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "documentation_pages_collectionId_fkey" FOREIGN KEY ("collectionId") REFERENCES "documentation_collections"("id") ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS "documentation_pages_collectionId_slug_key" ON "documentation_pages"("collectionId", "slug");
CREATE INDEX IF NOT EXISTS "documentation_pages_collectionId_path_idx" ON "documentation_pages"("collectionId", "path");
CREATE INDEX IF NOT EXISTS "documentation_pages_collectionId_status_updatedAt_idx" ON "documentation_pages"("collectionId", "status", "updatedAt");
CREATE INDEX IF NOT EXISTS "documentation_pages_locale_status_idx" ON "documentation_pages"("locale", "status");
CREATE INDEX IF NOT EXISTS "documentation_pages_sha256_idx" ON "documentation_pages"("sha256");

CREATE TABLE IF NOT EXISTS "documentation_snapshots" (
  "id" TEXT PRIMARY KEY,
  "collectionId" TEXT NOT NULL,
  "version" INTEGER NOT NULL,
  "sourceHash" TEXT NOT NULL,
  "pageCount" INTEGER NOT NULL DEFAULT 0,
  "totalWords" INTEGER NOT NULL DEFAULT 0,
  "totalBytes" INTEGER NOT NULL DEFAULT 0,
  "toc" JSONB,
  "metadata" JSONB,
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "documentation_snapshots_collectionId_fkey" FOREIGN KEY ("collectionId") REFERENCES "documentation_collections"("id") ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS "documentation_snapshots_collectionId_version_idx" ON "documentation_snapshots"("collectionId", "version");
CREATE INDEX IF NOT EXISTS "documentation_snapshots_sourceHash_idx" ON "documentation_snapshots"("sourceHash");

CREATE TABLE IF NOT EXISTS "documentation_checks" (
  "id" TEXT PRIMARY KEY,
  "collectionId" TEXT,
  "checkType" TEXT NOT NULL DEFAULT 'local_wiki',
  "status" TEXT NOT NULL DEFAULT 'passed',
  "summary" JSONB,
  "issues" JSONB,
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "documentation_checks_collectionId_fkey" FOREIGN KEY ("collectionId") REFERENCES "documentation_collections"("id") ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS "documentation_checks_collectionId_createdAt_idx" ON "documentation_checks"("collectionId", "createdAt");
CREATE INDEX IF NOT EXISTS "documentation_checks_status_checkType_createdAt_idx" ON "documentation_checks"("status", "checkType", "createdAt");

CREATE TABLE IF NOT EXISTS "documentation_exports" (
  "id" TEXT PRIMARY KEY,
  "collectionId" TEXT,
  "format" TEXT NOT NULL DEFAULT 'json_bundle',
  "status" TEXT NOT NULL DEFAULT 'created',
  "checksum" TEXT NOT NULL,
  "sizeBytes" INTEGER NOT NULL DEFAULT 0,
  "pageCount" INTEGER NOT NULL DEFAULT 0,
  "payload" JSONB,
  "storageObjectId" TEXT,
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "documentation_exports_collectionId_fkey" FOREIGN KEY ("collectionId") REFERENCES "documentation_collections"("id") ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE INDEX IF NOT EXISTS "documentation_exports_collectionId_createdAt_idx" ON "documentation_exports"("collectionId", "createdAt");
CREATE INDEX IF NOT EXISTS "documentation_exports_format_status_createdAt_idx" ON "documentation_exports"("format", "status", "createdAt");
CREATE INDEX IF NOT EXISTS "documentation_exports_checksum_idx" ON "documentation_exports"("checksum");
