ALTER TABLE "extensions" ADD COLUMN "artifactSource" TEXT, ADD COLUMN "artifactCodeSha256" TEXT;
CREATE TABLE "extension_releases" (
 "id" TEXT NOT NULL PRIMARY KEY, "extensionId" TEXT NOT NULL REFERENCES "extensions"("id") ON DELETE CASCADE,
 "version" TEXT NOT NULL, "manifest" JSONB NOT NULL, "signature" TEXT, "registryVersionId" TEXT,
 "artifactSource" TEXT NOT NULL, "artifactCodeSha256" TEXT NOT NULL, "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX "extension_releases_extensionId_version_key" ON "extension_releases"("extensionId", "version");

-- RC3 apply only recorded metadata; do not present it as a verified deployment.
UPDATE "update_plans" SET "status" = 'legacy_unverified' WHERE "status" = 'applied';
UPDATE "update_history" SET "status" = 'legacy_unverified' WHERE "status" = 'applied';
UPDATE "extensions" SET "status" = 'disabled' WHERE "status" = 'enabled';
