-- Older upgrade-plan runners recorded simulated steps as passed.
UPDATE release_upgrade_runs SET status = 'legacy_unverified' WHERE status = 'passed';

-- Unsafe historical references must not be sent to a provider or block all bots at startup.
-- Operators can rebind these disabled accounts to a namespaced secret.
UPDATE "bot_accounts"
SET "isActive" = false, "tokenRef" = NULL, "updatedAt" = CURRENT_TIMESTAMP
WHERE "tokenRef" IS NOT NULL AND NOT (
  length("tokenRef") <= 400 AND (
    "tokenRef" = 'env:' || upper("provider"::text) || '_BOT_TOKEN'
    OR "tokenRef" ~ ('^env:' || upper("provider"::text) || '_BOT_TOKEN_[A-Z0-9_]+$')
    OR ("tokenRef" ~ '^secret:bot\.[A-Za-z0-9_.:-]+$'
        AND starts_with("tokenRef", 'secret:bot.' || "communityId" || '.' || "provider"::text || '.')
        AND length("tokenRef") > length('secret:bot.' || "communityId" || '.' || "provider"::text || '.'))
  )
);
