-- Upgrade the four stock maintenance jobs without reseeding operator data.
-- Custom jobs, custom payloads and explicit non-stock version targets are left intact.
UPDATE "scheduled_jobs" AS j
SET "payload" = jsonb_set(j."payload", '{version}', '"1.0.2"'::jsonb),
    "updatedAt" = CURRENT_TIMESTAMP
FROM (VALUES
  ('maintenance.release_gates', '{"channel":"stable","rootDir":"."}'::jsonb),
  ('maintenance.integration_smoke', '{"suiteCode":"mvp-smoke","channel":"stable","dryRun":true}'::jsonb),
  ('maintenance.security_audit', '{"channel":"stable","persist":true}'::jsonb),
  ('maintenance.rc_readiness', '{"channel":"stable","rootDir":".","persist":true}'::jsonb)
) AS stock(code, payload_without_version)
WHERE j."code" = stock.code
  AND j."handler" = stock.code
  AND j."type" = 'built_in'
  AND j."payload"->>'version' IN ('1.0.0-rc.6', '1.0.0', '1.0.1')
  AND (j."payload" - 'version') = stock.payload_without_version;
