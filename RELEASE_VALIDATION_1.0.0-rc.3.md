# 1.0.0-rc.3 — Security & Reliability

## Upgrade

1. Stop all workers, bot runtimes and API before upgrading; back up PostgreSQL and configuration.
2. Apply the changed-file archive at repository root. RC2 did not contain a pnpm lockfile. On a registry-connected host with Node 22.16+ and pnpm 9.15.9 run `pnpm deps:lock`, commit the generated `pnpm-lock.yaml`, then `pnpm install --frozen-lockfile`.
3. Run `pnpm prisma:validate`, `pnpm build`, `pnpm typecheck`, `pnpm lint`, `pnpm test`, `pnpm test:stabilization`, `node --test scripts/rc3-security.test.mjs`. In staging with PostgreSQL and Redis run `pnpm prisma:deploy`, `pnpm prisma:seed`, `pnpm test:e2e`, `pnpm smoke:runtime` with services started. Verify real provider credentials and TLS before deployment.
4. Tenant API keys/admin accounts need `community:<id>` plus action scopes. A wildcard with a tenant grant is restricted to that tenant. Platform-wide limited scopes require `platform:global`; existing `*` without tenant grants remains global. Existing user JWTs without session IDs must refresh or sign in again. Global resource APIs and shared backup/import/export are platform-only.
5. Configure `BOT_WEBHOOK_CREDENTIALS` JSON per exact bot account ID, e.g. `{"telegram-account-id":{"secret":"a-long-random-secret"},"vk-account-id":{"secret":"another-random-secret","groupId":"123","confirmation":"provider-confirmation"},"discord-account-id":{"publicKey":"64-hex-characters"}}`. Replace all examples with actual provider values; Telegram/VK secrets must be at least 16 characters. Configure matching secrets at providers, TLS at ingress, and restart runtime after changing accounts/credentials. Keep secrets out of logs and source control. Production bot credentials are references (`env:NAME` or `secret:NAME`), not literal tokens. Polling remains supported.
6. Explicitly set `communityId` for tenant webhook endpoints and `filters.communityId` for tenant event subscriptions. Unscoped endpoints/subscriptions receive only global events. Migration backfills known ownership and quarantines mismatched historical webhook deliveries and legacy Discord channel retries. Review dead-letter records before replay. Never manually reset delivered messages.

## Validation status

- Five executable regression tests passed using transpiled actual source modules: community access, JWT validation, SSRF restrictions, provider authentication, tenant command/event filters.
- PostgreSQL E2E tests added for tenant API isolation and concurrent/stale event delivery claims. Not executed here.
- Full pnpm build, Prisma generation/migration, integration tests, real provider delivery and Compose deployment are **not verified**: the supplied RC2 archive has no `pnpm-lock.yaml`; dependency registry DNS failed in the preparation environment. No fabricated lockfile or successful CI claim. Generate the actual lockfile and run the commands above before release.

## Operational semantics

- External delivery is at-least-once, not exactly-once: a crash between provider acknowledgement and database commit can cause redelivery. Consumers must deduplicate stable event/delivery IDs. Workers do not retry already acknowledged rows. Stale claims are recovered and completion writes fenced by their claim.
- Discord interaction replies are inline and private by default; no conversion to ordinary channel messages on retry. A provider acknowledgement deadline can still expire. Telegram/VK inbound retries depend on provider redelivery; there is no independent inbound replay daemon.
- Webhook transport intentionally permits only public IPv4 destinations and standard HTTP(S) ports, blocks redirects, and enforces DNS/connect/request/response limits. Internal or IPv6-only webhooks need a separately reviewed egress policy. Native extensions remain trusted in-process platform code; no hostile-code sandbox is claimed. Stuck extension execution may need operator recovery after workers are stopped.
- Redis rate limiting fails closed. Bot/webhook secrets and signing keys must be independently provisioned; no insecure development fallback is enabled in production.
