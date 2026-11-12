# CajeerBots 1.0.0-rc.4 — Product Completion

## Upgrade

Use the delta archive over the exact rc.3 tree. Back up PostgreSQL, stop all rc.3 processes, generate and commit a real pnpm lockfile, then install, generate Prisma, build, typecheck, lint, migrate and run tests. Start all rc.4 services together. The migration `20261112000400_product_completion` adds executable artifact storage and immutable previous release snapshots. It disables old enabled extensions (they were not verified by this new installation path) and marks previously applied metadata-only update plans/history `legacy_unverified`. Review and re-enable extensions individually; do not run rc.3 binaries against the migrated database.

```sh
pnpm deps:lock
pnpm install --frozen-lockfile
pnpm prisma:validate
pnpm build
pnpm typecheck
pnpm lint
pnpm test
pnpm test:product-completion
pnpm prisma:deploy
pnpm test:e2e
```

## CLI / real bots

Set `CAJEER_API_URL` (HTTPS or local loopback), `CAJEER_API_KEY` or `CAJEER_JWT`. Existing operator-only DB commands (e.g. bot account provisioning, core staging) still require DB access. For a bot account already registered and configured with rc.3 credentials:

```sh
pnpm cajeer interactions form:add --community COMMUNITY_ID --code survey --title Survey --fields examples/product-completion/survey-fields.json
pnpm cajeer interactions callback:add --community COMMUNITY_ID --code survey.open --title Survey --handler form.start --target survey
pnpm cajeer interactions menu:add --community COMMUNITY_ID --code home --title Home --buttons examples/product-completion/home-menu.json
pnpm cajeer interactions command:add --community COMMUNITY_ID --name home --description Home --handler menu.open --code home
pnpm cajeer bot sync-commands ACCOUNT_ID # Telegram or Discord, replaces provider command list; Discord --guild optional
pnpm cajeer interactions forms:submissions --community COMMUNITY_ID
```

Use `/home`, tap Survey, enter answers; `/answer VALUE` (Discord slash command with `value`) and `/cancel` work on all providers. Forms validate each field and submit once per durable inbound update. `/answer /skip` skips an optional field; multiselect takes comma-separated values. Conversation definitions use existing `conversation:add` and `command:add --conversation CODE`; validate numbers/booleans/choices and support next/complete, emit `conversation.completed` to outbox. Session scope includes community, provider, bot, target chat and user; expires in 30 minutes. Buttons carry session nonces, and origin permissions are checked again on each answer. Telegram/VK group conversations are visible in the group; do not collect confidential information there. Discord webhook still uses synchronous initial replies; provider round-trip timing and callback acknowledgments need live testing. Provider registration is explicit; no external command list is overwritten automatically.

## Signed extension lifecycle

A registry package contains a signed JSON envelope `format: cajeer-extension-v1`, `manifest`, `source` (one bundled ESM module exporting default object with `handleEvent`). Bundle external npm dependencies into the single file; relative and bare imports are not supported by the data-URL loader. Node built-ins remain accessible. Code is **trusted host code**, not a sandbox; only approve reviewed publishers. Ed25519 keys belong to operators/publishers. Permissions restrict activation/dispatch, not arbitrary JavaScript system calls. Registry and artifacts must use HTTPS, public IPv4, no redirects; 2 MiB maximum for extensions. The source public key is the trust anchor. No local file URL or unsigned registry installation. Private authenticated registries are not implemented in this patch.

Build an example bundle and release descriptor:

```sh
openssl genpkey -algorithm ED25519 -out private.pem
openssl pkey -in private.pem -pubout -out public.pem
node scripts/package-extension.mjs examples/product-completion/hello.manifest.json examples/product-completion/hello.mjs private.pem hello.json https://YOUR_HOST/hello.json > version.json
```

Publish `hello.json` and an HTTPS registry JSON with `schemaVersion: "1.0"`, name, `packages: [{name:"demo-hello",kind:"plugin",versions:[<contents of version.json>]}]`. Do not publish the private key. Then:

```sh
pnpm cajeer registry sources:add --name trusted --url https://YOUR_HOST/registry.json --public-key-file public.pem
pnpm cajeer registry sources:sync SOURCE_ID
pnpm cajeer registry packages:install demo-hello --source SOURCE_ID
pnpm cajeer extension enable EXTENSION_ID
pnpm cajeer extension event:emit interaction.handled --extension EXTENSION_ID --payload '{}'
pnpm cajeer extension events:list --extension EXTENSION_ID
pnpm cajeer extension storage:list EXTENSION_ID --namespace demo
```

For another signed version (e.g. 1.0.1), sync registry, plan and review dry-run, disable extension and wait for in-flight handlers, apply and enable. Same extension ID, config history, and storage survive version replacement. New permissions require prior grant or explicitly authorized `--auto-grant`. Pending events prevent update; use `--discard-pending` only after reviewing the consequences (pending/failed rows move to dead with reason, processing rows still block). Rollback restores prior executable bytes and manifest and removes no-longer-declared grants, but does not undo business effects/storage writes or restore revoked permissions. Legacy manifest-only installations may be reinstalled from a signed registry under the same name/version after disabling. Local operator-managed code requires `EXTENSIONS_ALLOW_LOCAL=true` on API and worker, a real file under `EXTENSIONS_ROOT`, and explicit enable; no arbitrary paths.

```sh
pnpm cajeer update plan-extension EXTENSION_ID
pnpm cajeer update apply PLAN_ID --dry-run
pnpm cajeer extension disable EXTENSION_ID
pnpm cajeer update apply PLAN_ID
pnpm cajeer extension enable EXTENSION_ID
# rollback: disable, wait/drain, rollback, review grants, enable
pnpm cajeer extension disable EXTENSION_ID
pnpm cajeer update rollback PLAN_ID
```

Core `update check` creates a plan; `update stage-core PLAN_ID --output release.bin` downloads and verifies a signed artifact (max 64 MiB) using host operator DB access and writes a new file. It does not unpack, run migrations, restart services or mark deployed. Core deployment remains an explicit host/CI process; API apply refuses to fake success. Extension code is not process-isolated; handler timeouts/resource isolation and crash-recovery rehearsal remain RC5/RC6 tasks.

## Validation evidence

- 10 standalone Node regression tests passed using actual transpiled implementation: form state, typed answers, deduplication, user/chat isolation, permissions, menu callback, Ed25519 verification, executable integrity, SDK HTTP rollback. Bot persistence tests use an in-memory transaction double, **not PostgreSQL**.
- TypeScript parser check passed for all source files; not a dependency-aware typecheck.
- Actual npm registry DNS resolution failed in the build environment. Source has no pnpm-lock.yaml or installed packages/Prisma generated client. Full build, typecheck, lint, PostgreSQL migrations, Redis, E2E and live Telegram/VK/Discord were not run; do not claim stable production readiness before those pass.
- No files deleted. Apply the archive as a delta.
