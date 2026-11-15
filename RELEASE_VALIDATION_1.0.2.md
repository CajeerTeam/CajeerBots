# CajeerBots 1.0.2 — Stable Core Acceptance

Release gates are mandatory on the exact release commit. This source patch is not a signed release or a claim of production acceptance. It retains the 5-community / 10-bot pilot limit and existing schema. A data-only migration advances stock maintenance-job targets to 1.0.2, preserving custom payloads and job settings.

## Required validation

Run `pnpm install --frozen-lockfile`, `pnpm prisma:validate`, `pnpm build`, `pnpm typecheck`, `pnpm lint`, `pnpm prisma:deploy`, `pnpm prisma:seed`, `pnpm test`, `pnpm test:stable`, `pnpm test:core-acceptance`, `pnpm test:e2e` and the existing CI suite on disposable PostgreSQL/Redis. The CI workflow also runs runtime smoke and Compose startup.

Production rehearsal requires RC6, the signed 1.0.0 image and a separate image built from the supplied 1.0.1 source commit `b535b328bb633b1a495697fa3c15e171375b31ea`. The 1.0.1 source baseline is not represented as a signed published artifact. Historical 1.0.0 must match the pinned public key, commit and image ID; workflows prefer release assets and fall back to the pinned Actions artifact if available. Missing or invalid baseline data blocks the release.

```sh
CAJEER_REHEARSAL_ISOLATED=1 REHEARSAL_MIN_RPS=10 pnpm rehearsal:production RC6_IMAGE TARGET_IMAGE report.json 1000 16 VERIFIED_100_IMAGE PREVIOUS_101_IMAGE
```

The 1.0.2 gate requires all earlier phases plus `previous-patch-upgrade-restore`: actual persisted community, Redis and volume sentinels, event delivery, pre-upgrade backup, upgrade, restore to a fresh 1.0.1 instance and fresh writes/delivery after upgrading. The phase also checks that all four stock maintenance jobs target 1.0.2 with settings preserved and that restoring the backup restores the old job targets. Reports must bind the tested image to the target commit. No phase is optional for signing. Provider tests using fixtures do not replace live Telegram/VK/Discord checks.

## Pilot operations

Use a verified signed bundle and independently trusted key. Keep the existing private environment file unchanged during upgrade; back up outside the source tree. Never use disposable rehearsal against production.

```sh
pnpm pilot:upgrade PROJECT ENV OLD_IMAGE BACKUP BUNDLE PUBLIC_KEY 5 10
pnpm pilot:status PROJECT ENV IMAGE
pnpm pilot:pause PROJECT ENV IMAGE
pnpm pilot:resume PROJECT ENV BUNDLE PUBLIC_KEY
```

On failure, application writers remain stopped. Preserve the backup and investigate; restore into a fresh project and use the original image if rollback is necessary. Do not delete volumes or reuse the failed target as a recovery baseline. See `RELEASE_VALIDATION_1.0.1.md` for prior pilot procedures. GA requires a separate production pilot and live provider verification.
