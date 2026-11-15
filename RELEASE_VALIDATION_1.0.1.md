# CajeerBots 1.0.1 — Release validation

This patch retains the stable/limited-production policy. Release gates must run on the exact release commit. The signed release includes all six artifacts, including the full RC6-to-Stable rehearsal and an additional verified 1.0.0-to-1.0.1 upgrade/restore phase. The previous 1.0.0 image is obtained from the signed artifact of run 43398376975 and verified independently before use. Do not deploy until CI, security audits and manifest verification pass. The 1.0.0 draft is not an authoritative public release. Use the new signed bundle for pilot deployment; monitor health, capacity, backlog, dead letters and heartbeats before expanding production usage.

## Limited Production operations

Use the signed release bundle and its trusted Ed25519 public key (not a key shipped by an untrusted mirror). Run from the release source directory. `ENV` is an absolute path to the private production environment file; `IMAGE` is the digest-derived local image printed by `pilot:deploy` or `pilot:upgrade`.

```sh
pnpm pilot:deploy PROJECT ENV BUNDLE PUBLIC_KEY 5 10
pnpm pilot:status PROJECT ENV IMAGE
pnpm pilot:pause PROJECT ENV IMAGE
pnpm pilot:resume PROJECT ENV BUNDLE PUBLIC_KEY
```

For an existing 1.0.0 installation use `pnpm pilot:upgrade PROJECT ENV OLD_IMAGE BACKUP BUNDLE PUBLIC_KEY 5 10`. `BACKUP` must be outside the source tree. The capacity row is updated transactionally after migration; existing usage exceeding the requested cap aborts the operation. Resume does not reset counters or seed; it verifies the original signed image and the existing configuration and capacity, then checks the running services. A failed startup gate stops application writers, leaving data for investigation. No automatic destructive rollback is performed. Use `instance:restore` into a fresh project if recovery requires the pre-upgrade backup.

The gate rejects missing capacity, drift, unhealthy services, negative/impossible metrics, dead letters, and stale/latest failed heartbeats. Transient readiness/backlog conditions are retried up to 12 probes, with 5-second pauses. A runtime heartbeat tracks the current stored account binding, Telegram polling state and Discord gateway readiness; it is not a guarantee of remote delivery or webhook reachability. Restart the runtime after changing bot credentials or mode (including rotation within the same secret reference). Keep the deployment at no more than 5 communities and 10 bot accounts until GA acceptance.

Validation: the release still requires the full CI, security audit, signed exact-image rehearsal and real provider checks on the release commit. Local source-level tests do not replace these checks. No release signature, successful Docker rehearsal, or production pilot result is asserted by this source patch.
