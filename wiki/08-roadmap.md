# Roadmap CajeerBots

Текущая версия: `0.11.0`.

## Реализовано

### 0.2.0 — Product API Core
- JWT/API keys/scopes.
- Users/Roles/Permissions API.
- Bot Accounts API и CLI.
- Message Templates API и render.
- Outbox API и webhook delivery records.

### 0.3.0 — Secrets, Config, Import/Export
- Encrypted secrets store.
- tokenRef `secret:<key>` / `env:<NAME>`.
- Runtime config validation.
- Community import/export с dry-run/apply.

### 0.4.0 — Interaction Core
- DB-backed commands, menus, callback actions и conversation flows.
- Runtime dispatcher для Telegram/VK/Discord.
- Interaction events journal.

### 0.5.0 — Extension Runtime
- Manifest install + compatibility check.
- Permission grants/revoke.
- Runtime event queue и worker execution.
- Extension storage, config history и runtime logs.

### 0.6.0 — Security Hardening
- Admin accounts, password login, refresh sessions.
- TOTP 2FA и recovery codes.
- Session revoke, password change, security hardening endpoints.

### 0.7.0 — Bot Runtime MVP
- Runtime heartbeat/status.
- Inbound/outbound bot journals.
- Webhook-mode HTTP server.
- Provider senders и retry исходящих сообщений.

### 0.8.0 — Message Core
- Locale catalogs.
- Provider-aware button layouts.
- Forms/submissions.
- Preview/render и render logs.

### 0.9.0 — Event API
- Durable event journal.
- Topics/subscriptions.
- Replay/retry deliveries.
- Dead-letter queue.
- Delivery targets: outbox, webhook endpoint, extension hook, log.

### 0.10.0 — Registry & Updates
- Registry sources для GitFlic Registry/Releases manifest.
- Registry package/version catalog.
- SemVer и Engine API compatibility planner.
- Установка modules/plugins из registry.
- Update plans и update history.
- API/CLI/SDK для registry/update flow.

## Следующие версии

### 0.11.0 — Backup & Restore
- Full backup archive.
- Restore workflow.
- Extension configs backup.
- Backup/restore guide для Docker Compose.

### 0.12.0 — Production Observability
- Расширенные Prometheus metrics.
- OpenTelemetry spans для API/worker/bot-runtime.
- Queue/cache/storage diagnostics.
- Audit completeness checks.

### 0.14.0 — Contract & SDK Hardening
- Contract tests для OpenAPI/SDK.
- SDK generation/check workflow.
- Backward compatibility policy.

### 0.14.0 — Docker Compose Production Profile
- `docker-compose.prod.yml`.
- migration job.
- backup job.
- non-root runtime images.

### 0.15.0-beta.1 — Feature Freeze
- Только багфиксы, тесты, docs и hardening.

### 1.0.0 — Stable Product Release
- API, bot-runtime, worker и CLI стабильно запускаются через Docker Compose.
- Telegram/VK/Discord имеют рабочие интерфейсы управления.
- Security/API/Bot Runtime/Extension Runtime/Event API/Registry стабилизированы.
- Документация на русском закрывает установку, настройку, безопасность, обновления и восстановление.


### 0.11.0 — Job Scheduler & Maintenance

Готово: DB-backed scheduled jobs, run history, maintenance snapshots, scheduler worker, manual run, default cleanup/retention/diagnostics jobs, CLI/API/SDK.
