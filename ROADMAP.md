
## 1.0.0-rc.1 — Security Beta

Статус: выполнено в patch-архиве `1.0.0-rc.1`.

- Добавлен `@cajeerbots/security-audit`.
- Добавлены Prisma-модели `SecurityAuditPolicy`, `SecurityAuditRun`, `SecurityAuditFinding`, `SecurityRiskSnapshot`.
- Добавлены API/CLI/SDK для security audit.
- Добавлен scheduled handler `maintenance.security_audit`.
- Цель версии: закрыть security beta перед `1.0.0-rc.1`, не добавляя новые крупные продуктовые функции.


## 0.18.0-beta.1 — MVP feature freeze / release gates

Статус: реализовано в этом patch.

- Feature-freeze для beta-ветки.
- DB-backed release freezes, quality gates, gate runs, blockers, release candidate reports.
- CLI/API/SDK для запуска beta readiness checks.
- Scheduled job `maintenance.release_gates`.
- Дальше до RC допускаются bugfix/test/docs/security изменения и стабилизация существующих подсистем.

# Roadmap CajeerBots

Текущая версия: `0.18.0-beta.1`.

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

### 0.14.0 — Production Observability
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

### 0.18.0-beta.1 — Docs / GitFlic Wiki Operations
- DB-backed GitFlic Wiki documentation collections, pages, snapshots, checks и exports.
- Markdown/frontmatter parser, link/anchor checker и search.
- API `/api/docs/*`, CLI `cajeer docs ...`, SDK methods.
- Scheduler handlers `maintenance.docs_sync` и `maintenance.docs_check`.


### 0.16.0-beta.1 — Feature Freeze
- Только багфиксы, тесты, docs и hardening.

### 1.0.0 — Stable Product Release
- API, bot-runtime, worker и CLI стабильно запускаются через Docker Compose.
- Telegram/VK/Discord имеют рабочие интерфейсы управления.
- Security/API/Bot Runtime/Extension Runtime/Event API/Registry стабилизированы.
- Документация на русском закрывает установку, настройку, безопасность, обновления и восстановление.


### 0.11.0 — Job Scheduler & Maintenance

Готово: DB-backed scheduled jobs, run history, maintenance snapshots, scheduler worker, manual run, default cleanup/retention/diagnostics jobs, CLI/API/SDK.


### 0.14.0 — DevTools и bot scenario runner

Статус: выполнено. Добавлены debug sessions/events, provider-update simulator, bot scenarios, scenario runs, CLI/API/SDK и импорт/экспорт сценариев.


## 0.14.0 — Observability / Production Diagnostics

Готово: metric samples, probes, alert rules, alert events, incidents, worker, API, CLI, SDK, seed и GitFlic Wiki документация.


## 0.16.0 — Deployment & Release Operations

Готово: production Docker Compose profile, migration job, non-root runtime images, generated production env, Nginx/systemd generators, DB-backed deployment targets/checks/runs, release artifacts/manifest, API, CLI, SDK и scheduler readiness check.


## 0.16.0 — Contract-first API Governance / SDK Sync

Статус: выполнено в product-layer.

- Добавлены OpenAPI snapshots и baseline snapshots.
- Добавлен diff engine для breaking changes.
- Добавлена проверка SDK coverage по фактическому `CajeerClient`.
- Добавлены API deprecations с replacement/removeAfter policy.
- Добавлены API/CLI/SDK методы для contract checks и SDK generation runs.
- Добавлен scheduled job `maintenance.contracts_check`.

Следующий слой после 0.18.0-beta.1: beta-hardening, contract/docs checks в CI и стабилизация release candidate.


## 0.18.0-beta.2 — Integration Testing / Beta Regression

Статус: реализовано в patch `0.18.0-beta.2`.

Цель: не добавлять крупные новые feature-subsystems, а закрепить beta 1 через воспроизводимые integration runs.

Состав:

- integration suites/cases/runs/step-runs/coverage в БД;
- runner для API, DB, Event API, Outbox, Bot Scenario и Release Gates;
- manual run через API/CLI;
- queued run через worker;
- scheduled smoke job;
- coverage по MVP-областям: API, security, bots, events, extensions, ops.

Критерий готовности beta 2:

- `mvp-smoke` проходит без ручных правок;
- failed step сохраняется с request/response/assertion/error;
- queued run выполняется worker-ом;
- coverage показывает непокрытые target-области для подготовки beta 3/security beta.

## 1.0.0-rc.1 — API freeze и production rehearsal

Статус: реализовано в patch `1.0.0-rc.1`.

Критерии RC1:

- API freeze snapshot создан и хранится в БД;
- OpenAPI проверяется на удалённые операции, изменённые operationId и duplicate operationId;
- compatibility matrix фиксирует поддерживаемые runtime/database/cache/provider/deployment targets;
- upgrade plan описывает переход `0.18.0-beta.3 -> 1.0.0-rc.1`;
- upgrade rehearsal пишет результат в `release_upgrade_runs`;
- readiness run объединяет API freeze, compatibility, release blockers, security findings и integration smoke;
- scheduled job `maintenance.rc_readiness` запускает регулярную RC-проверку.

Следующий этап: `1.0.0-rc.2` — full production rehearsal: чистая установка, подключение Telegram/VK/Discord, установка extension, backup/restore, update check, security/integration run.

