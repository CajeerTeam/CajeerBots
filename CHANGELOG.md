# Changelog

## [1.0.0-rc.1] - 2026-06-17

### Добавлено
- Рабочий слой Security Beta Audit: policies, runs, findings и risk snapshots.
- API `/api/security/audit/*` и `/api/security/risk-snapshots` для beta security checks.
- CLI `cajeer security` для запуска audit, управления policies/findings и отзыва stale sessions.
- Package `@cajeerbots/security-audit` с проверками 2FA, API keys, secrets rotation, webhook secretRef, rate-limit violations и open security blockers.
- Scheduled job `maintenance.security_audit` для регулярного security audit перед RC.

### Изменено
- Версия проекта обновлена до `1.0.0-rc.1`.
- `security/hardening` теперь учитывает открытые critical/error security findings.


## [0.18.0-beta.2] - 2026-06-17

### Добавлено
- Рабочий слой Integration Testing / Beta Regression: suites, cases, queued/manual runs, step results и coverage snapshots.
- Package `@cajeerbots/integration-tests` с runner для `api_health`, `http_request`, `database_count`, `event_publish`, `outbox_probe`, `bot_scenario`, `release_gate_summary`.
- API `/api/integration-tests/*`, CLI `cajeer integration-tests` и SDK methods для regression/smoke проверок.
- Worker `IntegrationTestsWorker` для queued integration runs.
- Scheduled job `maintenance.integration_smoke` и seed default MVP smoke suites.

### Изменено
- Версия проекта обновлена до `0.18.0-beta.2`.
- Scheduler получил built-in handler `maintenance.integration_smoke`.
- Release beta-процесс дополнен coverage snapshot для MVP областей: API, security, bots, events, extensions, ops.

## [0.18.0-beta.1] - 2026-06-19

### Added
- Рабочий слой GitFlic Wiki Docs Operations / Knowledge Base Runtime.
- Prisma migration `20260617171700_product_0170`.
- DB-модели `documentation_collections`, `documentation_pages`, `documentation_snapshots`, `documentation_checks`, `documentation_exports`.
- Package `@cajeerbots/docs`:
  - markdown/frontmatter parser;
  - scanner локальной wiki-директории;
  - проверка внутренних ссылок и anchors;
  - простой полнотекстовый поиск по документации;
  - export bundle `cajeerbots.docs.bundle.v1`.
- API `/api/docs/*` для collections, sync, pages, search, checks, snapshots и exports.
- CLI `cajeer docs ...` для синхронизации, проверки, поиска и экспорта wiki.
- Scheduler handlers `maintenance.docs_sync` и `maintenance.docs_check`.
- SDK methods для documentation operations.
- Seed добавляет collection `gitflic-wiki`, permissions `docs:*` и maintenance jobs.
- Wiki `18-docs-gitflic-wiki-operations.md`.

### Changed
- Workspace packages обновлены до `0.18.0-beta.1`.
- OpenAPI version обновлён до `0.18.0-beta.1` и дополнен `Docs` operations.



## [0.16.0] - 2026-06-17

### Added
- Рабочий слой Deployment & Release Operations без внешней веб-панели.
- Prisma migration `20260617151500_product_0150`.
- DB-модели `deployment_targets`, `deployment_checks`, `deployment_runs`, `release_artifacts`.
- Package `@cajeerbots/deployment`:
  - генерация `.env.production`;
  - validation production env;
  - генерация Nginx reverse proxy config;
  - генерация systemd units;
  - Docker Compose production readiness checks;
  - release manifest builder/validator.
- API `/api/deployment/*` для targets/checks/runs/env/nginx/systemd/release-artifacts.
- CLI `cajeer deployment`:
  - `targets:list`, `targets:set`;
  - `checks:run`, `checks:list`;
  - `env:generate`, `env:validate`;
  - `nginx:config`, `systemd:units`;
  - `release:manifest`.
- `docker-compose.prod.yml` с PostgreSQL, Redis, migration job, API, bot-runtime и worker.
- `.env.production.example`.
- `docker/nginx/cajeerbots.conf`.
- systemd unit templates для API, bot-runtime и worker.
- Scheduler handler `maintenance.deployment_readiness`.
- SDK methods для Deployment & Release Operations.
- Wiki `16-deployment-release-operations.md`.

### Changed
- Workspace packages обновлены до `0.16.0`.
- Runtime Dockerfiles переведены на non-root user `10001:10001`.
- Seed добавляет default deployment target `docker.production` и permissions `deployment:*`.

## [0.14.0] - 2026-06-17

### Добавлено

- Рабочий слой Backup/Restore & Storage Catalog.
- Таблицы `storage_objects`, `backup_artifacts`, `backup_restore_runs`.
- Package `@cajeerbots/backup` с backup bundle v1, checksum verification, local/S3 storage factory и community snapshot helpers.
- API `/api/storage/*`, `/api/backups/*`, `/api/communities/:communityId/backups`.
- CLI `cajeer storage` и `cajeer backup`.
- Worker `BackupRetentionWorker` для purge удалённых storage objects после retention window.
- SDK методы для storage objects, backup artifacts, verify и restore runs.
- Maintenance job `maintenance.backup_retention`.

### Изменено

- Версия проекта и workspace packages обновлена до `0.14.0`.
- `import-export` теперь отдаёт payload версии `0.14.0`.

## [0.12.0] - 2026-06-18

### Добавлено
- Рабочий DX DevTools слой: debug sessions, debug events, bot scenarios и scenario runs.
- Provider-update simulator для Telegram/VK/Discord без внешней веб-панели.
- Bot scenario runner с шагами `message`, `command`, `callback`, `form`, `wait`, `assert_outbound`.
- API `/api/devtools/*`, CLI `cajeer dev`, `cajeer devtools ...` и SDK-методы DevTools.
- Prisma migration `20260617121200_product_0120`.
- Seed-сценарий `system.help-smoke` и debug-команда `cajeer_debug`.

### Изменено
- Версия workspace packages обновлена до `0.12.0`.
- Import/export community теперь переносит bot scenarios.

## [0.11.0] - 2026-06-17

### Добавлено

- Рабочий DB-backed Job Scheduler & Maintenance layer.
- Таблицы `scheduled_jobs`, `scheduled_job_runs`, `maintenance_snapshots`.
- API `/api/jobs/*` для расписаний, ручного запуска, run history, queue summary и maintenance snapshots.
- Worker `SchedulerWorker` с lock, nextRunAt, run history и audit log.
- Built-in maintenance handlers для idempotency cleanup, Event API retention, stale processing retry, diagnostics snapshots и очистки render logs.
- CLI `cajeer jobs` для управления scheduled jobs.
- SDK методы для jobs/scheduler.
- Seed default maintenance jobs.

### Изменено

- Версия workspace packages обновлена до `0.11.0`.
- Worker теперь запускает scheduler worker вместе с outbox, extension runtime, bot outbound и Event API delivery workers.

Формат основан на [Keep a Changelog](https://keepachangelog.com/ru/1.1.0/), версия проекта следует SemVer.


## [0.10.0] - 2026-06-17

### Добавлено
- Рабочий Registry/Updates product-layer: registry sources, packages, versions, update plans и update history.
- Новый package `@cajeerbots/registry` с parser registry manifest, SemVer/range planner, integrity helpers и update candidate selection.
- Prisma migration `20260617101000_product_0100` с таблицами `registry_sources`, `registry_packages`, `registry_package_versions`, `update_plans`, `update_history`.
- API `/api/registry/*` для source management, sync, catalog browsing и установки modules/plugins из registry.
- API `/api/updates/*` для создания core/extension update plans, dry-run/apply и просмотра history.
- CLI `cajeer registry ...` и расширенный `cajeer update ...`.
- SDK methods для registry sources/packages/install и update plans/history.
- Registry JSON Schema 1.0 для GitFlic Registry/Releases manifest.
- Permissions: `registry:read`, `registry:write`, `registry:sync`, `updates:read`, `updates:plan`, `updates:apply`.

### Изменено
- Версия workspace packages обновлена до `0.10.0`.
- Extension install из registry теперь сохраняет `registrySourceId`, `registryPackageName`, `registryVersionId`.
- Seed добавляет permissions Registry/Updates и source `cajeer-official`.

## [0.9.0] - 2026-06-17

### Добавлено
- Рабочий Event API: durable event journal, topics, subscriptions, deliveries, replay и dead-letter queue.
- API `/api/events/*` для публикации событий, управления topics/subscriptions, просмотра journal и повторной доставки.
- Worker `EventApiDeliveryWorker` для доставки event subscriptions в `outbox`, `webhook_endpoint`, `extension_hook` или `log`.
- Prisma migration `20260617090900_product_090` с таблицами `event_topics`, `event_records`, `event_subscriptions`, `event_subscription_deliveries`, `event_dead_letters`.
- CLI-команда `cajeer events` для publish/replay/subscriptions/deliveries/dead-letter.
- SDK methods для Event API.
- Event API permissions: `event_api:read`, `event_api:write`, `event_api:publish`, `event_api:replay`.

### Изменено
- Incoming signed webhooks теперь дополнительно пишутся в durable event journal и создают deliveries для Event API subscriptions.
- Import/export community переносит Event API topics и subscriptions.
- Worker запускает outbox, extension runtime, bot outbound и event-api delivery loop.

## [0.8.0] - 2026-06-17

### Добавлено
- Рабочий Message Core product-layer: locale catalogs, provider-aware button layouts, form definitions/submissions и render logs.
- Prisma migration `20260617080800_product_080` с таблицами `locale_catalogs`, `message_button_layouts`, `message_form_definitions`, `message_form_submissions`, `message_render_logs`.
- API `/api/communities/:communityId/message-core/*` для локализации, кнопок, форм, form submissions, preview/render и журнала рендеров.
- Provider-aware renderer в `@cajeerbots/message-core`: Telegram inline keyboard, VK callback keyboard, Discord components.
- Валидация форм без внешней панели: `text`, `number`, `boolean`, `select`, `multiselect`, required/min/max/pattern/options.
- CLI `cajeer message-core ...`: `catalog:list/set`, `buttons:list/set`, `forms:list/set/submit`, `render`.
- SDK методы для Message Core: locale catalogs, button layouts, forms, submissions, render и render logs.
- Import/export community теперь переносит locale catalogs, button layouts и message forms.
- Seed добавляет системный catalog, layout `system.main` и форму `support.request`.

### Изменено
- Версия workspace packages обновлена до `0.8.0`.
- Bot runtime теперь сохраняет render logs и передаёт buttons в outbound delivery.
- Provider senders теперь отправляют кнопки нативно: Telegram `reply_markup`, VK `keyboard`, Discord `components`.
- Runtime responses больше не добавляют кнопки текстом, если provider поддерживает native buttons.
- Добавлены scopes `message_core:read` и `message_core:write`.

### Исправлено
- Убран дублированный `@@map("admin_accounts")` в Prisma schema.

## [0.7.0] - 2026-06-17

### Добавлено
- Рабочий Bot Runtime MVP: heartbeat/status в БД, journal входящих provider updates и journal исходящих bot messages.
- Prisma migration `20260617070700_product_070` с таблицами `bot_runtime_statuses`, `bot_inbound_updates`, `bot_outbound_messages`.
- Webhook-mode HTTP server в `bot-runtime`: `/runtime/health`, `/runtime/webhooks/:accountId`, `/runtime/webhooks/:provider/:accountId`.
- Нормализация Telegram/VK/Discord webhook payloads в общий Interaction Core pipeline.
- Provider senders для Telegram `sendMessage`, VK `messages.send`, Discord channel messages.
- API `/api/communities/:communityId/bot-runtime/*` для status, inbound updates, outbound messages, retry и webhook URL.
- CLI `cajeer runtime status`, `inbound:list`, `outbound:list`, `outbound:retry`, `webhook-url`.
- SDK методы `botRuntimeStatus`, `botInboundUpdates`, `botOutboundMessages`, `retryBotOutboundMessage`, `botWebhookUrl`.

### Изменено
- Версия workspace packages обновлена до `0.7.0`.
- Bot adapters теперь записывают входящие события и статусы исходящих ответов в БД.
- Runtime config дополнен `BOT_WEBHOOK_*`, `BOT_RUNTIME_HEARTBEAT_MS`, `BOT_RUNTIME_STALE_MS`, `VK_CALLBACK_CONFIRMATION`.
- Добавлены scopes `bot_runtime:read` и `bot_runtime:write`.

### Исправлено
- Webhook mode больше не является декларативной настройкой: bot-runtime поднимает HTTP endpoint для provider callbacks.

## [0.6.0] - 2026-06-17

### Добавлено
- Рабочий security hardening слой для административного доступа: admin accounts, password login, refresh sessions, TOTP 2FA и recovery codes.
- Prisma migration `20260617060600_product_060` с таблицами `admin_accounts`, `admin_sessions`, `two_factor_secrets`, `recovery_codes`, `rate_limit_violations`.
- API endpoints `POST /api/auth/admin/bootstrap`, `/login`, `/refresh`, `/logout`, `/password/change`, `/2fa/*`, `/sessions/*`, `/security/*`.
- Password hashing на PBKDF2-SHA256 с pepper, refresh-token rotation и audit log для login/logout/session/2FA/password событий.
- TOTP реализация без внешних зависимостей: secret generation, otpauth URI, verification window и recovery-code fallback.
- Rate-limit telemetry в БД и `/api/security/rate-limit-violations`.
- CLI `cajeer auth admin:create/list`, `sessions:list/revoke`.
- SDK методы для admin login, refresh/logout, sessions, 2FA и security hardening.

### Изменено
- Версия workspace packages обновлена до `0.6.0`.
- Runtime config дополнен `PASSWORD_PEPPER`, `REFRESH_TOKEN_PEPPER`, `RECOVERY_CODE_PEPPER`, `TOTP_ISSUER`, `API_RATE_LIMIT_*`.
- `GET /api/auth/me` теперь возвращает данные admin account для JWT actor type `user`.
- Исправлена ошибка Prisma schema с дублированным объявлением `ExtensionRuntimeLog`.

### Безопасность
- В production обязательны отдельные peppers для password, refresh token и recovery codes.
- Refresh tokens хранятся только в HMAC-hash виде и ротируются при каждом refresh.
- Recovery codes показываются только один раз при включении/ротации 2FA и хранятся только как hash.

## [0.5.0] - 2026-06-17

### Добавлено
- Рабочий Extension Runtime MVP: permission grants, runtime events, worker execution, extension storage, config history и runtime logs.
- Prisma migration `20260617050500_product_050` с таблицами `extension_permission_grants`, `extension_runtime_events`, `extension_storage_items`, `extension_runtime_logs`.
- Новый package `@cajeerbots/extension-runtime` с DB-backed queue, local entrypoint loader и execution context.
- Extension SDK расширен: `handleEvent`, typed event envelope, logger, events publish/enqueueHook и key/value storage.
- API endpoints для grants/revoke, config, events, retry, storage и logs расширений.
- Worker теперь запускает outbox worker и extension runtime worker.
- Hooks автоматически ставятся в очередь для `webhook.received`, `interaction.handled`, `interaction.failed`, `interaction.event`.
- CLI `cajeer extension install/list/enable/disable/permissions:*/event:*/storage:*`.
- Example extension теперь реально обрабатывает hooks, пишет storage и публикует outbound event.

### Изменено
- Версия workspace packages обновлена до `0.5.0`.
- Enable расширения проверяет, что permissions из manifest выданы через grants.
- Runtime config дополнен `EXTENSIONS_ROOT` и `EXTENSION_RUNTIME_ENABLED`.
- OpenAPI и SDK дополнены endpoints extension runtime.

### Безопасность
- Permissions расширения не считаются выданными автоматически: включение блокируется до grant/revoke workflow, если не использован `autoGrant`.
- Runtime logs и config history не хранят plain-text секреты сами по себе; секреты должны передаваться через encrypted secrets store.

## [0.4.0] - 2026-06-17

### Добавлено
- Рабочий Interaction Core product-layer: команды, меню, callback actions, conversation flows и журнал interaction events.
- API `/api/communities/:communityId/interactions/*` для управления bot-интерфейсами без внешней панели.
- Runtime-диспетчер bot interactions: Telegram/VK/Discord теперь читают команды из БД, рендерят message templates и пишут interaction events.
- Prisma migration `20260617040400_product_040` с таблицами `interaction_commands`, `interaction_menus`, `interaction_callback_actions`, `conversation_flows`, `interaction_events`.
- CLI `cajeer interactions ...` для создания/list команд, меню, conversations и просмотра событий.
- SDK-методы для команд, меню, callbacks, conversations и interaction events.

### Изменено
- Seed создаёт базовые команды `start`, `status`, `help`, главное меню и новые scopes `interactions:read/write`.
- Import/export community теперь включает commands, menus, callback actions и conversation flows.
- Bot-runtime больше не держит команды только в коде: базовый runtime поведение настраивается через БД.

## [0.3.0] - 2026-06-17

### Добавлено
- Encrypted secrets store на AES-256-GCM: API `GET|POST /api/secrets`, `PUT/DELETE /api/secrets/:key`, `POST /api/secrets/:key/rotate`, `GET /api/secrets/:key/value` с отдельным scope `secrets:reveal`.
- Prisma migration `20260617030300_product_030` с таблицами `secrets` и `secret_history`.
- Runtime config API: `GET /api/config/validate`, `GET /api/config/summary`, `GET /api/config/env` с redaction секретов.
- Import/export API для community: экспорт users/settings/roles/bot accounts/message templates и импорт с `dry-run`/`apply`.
- CLI-команды `cajeer config validate`, `cajeer config summary`, `cajeer secrets list/set/get/delete`, `cajeer community export/import`.
- `cajeer bot add --token`: токен сохраняется в encrypted secrets store, в bot account пишется `secret:<key>`.
- Bot runtime теперь загружает активные bot accounts из БД и резолвит `secret:<key>` / `env:<NAME>` tokenRef.
- SDK-методы для config, secrets и import/export.

### Изменено
- Версия workspace packages обновлена до `0.3.0`.
- Telegram/VK/Discord adapters больше не завязаны только на env-токены; env используется как fallback.
- `.env.example` дополнен `SECRETS_ENCRYPTION_KEY` и `IMPORT_EXPORT_MAX_BYTES`.
- Seed дополняет permissions scopes для secrets/config/import-export.

### Безопасность
- Secret values хранятся только в зашифрованном виде: ciphertext, iv, authTag, version.
- История ротации secret сохраняется без plain-text значения.
- Доступ к расшифровке отделён отдельным permission scope `secrets:reveal`.

## [0.2.0] - 2026-06-17

### Добавлено
- Рабочая авторизация API: bootstrap JWT через `POST /api/auth/token`, API keys через `POST /api/auth/api-keys`, отзыв ключей, scopes и `GET /api/auth/me`.
- Реальный `ScopedAuthGuard` для NestJS: JWT Bearer, `x-api-key`, bootstrap admin token, проверка scopes.
- Управление пользователями community: создание, чтение, обновление, назначение/снятие ролей.
- Управление ролями и permissions: список permissions, создание ролей, замена scopes роли.
- Управление bot accounts для Telegram, VK и Discord через API и CLI.
- Управление message templates: upsert, список, получение, render с переменными.
- Outbox API: просмотр сообщений, recent deliveries, ручной retry.
- Webhook delivery records: отдельные записи доставки по каждому endpoint, статусы, попытки, HTTP status, response body, last error.
- Prisma migration `20260617020200_product_020`.
- CLI-команды `cajeer auth token`, `cajeer auth api-key:create`, `cajeer bot add`, `cajeer bot list`.
- SDK-методы для auth, users, roles, bot accounts, message templates и outbox.

### Изменено
- Версия всех workspace packages обновлена до `0.2.0`.
- Worker outbox теперь не просто помечает message delivered/failed, а доставляет по endpoint delivery records с backoff и dead-state.
- Controllers переведены с простого admin-token guard на scoped auth.
- Seed создаёт реальные scopes, роли owner/admin и базовые message templates.
- OpenAPI обновлён под новые product endpoints.
- `PrismaService` стал NestJS injectable service с lifecycle hooks.

### Безопасность
- API keys хешируются через HMAC-SHA256 с `API_TOKEN_PEPPER`; raw token возвращается только один раз.
- JWT подписываются HS256 через `JWT_SECRET`, имеют `exp`, `iat`, scopes и principal type.
- Все важные изменения пишутся в audit log с actor id/type.

## [0.1.1] - 2026-06-17

### Добавлено
- Первый рабочий API слой: health, diagnostics, communities, settings, extensions, webhooks, webhook endpoints, audit.
- Signed payloads, idempotency и outbox enqueue.
- Worker доставки outbox events.
- CLI install/doctor/generate/extension validate/update check.
