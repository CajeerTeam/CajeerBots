> Runtime version: 0.31.0

# NMDiscordBot

NMDiscordBot — боевой Discord runtime для community-layer NeverMine.

## Канонические источники истины
- релизная версия: `pyproject.toml`
- build metadata: `build_info.json`
- content pack: `templates/content.json`
- server layout spec: `templates/server_layout.json`
- machine-readable change journal: `change_journal.json`

Если runtime/build/content schema расходятся, strict preflight завершится ошибкой.
Перед публикацией/деплоем production-архива выполняй `python -m nmbot.main --release-check`: команда проверяет конфиг, версии, schema/event contract, env schema, JSON-шаблоны, права запуска скриптов и отсутствие запрещённых release-путей.

## Основные live-ops возможности
- onboarding и панели ролей интересов;
- forum workflow: поддержка, баги, предложения, апелляции, набор в гильдию;
- chronicle / lore / world-signals workflows;
- triage, ownership, SLA/escalation, scheduled jobs;
- bridge ingress/egress, replay, DLQ, диагностика и dead-letter handling;
- shared PostgreSQL/SQLite/Redis operational model;
- recovery mode для безопасного старта во время инцидента;
- русскоязычный visible/staff UX.

## CLI
```bash
python -m nmbot.main --check-config
python -m nmbot.main --prepare-runtime
python -m nmbot.main --migrate-only
python -m nmbot.main --schema-info
python -m nmbot.main --schema-doctor
python -m nmbot.main --preflight
python -m nmbot.main --self-test
python -m nmbot.main --history-snapshot
python -m nmbot.main --list-backups
python -m nmbot.main --run-cleanup-once
python -m nmbot.main --release-check
python -m nmbot.main --recovery-mode
```


## Setup wizard
```bash
./setup_wizard.py --profile minimal
./setup_wizard.py --profile production
./setup_wizard.py --profile bridge
./setup_wizard.py --profile discord-layout
./setup_wizard.py --validate-only
./setup_wizard.py --profile production --non-interactive
```

Профили позволяют не проходить все env-переменные подряд. `production` сохраняет прежний полный режим, `minimal` нужен для базового старта, `bridge` — для Telegram/VK/Workspace ingress/egress, `discord-layout` — для ролей и каналов Discord.

## Release packaging
```bash
python -m nmbot.release_pack --mode private-production --format both
python -m nmbot.release_pack --mode public-release --format zip
python -m nmbot.release_pack --mode patch-changed-files --changed-file changed-files.txt --format zip
```

Режимы:
- `private-production` сохраняет реальный `.env` и подходит для внутреннего боевого архива;
- `public-release` отказывается собираться, если в корне есть `.env`;
- `patch-changed-files` собирает только указанные изменённые файлы и не включает `.env`.

`zip`/`tar.gz` через packer сохраняют executable-bit для `install.sh`, `run.sh`, `upgrade.sh` и `setup_wizard.py`.

## Command surface
`COMMAND_SURFACE_MODE=compat` — режим по умолчанию: grouped-команды являются основными, legacy flat-команды оставлены для совместимости.

Доступные значения:
- `compat`;
- `grouped-only`;
- `legacy-only`.

Staff-команды дополнительно закрываются через Discord `default_permissions`, а runtime `_require_scope()` остаётся вторым слоем защиты.

## Install / upgrade / run
```bash
./install.sh   # первое развёртывание и миграции
./upgrade.sh   # обновление зависимостей + preflight + migrate-only
./run.sh       # ручной запуск
```

Для systemd рекомендуется запускать `python -m nmbot.main` напрямую через unit-файл, а не через интерактивный `run.sh`.

## BotHost storage compatibility

Для BotHost все изменяемые файлы должны находиться вне Git-синхронизируемого кода:

- `/app/data` / `DATA_DIR` — постоянные данные конкретного бота: SQLite, backups, cache, runtime-state, файловые логи.
- `/app/data/logs` / `LOG_DIR` — файловые логи, если они нужны дополнительно к stdout/stderr панели.
- `/app/data/backups` / `BACKUP_DIR` — JSON backup перед критичными runtime-изменениями.
- `/app/shared` / `SHARED_DIR` — общее хранилище для нескольких ботов. Оно появляется только после включения Общего хранилища в BotHost и редеплоя бота.

Бот не создаёт `/app/shared` автоматически: если общее хранилище не включено, preflight выдаст предупреждение, а не ошибку. Секреты, `.env`, session-файлы и персональные SQLite-базы не следует хранить в `/app/shared`.

Для HTTP ingress/webhook/Mini App внутри BotHost приложение должно слушать `0.0.0.0:8080`: в `.env` используется `INGRESS_HOST=0.0.0.0`, `INGRESS_PORT=8080`, `PORT=8080`, а публичный HTTP-сервер задан как `APP_PUBLIC_URL=https://nmdiscordbot.bothost.ru`.


## Remote logs.cajeer.ru

NMDiscordBot умеет отправлять runtime-логи в self-hosted `logs.cajeer.ru` через встроенный stdlib-only handler `nmbot.remote_logs.RemoteLogHandler`. Внешний Python-пакет не нужен.

1. В `logs.cajeer.ru` создай bot token для `project=NeverMine`, `bot=NMDiscordBot`, `environment=production` через `/bots` или `bin/make-bot-token.php`.
2. В BotHost добавь env-переменные:

```env
REMOTE_LOGS_ENABLED=true
REMOTE_LOGS_URL=https://logs.cajeer.ru/api/v1/ingest
REMOTE_LOGS_TOKEN=<raw-token-from-logs>
REMOTE_LOGS_PROJECT=NeverMine
REMOTE_LOGS_BOT=NMDiscordBot
REMOTE_LOGS_ENVIRONMENT=production
REMOTE_LOGS_LEVEL=INFO
REMOTE_LOGS_BATCH_SIZE=25
REMOTE_LOGS_FLUSH_INTERVAL=5
REMOTE_LOGS_TIMEOUT=3
REMOTE_LOGS_SPOOL_DIR=/app/data/remote-logs-spool
REMOTE_LOGS_SIGN_REQUESTS=false
```

Если `logs.cajeer.ru` временно недоступен, handler складывает batch-файлы в spool-каталог и переотправляет их позже. Если `REMOTE_LOGS_ENABLED=true`, но URL или token не заданы, бот не падает: удалённая отправка отключается с warning в stderr.

## Content-layer
Канонические источники Discord presentation/runtime schema: `templates/content.json` и `templates/server_layout.json`.

## Ключевые env-переменные
- `DISCORD_TOKEN`
- `DISCORD_GUILD_ID`
- `DISCORD_CONTENT_FILE_PATH=./templates/content.json`
- `CONTENT_SCHEMA_VERSION_REQUIRED=4`
- `STRICT_RUNTIME_PRECHECK=true`
- `STRICT_ENV_PRODUCTION_HYGIENE=true`
- `BACKUP_ON_CRITICAL_CHANGES=true`
- `DATA_DIR=/app/data`
- `SHARED_DIR=/app/shared`
- `SQLITE_PATH=/app/data/nmdiscordbot.sqlite3`
- `BACKUP_DIR=/app/data/backups`
- `LOG_DIR=/app/data/logs`
- `REMOTE_LOGS_ENABLED=false`
- `REMOTE_LOGS_URL=https://logs.cajeer.ru/api/v1/ingest`
- `REMOTE_LOGS_TOKEN=<raw-token-from-logs>`
- `REMOTE_LOGS_SPOOL_DIR=/app/data/remote-logs-spool`
- `MIN_FREE_DISK_MB=256`
- `RECOVERY_MODE_DEFAULT=false`
- `COMMAND_SURFACE_MODE=compat`
- `INGRESS_ENABLED=true`
- `APP_PUBLIC_URL=https://nmdiscordbot.bothost.ru`
- `INGRESS_HOST=0.0.0.0`
- `INGRESS_PORT=8080`
- `PORT=8080`
- `STAFF_SCOPE_ROLE_MAP_JSON`
- `FORUM_POLICY_OVERRIDES_JSON`
- `BRIDGE_PAYLOAD_ALLOWLIST_JSON`
- `BRIDGE_EVENT_RULES_JSON`
- `BRIDGE_MAX_ATTEMPTS=8`
- `BRIDGE_RETRY_BACKOFF_BASE_SECONDS=15`
- `BRIDGE_RETRY_BACKOFF_MAX_SECONDS=900`
- `BRIDGE_DESTINATION_CIRCUIT_BREAKER_THRESHOLD=5`
- `BRIDGE_DESTINATION_CIRCUIT_OPEN_SECONDS=300`
- `SCHEDULER_MAX_ATTEMPTS=5`
- `SCHEDULER_RETRY_BACKOFF_BASE_SECONDS=30`
- `SCHEDULER_RETRY_BACKOFF_MAX_SECONDS=1800`
- `DRIFT_ALERT_COOLDOWN_SECONDS=1800`
- `METRICS_ENABLED=false`
- `METRICS_PATH=/internal/metrics`
- `METRICS_REQUIRE_AUTH=true`
- `METRICS_BEARER_TOKEN`
- `METRICS_ALLOWED_IPS=127.0.0.1,::1`
- `RULES_REACCEPTANCE_ENFORCEMENT_ENABLED=true`
- `RULES_REACCEPTANCE_GRACE_HOURS=72`
- `RULES_REACCEPTANCE_REMINDER_HOURS=24`
- `RULES_REACCEPTANCE_CHECK_INTERVAL_SECONDS=900`

## Политика хранения данных
- audit log очищается по `AUDIT_LOG_RETENTION_DAYS`
- verification sessions очищаются по `VERIFICATION_SESSION_RETENTION_DAYS`
- relay history очищается по `RELAY_HISTORY_RETENTION_DAYS`
- scheduled jobs и bridge events без явной policy не удаляются автоматически
- перед критичными изменениями бот может сохранять JSON backup в `BACKUP_DIR`

## Восстановление
1. Выполни `python -m nmbot.main --list-backups` и выбери нужный snapshot.
2. Прогони `python -m nmbot.main --schema-info` и `--preflight`.
3. При инциденте запусти runtime в recovery mode.
4. Восстанови maintenance mode / runtime forum policy overrides / panel bindings / topic metadata / replay failed bridge events / runtime markers по backup или `state_export`.
5. Для `state_restore` используй dry-run, затем approval flow и только потом применение.
6. После восстановления прогони `layout_repair` в dry-run и затем в apply-режиме.

## Deployment
- nginx template: `infra/nginx/nmdiscordbot-webhook.conf`
- systemd unit: `infra/systemd/nmdiscordbot.service`
- logrotate policy: `infra/logrotate/nmdiscordbot`

## Операционный плейбук
### Первый запуск
1. заполнить `.env`
2. выполнить `./install.sh`
3. выполнить `python -m nmbot.main --preflight`
4. выполнить `python -m nmbot.main --schema-doctor`
5. выполнить `python -m nmbot.main --release-check`
5. запустить `./run.sh` или systemd unit

### Обновление
1. сохранить `state_export`
2. выполнить `./upgrade.sh`
3. проверить `python -m nmbot.main --schema-info`
4. проверить `python -m nmbot.main --schema-doctor`
5. проверить `python -m nmbot.main --release-check`
6. перезапустить runtime

### Maintenance mode
Используй `/maintenance_mode`, чтобы временно закрыть создание новых public topics без отключения staff-инструментов.

### Recovery mode
`python -m nmbot.main --recovery-mode` запускает runtime без panel reconcile, ingress, relay, external sync и scheduler loops. Это режим для инцидентов и ручной диагностики.

## Язык
Имена slash-команд остаются на английском. Всё, что пользователь и staff видят в Discord, должно быть на русском.


## Восстановление состояния

- `state_export` формирует JSON-снимок operational state.
- `state_restore` поддерживает planner/dry-run и безопасное восстановление `maintenance_mode`, `runtime_forum_policy_overrides`, `panel_registry`, `layout_alias_bindings`, `scheduled_jobs`, `topics`, `failed_bridge_events`, `panel_drift`, `bridge_destination_state`, `schema_meta`, `schema_meta_ledger` и `runtime_markers` через approval flow.
- Перед критичными изменениями бот сохраняет резервную копию в `BACKUP_DIR`.

## Логи

Для production используется один канонический путь: файловый лог без встроенной ротации Python + внешняя ротация через `infra/logrotate/nmdiscordbot`.


## Политика вложений
Бот поддерживает ограничения по вложениям для forum-тем через `FORUM_ATTACHMENT_POLICY_JSON`, `ATTACHMENT_MAX_BYTES_DEFAULT`, `ATTACHMENT_ALLOWED_EXTENSIONS_DEFAULT` и `ATTACHMENT_BLOCKED_EXTENSIONS_DEFAULT`.

## Staff digest и observability
- `/staff_digest_now` — мгновенная staff-сводка
- `/staff_digest_schedule` — отложенная staff-сводка через scheduler
- `/history_snapshot` — сводка по bridge/job истории с фильтрами и реальным временным окном
- `/audit_search` — поиск аудита с export в embed/csv/json
- `/chronicle_entry` и `/world_signal_publish` — lore/world workflows

## Безопасичное восстановление
`/state_restore` поддерживает planner/dry-run, approval flow и восстанавливает только безопасные секции снимка.


## Что нового в 0.29.0
- Inbound bridge-комментарии теперь зеркалируются по `external_comment_id`: `comment.appended`/`edited` редактируют существующее Discord-сообщение, а `comment.deleted` удаляет mirror вместо публикации отдельной заметки.
- Добавлен enforcement для повторного принятия правил: grace/reminder policy, staff summary и автоматическое ограничение ролей после истечения окна перепринятия.
- `state_restore` умеет безопасно восстанавливать и replay-секции `topics`, `failed_bridge_events`, `panel_drift`, а diagnostics snapshot сохраняется в runtime markers namespace.
- Subscription live routing расширен на `announcement`, `devlog`, `guild_recruitment`, `chronicle` и `lore_discussion`.
- Появились `schema_migration_plan`, `bridge_comment_mirror` storage layer и governance helpers для surface policy/alias map.
- Capability report теперь проверяет runtime hooks для comment mirror, reacceptance loop, restore replay и group alias policy.

## Что нового в 0.27.0
- `state_restore` теперь может восстанавливать не только maintenance/panels/jobs, но и `bridge_destination_state`, `schema_meta`, `schema_meta_ledger`, `runtime_markers`.
- Добавлен фоновый sweeper для истёкших approval requests.
- Bridge для forum-комментариев расширен до `comment.edited` и `comment.deleted`.
- Появилось планирование `targeted_digest` через scheduler и digest group alias.
- `layout_repair` стал строже: exact role permissions и legacy-marking для лишних ресурсов/заменённых каналов.
- Capability report показывает grouped/flat command surface policy, а schema parity check стал глубже.

## Что нового в 0.26.0
- scheduler получил полноценный retry lifecycle: pending/retry/dead-letter, backoff и due-window выборку;
- cleanup, scheduler и runtime drift loops защищены distributed locks для multi-instance режима;
- server layout spec расширен role permissions и channel/forum permission matrix, а `layout_repair` умеет глубже чинить reconcile;
- bridge delivery ведёт destination-level circuit state с consecutive failures и circuit open window;
- runtime drift публикует deduped staff alerts и отправляет resolved-сигнал после восстановления;
- event contracts переведены на schema version 3 и валидируют unlink payloads для Telegram/VK/Workspace.

- `targeted_digest_schedule` и `staff_digest_schedule` поддерживают recurring schedule: можно задать интервал повторения и лимит запусков, а scheduler сам создаёт следующий job после успешной отправки.
- `state_restore` теперь понимает отдельные diagnostics-секции: `content_pack_meta`, `layout_spec_meta`, `runtime_markers_snapshot`, `build_metadata`.
- Capability report стал глубже: показывает routing coverage, recurring digest support, migration plan sync и расширенные runtime hook checks.
- Subscription routing поддерживает wildcard/prefix matching по `event_kinds`, поэтому preferences вида `community.report.*` и `community.appeal.*` теперь реально работают.


## Что нового в 0.30.0
- Transport layer теперь симметричнее: добавлены validators и runtime coverage для `community.report.*`, `community.announcement.*` и `community.devlog.*`.
- Inbound mirror использует registry-таблицы для внешних discussion/content идентификаторов, поэтому внешняя система больше не обязана знать Discord thread/message id напрямую.
- `announcement`/`devlog` события теперь редактируют и удаляют ранее опубликованные Discord-сообщения через stable message mirror.
- Внешние комментарии публикуют attachment links и image preview embeds, а inbound `report` получил собственный lifecycle-path через `#reports`.
- Добавлены calendar-команды для targeted/staff digests: daily/weekly schedule по локальному времени и timezone.
- `layout_repair` теперь кладёт лишние ресурсы в legacy review queue с review/delete окнами, а cleanup loop шлёт staff notice по накопившимся legacy-ресурсам.
- Capability report показывает declared/handled transport coverage и routing gaps для subscription events.


## Patch 0.31.0
- Snapshot теперь включает mirror registries.
- Добавлены outbound lifecycle-команды для announcement/devlog и topic_update.
- Report workflow переведён в thread-based path.
- Scheduler поддерживает weekday_set/monthly.


## Интеграция с NMTelegramBot

NMDiscordBot интегрируется с Telegram через signed bridge endpoint NMTelegramBot. Discord-токен не передаётся Telegram-боту, Telegram-токен не хранится в Discord-боте.

### Discord → Telegram

```env
TELEGRAM_BRIDGE_URL=http://127.0.0.1:8090/internal/discord/event
OUTBOUND_HMAC_SECRET=<same-as-NMTelegramBot-BRIDGE_INBOUND_HMAC_SECRET>
OUTBOUND_KEY_ID=v1

BRIDGE_EVENT_RULES_JSON={"community.announcement.created":["telegram"],"community.devlog.created":["telegram"],"community.event.created":["telegram"],"community.world_signal.created":["telegram"]}
```

### Telegram → Discord

Включи HTTP ingress:

```env
INGRESS_ENABLED=true
APP_PUBLIC_URL=https://nmdiscordbot.bothost.ru
INGRESS_HOST=0.0.0.0
INGRESS_PORT=8080
PORT=8080
INGRESS_HMAC_SECRET=<same-as-NMTelegramBot-DISCORD_BRIDGE_HMAC_SECRET>
INGRESS_STRICT_AUTH=true
```

NMTelegramBot должен отправлять сюда. Для BotHost внешний URL — `https://nmdiscordbot.bothost.ru/`, внутренний порт контейнера — `8080`:

```env
DISCORD_BRIDGE_URL=https://nmdiscordbot.bothost.ru/internal/bridge/event
DISCORD_BRIDGE_HMAC_SECRET=<same-as-NMDiscordBot-INGRESS_HMAC_SECRET>
```

Для первого этапа рекомендуется синхронизировать только `announcements`, `devlog`, `events` и `world_signal`, без зеркалирования всего чата.


## Production diagnostics patch

Дополнительные проверки перед запуском:

```bash
python -m nmbot.main --env-doctor
python -m nmbot.main --discord-bindings-check
python -m nmbot.main --export-discord-bindings
python -m nmbot.release_pack --check-archive NMDiscordBot-main.zip --check-mode private-production
```

`--env-doctor` печатает redacted-отчёт по `.env`: заполненность каналов/ролей, bridge-destinations, Redis/ingress/metrics и валидность `BRIDGE_EVENT_RULES_JSON`.

`--discord-bindings-check` подключается к Discord API и проверяет, что channel/role IDs существуют и соответствуют ожидаемым типам (`Forum`, `Stage`, `Text`).

`--export-discord-bindings` генерирует готовый блок `.env` с ID каналов и ролей по canonical names из `templates/server_layout.json`.

`release_pack --check-archive` проверяет уже собранный `.zip`/`.tar.gz`: executable-bit, запрет `.github/`, `tests/`, `.env.example`, `.pyc`/`__pycache__`, а также наличие production-safety файлов.

Bridge rules теперь поддерживают event-contract keys напрямую:

```env
BRIDGE_EVENT_RULES_JSON={"community.announcement.created":["telegram","vk"],"community.devlog.created":["telegram","vk"],"community.event.created":["telegram","vk"],"community.world_signal.created":["telegram","vk"]}
```

Для Community Core поддерживаются оба alias: `community_core` и legacy `community`.

## Bridge diagnostics and integration profile

Дополнительная диагностика bridge-маршрутов:

```bash
python -m nmbot.main --bridge-doctor
```

`--bridge-doctor` показывает, какие destinations реально настроены (`community_core`, `telegram`, `vk`, `workspace`), какие события маршрутизируются через `BRIDGE_EVENT_RULES_JSON`, есть ли outbound auth и не настроены ли правила на destination без URL.

Для быстрой настройки интеграций используй отдельный профиль wizard:

```bash
./setup_wizard.py --profile integrations
```

Профиль `integrations` трогает только bridge/ingress/metrics-поля: `TELEGRAM_BRIDGE_URL`, `VK_BRIDGE_URL`, `COMMUNITY_CORE_EVENT_URL`, `WORKSPACE_BRIDGE_URL`, outbound/ingress secrets и `BRIDGE_EVENT_RULES_JSON`.

Canonical private-production сборка должна выполняться через release packer:

```bash
python -m nmbot.release_pack --mode private-production --format both
python -m nmbot.release_pack --check-archive dist/NMDiscordBot-private-production.zip --check-mode private-production
```

`release_required_files.json` фиксирует обязательный production-safety слой, чтобы следующие архивы не теряли `config_schema`, release/schema/env/discord/bridge diagnostics и bridge runtime extraction.


## Bridge smoke, ingress smoke и event coverage

Для проверки реальной доставки Discord → Telegram/VK/Workspace/community core используй:

```bash
python -m nmbot.main --bridge-smoke
```

По умолчанию отправляется synthetic event `community.world_signal.created`. Другой event type можно указать так:

```bash
python -m nmbot.main --bridge-smoke --bridge-smoke-event community.announcement.created
```

Для проверки обратного входящего HTTP ingress, когда основной процесс бота уже запущен с `INGRESS_ENABLED=true`:

```bash
python -m nmbot.main --ingress-smoke
```

Команда отправляет валидный signed admin smoke event в `/internal/bridge/admin`, затем проверяет, что неверная HMAC-подпись отклоняется.

Для отчета по покрытию event contract и маршрутам bridge:

```bash
python -m nmbot.main --event-coverage
```

В Discord доступны staff-команды:

```text
/bridge preview
/bridge coverage
/bridge_preview
/event_coverage
```

`/bridge preview` показывает, куда будет отправлен конкретный event key, есть ли payload validator, какие destinations настроены и какой тип outbound auth активен.

## Runtime modular split

`bot.py` дополнительно разгружен:

- grouped slash aliases вынесены в `nmbot/bot_grouped_commands.py`;
- layout drift/repair runtime helpers вынесены в `nmbot/bot_layout_runtime.py`;
- bridge routing runtime уже находится в `nmbot/bot_bridge_runtime.py`.

Это снижает риск регрессий при следующих изменениях команд, layout repair и bridge routing.
