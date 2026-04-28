# NMTelegramBot 2.2.0

Telegram runtime для экосистемы NeverMine.

## Что уже есть

- `/start`, `/help`, `/status`, `/online`, `/links`, `/stats`
- `/health`, `/adminstats`, `/announce`, `/broadcast`, `/schedule`, `/chatsettings`, `/pullannouncements`
- `/link`, `/link request`, `/link status`, `/link unlink`, `/link approve`, `/link reject`, `/link pending`, `/link history`, `/link revoke`, `/link cleanup`
- async HTTP client на `httpx`
- auth headers для NeverMine/community-core (`Bearer` + optional HMAC)
- кэш статуса + background refresh
- segmented broadcasts по scope/tags
- scheduled broadcasts + retry + dead letters
- media support: `photo`, `document`, `video`, `animation`
- thread/topic support через `thread=<id>` и `default_thread_id`
- richer chat management: `list`, `bulk`, `default_thread_id`, `disable_notifications`
- incoming announcements feed
- linking foundation + server verification hook
- storage factory с режимами SQLite и PostgreSQL
- schema migrations (`schema_meta`)
- leader locks для multi-instance safety по cleanup/scheduler/feed
- idempotency keys для callback/admin flows
- structured logging (`plain`/`json`) + redaction секретов
- polling и webhook mode
- `--check-config`
- Linux-first запуск без Docker
- Docker и systemd как опции
- русский язык как единственный поддерживаемый язык runtime
- signed push endpoints: `/push/security`, `/push/feed`
- approval workflow: `/approval list|approve|reject`
- внешняя admin control surface через `EXTERNAL_ADMIN_SITE_URL` / `EXTERNAL_ADMIN_API_URL`
- community-core bridges: `COMMUNITY_CORE_EVENT_URL`, `DISCORD_BRIDGE_URL`, `VK_BRIDGE_URL`
- Redis optional backend для rate limits через `REDIS_URL` (зависимость включена в requirements.lock)
- release pipeline: `release_build.py`
- QA smoke scripts: `qa_smoke.py`, `qa_contract_nm_auth.py`
- manifest verify и cutover tooling: `python3 release_build.py --verify-manifest ...`, `python3 db_tools.py pg-smoke`, `python3 db_tools.py cutover-postgres`
- native PostgreSQL storage layer для multi-instance backend через `DATABASE_URL=postgresql://...`
- honest SQLite export bundle tooling: `sqlite_export_bundle.py`

## Быстрый старт

```bash
python3 setup_wizard.py
./bootstrap.sh
./run.sh --check-config
./run.sh --readiness-check
./run.sh
```

## Синтаксис команд

### Announce/Broadcast/Schedule

```text
/announce [media=kind:url] [thread=id] [silent=true] -- <text>
/broadcast [scope=all|current|private|groups] [tags=a,b] [media=kind:url] [thread=id] [silent=true] -- <text>
/schedule at=YYYY-MM-DDTHH:MM [scope=...] [tags=...] [media=kind:url] [thread=id] [silent=true] -- <text>
```

### Schedule / DLQ

- `/schedule list`
- `/schedule cancel <id>`
- `/schedule requeue <id>`
- `/schedule dlq`
- `/schedule resolve <dead_letter_id>`
- `/schedule replay <dead_letter_id>|all`

### Approval / external admin

- `/approval list`
- `/approval approve <id>`
- `/approval reject <id>`
- `/adminsite show`
- `/adminsite push`

### Chat management

- `/chatsettings show`
- `/chatsettings set <key> <value>`
- `/chatsettings list [type=private|group|supergroup|channel] [tag=x]`
- `/chatsettings bulk <key> <value> [type=...] [tag=...]`

### Link management

- `/link pending`
- `/link history [limit]`
- `/link reject <CODE>`
- `/link revoke <user_id>`
- `/link cleanup`

## Важное по storage и BotHost

Поддерживаются два backend-режима:
- `sqlite://<path>` или `SQLITE_PATH=...`
- `postgresql://user:pass@host:port/dbname`

Для BotHost runtime-файлы разделены на два уровня:

- `/app/data` или `DATA_DIR` — постоянные данные конкретного бота: SQLite, логи, backup/export, runtime-state.
- `/app/shared` или `SHARED_DIR` — общее хранилище, доступное только ботам с включённым «Общим хранилищем».

После включения общего хранилища на BotHost нужно сделать редеплой бота, иначе контейнер может не увидеть mount и переменную `SHARED_DIR`.

Рекомендуемые значения для BotHost:

```env
BOT_MODE=webhook
PUBLIC_HTTP_SERVER_URL=https://nmtelegrambot.bothost.ru/
WEBHOOK_URL=https://nmtelegrambot.bothost.ru/
WEBHOOK_LISTEN=0.0.0.0
WEBHOOK_PORT=8080
PORT=8080
HEALTH_HTTP_PORT=0

DATA_DIR=/app/data
SHARED_DIR=/app/shared
NMBOT_RUNTIME_DIR=/app/data
LOG_FILE=logs/nmtelegrambot.log
SQLITE_PATH=storage/nmtelegrambot.db
ARTIFACT_ROOT=artifacts
TEMPLATES_DIR=templates
```

Относительные `LOG_FILE`, `SQLITE_PATH` и `ARTIFACT_ROOT` автоматически резолвятся внутрь `DATA_DIR`, то есть фактически в:
- `/app/data/logs/nmtelegrambot.log`
- `/app/data/storage/nmtelegrambot.db`
- `/app/data/artifacts`

`TEMPLATES_DIR=templates` сначала ищется в `$SHARED_DIR/templates`, затем в bundled `./templates`.
Это позволяет держать общие шаблоны между Telegram/VK/Discord-ботами в `/app/shared/templates`, но не ломает автономный запуск без общего хранилища.

SQLite подходит для одиночного инстанса. PostgreSQL — для multi-instance сценариев.


## Интеграция с NMDiscordBot

Интеграция реализована как signed bridge, без хранения Discord-токена внутри Telegram-бота.

### Discord → Telegram

NMDiscordBot отправляет transport events на HTTP endpoint NMTelegramBot:

```text
POST /internal/discord/event
POST /internal/bridge/event
```

Для этого у NMTelegramBot должен быть включён внутренний HTTP-сервер:

```env
HEALTH_HTTP_LISTEN=127.0.0.1
HEALTH_HTTP_PORT=8090

BRIDGE_INBOUND_HMAC_SECRET=<same-as-NMDiscordBot-OUTBOUND_HMAC_SECRET>
BRIDGE_INBOUND_BEARER_TOKEN=
BRIDGE_INGRESS_STRICT_AUTH=true

BRIDGE_TARGET_CHAT_IDS=-1001234567890
BRIDGE_TARGET_SCOPE=all
BRIDGE_TARGET_TAGS=news,events,devlogs
BRIDGE_ALLOWED_EVENT_TYPES=community.announcement.created,community.devlog.created,community.event.created,community.world_signal.created
```

На стороне NMDiscordBot:

```env
TELEGRAM_BRIDGE_URL=http://127.0.0.1:8090/internal/discord/event
OUTBOUND_HMAC_SECRET=<same-as-NMTelegramBot-BRIDGE_INBOUND_HMAC_SECRET>
OUTBOUND_KEY_ID=v1
BRIDGE_EVENT_RULES_JSON={"community.announcement.created":["telegram"],"community.devlog.created":["telegram"],"community.event.created":["telegram"],"community.world_signal.created":["telegram"]}
```

### Telegram → Discord

NMTelegramBot отправляет события на ingress NMDiscordBot:

```env
DISCORD_BRIDGE_URL=http://127.0.0.1:8080/internal/bridge/event
DISCORD_BRIDGE_HMAC_SECRET=<same-as-NMDiscordBot-INGRESS_HMAC_SECRET>
DISCORD_BRIDGE_BEARER_TOKEN=
OUTBOUND_KEY_ID=v1
```

На стороне NMDiscordBot:

```env
INGRESS_ENABLED=true
INGRESS_HOST=127.0.0.1
INGRESS_PORT=8080
INGRESS_HMAC_SECRET=<same-as-NMTelegramBot-DISCORD_BRIDGE_HMAC_SECRET>
INGRESS_STRICT_AUTH=true
```

Сейчас Telegram → Discord включён для `/announce` и подтверждённого `/broadcast`: они создают `community.announcement.created` и отправляют его в Discord bridge.

### Reverse proxy

Оба endpoint’а лучше держать на `127.0.0.1` и открывать наружу только через Nginx с HTTPS, allowlist и HMAC/Bearer auth.

## Webhook / BotHost HTTP-сервер

Для BotHost используется публичный HTTP-сервер:

```text
https://nmtelegrambot.bothost.ru/
```

Внутренний порт веб-приложения в контейнере:

```text
8080
```

Минимальная конфигурация:

```env
BOT_MODE=webhook
PUBLIC_HTTP_SERVER_URL=https://nmtelegrambot.bothost.ru/
WEBHOOK_URL=https://nmtelegrambot.bothost.ru/
WEBHOOK_LISTEN=0.0.0.0
WEBHOOK_PORT=8080
PORT=8080
HEALTH_HTTP_PORT=0
```

Webhook path строится как:

```text
/<WEBHOOK_PATH_PREFIX>/<bot_id>
```

Итоговый Telegram webhook URL будет иметь вид:

```text
https://nmtelegrambot.bothost.ru/<WEBHOOK_PATH_PREFIX>/<bot_id>
```

При стандартном `WEBHOOK_PATH_PREFIX=telegram`:

```text
https://nmtelegrambot.bothost.ru/telegram/<bot_id>
```

На BotHost не нужно открывать внешний порт вручную: платформа принимает HTTPS-трафик на публичном URL и проксирует его внутрь контейнера на `PORT=8080`.

## Templates

Можно переопределять шаблоны через файлы в `templates/`:

- `templates/start.txt`
- `templates/announcement.txt`
- `templates/feed.txt`


## Bootstrap и запуск

- `./bootstrap.sh` — создаёт `.venv`, ставит зависимости из `requirements.lock` и готовит директории.
- `./run.sh --check-config` — проверяет только конфиг.
- `./run.sh --readiness-check` — проверяет конфиг, директории, templates и backend readiness.
- `./run.sh` — запускает runtime.

## Backup / Export SQLite

```bash
python3 db_tools.py backup
python3 db_tools.py export interactions --format json
python3 db_tools.py export scheduled_broadcasts --format csv --limit 100
```

## Release hygiene

- `.releaseignore` описывает, что не должно попадать в clean release archive.


## HTTP health endpoint

Если в `.env` задан `HEALTH_HTTP_PORT>0`, бот поднимает отдельный HTTP endpoint:

```text
/healthz
/readyz
```

По умолчанию он выключен. Это полезно для hosting-панелей и внешнего мониторинга.


## Ops / Release

Основные команды:
```bash
python3 preflight_check.py --production-archive
python3 db_tools.py restore-drill
python3 db_tools.py pg-smoke --database-url postgresql://user:pass@host:5432/db
python3 release_build.py --verify-manifest release-manifest.json
```

## Режимы сборки

- `python3 release_build.py --mode clean-release` — чистый архив без боевых секретов.
- `python3 release_build.py --mode production-package` — боевой пакет для приватного деплоя.
- `python3 release_build.py --mode cutover-package` — пакет для миграции SQLite -> PostgreSQL.
- `python3 release_build.py --mode rollback-package` — пакет для отката после cutover.

## Cutover SQLite -> PostgreSQL

Основной flow теперь такой:

```bash
python3 db_tools.py pg-smoke --database-url postgresql://user:pass@host:5432/db
python3 db_tools.py cutover-postgres --database-url postgresql://user:pass@host:5432/db --verify
python3 preflight_check.py --production-archive --live-backends
```

## Внешний admin contract

Контракт внешней admin surface формализован в коде (`nmbot/event_contracts.py`) и проверяется через `preflight_check.py`. Отдельные `.md` файлы для этого не требуются.
