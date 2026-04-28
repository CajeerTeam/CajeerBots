# NMVKBot

VK-бот экосистемы NeverMine для боевого деплоя.

## Что есть сейчас

- запуск через `python main.py`;
- профили `development`, `production`, `bothost`;
- VK Long Poll runtime;
- inbound HTTP bridge с HMAC/Bearer и replay protection;
- outbound queue с retry/backoff, dead-letter и operator tooling;
- support workflow с тикетами, timeline, assign/status/priority/reopen/reply;
- PostgreSQL path для production/shared deployment и SQLite fallback для dev;
- shared storage через `SHARED_DIR` (`attachments`, `exports`, `bridge`, `tmp`, `dead-letter`, `remote-logs`);
- remote logs integration;
- cleanup/retention policy;
- BotHost-aware HTTP profile (`0.0.0.0` + `PORT`).

## Entry point

Канонический запуск:

```bash
python main.py
```

## Важные env

Используй `.env.example` как шаблон. Реальные секреты в архивы и репозитории не кладём.

```env
APP_PROFILE=bothost
BOTHOST_MODE=true
PORT=8080
VK_GROUP_TOKEN=...
VK_GROUP_ID=...
DATABASE_URL=postgresql://user:pass@host:5432/db
SHARED_DIR=/app/shared
REMOTE_LOGS_ENABLED=true
REMOTE_LOGS_URL=https://logs.cajeer.ru/api/v1/ingest
REMOTE_LOGS_TOKEN=...
```

## BotHost notes

- bind должен быть `0.0.0.0`;
- порт — `PORT` или `HEALTH_HTTP_PORT`;
- запуск — через `main.py`;
- для боевого режима рекомендуется внешний PostgreSQL;
- `SHARED_DIR` по умолчанию `/app/shared`.

## Команды

Публичные:
- `!help`
- `!ping`
- `!about`
- `!links`
- `!rules`
- `!id`
- `!support <текст>`

Staff:
- `!announce <текст> [|| attach=photo-1_2,doc-1_3]`
- `!bridge`
- `!tickets [status]`
- `!ticket <ticket_id>`
- `!resolve <ticket_id>`
- `!reopen <ticket_id>`
- `!status <ticket_id> <status>`
- `!priority <ticket_id> <low|normal|high|urgent>`
- `!assign <ticket_id> <vk_user_id>`
- `!comment <ticket_id> <text>`
- `!reply <ticket_id> <text>`
- `!outbox [all|inspect <id>|purge <sent|dead|all>]`
- `!deadletters`
- `!retry <outbox_id|event_id>`
- `!retrydead <outbox_id|event_id>`

## Health

- `GET /healthz`
- `GET /readyz`
- `GET /internal/health/liveness`
- `GET /internal/health/readiness`

Readiness возвращает реальный `ok` и отдаёт `503`, если runtime не готов.

## Retention

Настраивается через:
- `PROCESSED_EVENTS_RETENTION_DAYS`
- `OUTBOUND_SENT_RETENTION_DAYS`
- `OUTBOUND_DEAD_RETENTION_DAYS`
- `CLOSED_TICKET_RETENTION_DAYS`
- `SHARED_FILE_RETENTION_DAYS`

## Deployment checklist

1. Скопируй `.env.example` в `.env`.
2. Заполни VK token/group id, bridge secrets, remote logs и БД.
3. Для BotHost используй `APP_PROFILE=bothost`.
4. Для production не оставляй localhost в `DISCORD_BRIDGE_URL`.
5. Для shared deployment используй внешний PostgreSQL и `SHARED_DIR`.
