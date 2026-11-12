# Scheduler & Maintenance

CajeerBots `0.11.0` добавляет встроенный DB-backed scheduler. Он нужен для обслуживания платформы без внешней веб-панели: очистка временных данных, возврат зависших задач, Event API retention, diagnostics snapshots и ручные maintenance-запуски через API/CLI.

## Таблицы

- `scheduled_jobs` — описание задач, расписание, payload, lock, nextRunAt.
- `scheduled_job_runs` — история запусков, статус, duration, result/error/logs.
- `maintenance_snapshots` — снимки состояния очередей и maintenance-проверок.

## Расписания

Поддерживаются:

```text
manual
once
at:2026-06-17T12:00:00Z
every:5m
interval:1h
cron:*/15 * * * *
```

Cron поддерживает 5 полей: minute, hour, day-of-month, month, day-of-week.

## Built-in handlers

- `maintenance.cleanup_expired_idempotency` — удаляет просроченные idempotency keys.
- `maintenance.retry_stale_processing` — переводит зависшие processing/sending записи в failed/retry.
- `maintenance.event_retention` — чистит `event_records` по `retentionDays` каждого topic.
- `maintenance.diagnostics_snapshot` — пишет snapshot очередей и dead-состояний.
- `maintenance.cleanup_render_logs` — чистит старые `message_render_logs`.

## CLI

```bash
pnpm cajeer jobs list
pnpm cajeer jobs set --code maintenance.diagnostics_snapshot --title "Diagnostics snapshot" --handler maintenance.diagnostics_snapshot --schedule "cron:*/15 * * * *"
pnpm cajeer jobs run maintenance.diagnostics_snapshot
pnpm cajeer jobs runs:list
pnpm cajeer jobs queue-summary
pnpm cajeer jobs snapshots:list
```

## API

- `GET /api/jobs`
- `PUT /api/jobs`
- `GET /api/jobs/{idOrCode}`
- `POST /api/jobs/{idOrCode}/enable`
- `POST /api/jobs/{idOrCode}/disable`
- `POST /api/jobs/{idOrCode}/run`
- `GET /api/jobs/runs`
- `GET /api/jobs/queue-summary`
- `GET /api/jobs/maintenance/snapshots`

## Worker

`SchedulerWorker` запускается в `apps/worker` вместе с outbox, extension runtime, bot outbound и Event API delivery worker.

Переменные окружения:

```env
SCHEDULER_WORKER_INTERVAL_MS=10000
SCHEDULER_WORKER_BATCH_SIZE=10
SCHEDULER_LOCK_SECONDS=600
```
