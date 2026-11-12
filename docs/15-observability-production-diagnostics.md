# Observability / Production Diagnostics

CajeerBots 0.14.0 добавляет рабочий слой наблюдаемости без обязательной внешней веб-панели.

## Что входит

- `observability_metric_samples` — журнал метрик runtime, очередей, delivery, scheduler, backups и diagnostics.
- `observability_probes` — DB-backed проверки PostgreSQL, Redis TCP, произвольного TCP target и local storage.
- `observability_alert_rules` — правила с операторами `gt/gte/lt/lte/eq/neq`.
- `observability_incidents` — открытые и закрытые инциденты.
- `observability_alert_events` — история срабатываний и ручного/автоматического восстановления.

## Worker

`ObservabilityWorker` запускается в `apps/worker` и регулярно выполняет:

1. сбор snapshot-метрик;
2. due probes;
3. evaluation alert rules;
4. запись incidents при срабатывании правил;
5. auto-resolve, если метрика вернулась в норму.

Переменные окружения:

```env
OBSERVABILITY_WORKER_INTERVAL_MS=30000
OBSERVABILITY_RETENTION_DAYS=14
OBSERVABILITY_PROBE_BATCH_SIZE=20
```

## CLI

```bash
cajeer observability summary
cajeer observability collect
cajeer observability samples:list --metric outbox.dead
cajeer observability probes:set --code database.default --title "PostgreSQL primary" --type database
cajeer observability probes:run database.default
cajeer observability alerts:set --code outbox.dead.nonzero --title "Dead outbox" --metric outbox.dead --threshold 1
cajeer observability alerts:evaluate
cajeer observability incidents:list --status open
cajeer observability incidents:resolve alert.outbox.dead.nonzero --reason "Очередь очищена"
```
