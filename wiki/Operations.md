# Эксплуатация

## Проверка

```bash
cajeer-bots doctor --offline
```

Проверка контролирует структуру проекта, manifest-файлы, compatibility matrix, исполняемые права скриптов, отсутствие устаревших имён пакетов и демонстрационных секретов.

## API

Публичные без токена:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

Остальные маршруты требуют заголовок:

```text
Authorization: Bearer <API_TOKEN>
```

## Метрики

`GET /metrics` возвращает Prometheus-compatible текст со счётчиками runtime, событий, registry и dead letters.

## systemd

Production unit использует console script:

```text
/opt/cajeer-bots/.venv/bin/cajeer-bots run all
```

`python -m core` остаётся допустимым режимом разработки.

## Maintenance cleanup

```bash
cajeer-bots maintenance cleanup
```

Команда очищает локальные runtime-файлы по retention policy и выводит активные настройки:

```env
AUDIT_RETENTION_DAYS=90
DEAD_LETTER_RETENTION_DAYS=30
EVENT_BUS_RETENTION_DAYS=14
DELIVERY_SENT_RETENTION_DAYS=7
UPDATE_HISTORY_RETENTION=200
```

DB-backed retention должен запускаться оператором через SQL/job runner до появления отдельной `maintenance db-cleanup` команды.
