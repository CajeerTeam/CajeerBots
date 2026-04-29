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
