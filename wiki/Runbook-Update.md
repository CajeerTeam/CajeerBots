# Runbook: обновление

## Назначение

Фиксирует безопасный путь staged update без ручной замены файлов.

## Процедура

```bash
cajeer-bots update stage dist/CajeerBots-0.10.1.tar.gz
cajeer-bots update apply --version 0.10.1 --dry-run
cajeer-bots update apply --version 0.10.1
```

## Контроль

Перед применением должны пройти `release verify --deep`, Alembic preflight и `doctor`.
