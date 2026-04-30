# Runbook: откат

## Назначение

Описывает возврат на previous release при ошибке применения или деградации runtime.

## Процедура

```bash
cajeer-bots update status
cajeer-bots update rollback
cajeer-bots doctor --profile production
```

## Контроль

После отката проверить `/readyz`, delivery backlog, dead letters и audit.
