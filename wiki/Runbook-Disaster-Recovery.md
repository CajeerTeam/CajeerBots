# Runbook: Disaster Recovery

## Назначение

Минимальный порядок восстановления после потери процесса, worker, Redis или PostgreSQL.

## Контрольные точки

1. Зафиксировать incident в audit/ops журнале.
2. Проверить `/livez`, `/readyz`, `/status/dependencies`.
3. Восстановить PostgreSQL из последнего backup при необходимости.
4. Перезапустить worker и проверить lease reclaim.
5. Проверить dead letters и выполнить retry.
