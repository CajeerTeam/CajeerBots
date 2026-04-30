# Runbook: Redis

## Назначение

Redis используется для event bus, delivery, idempotency и queues в distributed/staging профилях.

## Проверка

```bash
REDIS_URL=redis://127.0.0.1:6379/0 cajeer-bots self-test --profile staging
```

## Failure drill

Перезапустить Redis, проверить reconnect, lease reclaim и отсутствие дублей через idempotency.
