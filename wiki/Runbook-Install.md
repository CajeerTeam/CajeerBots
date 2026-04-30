# Runbook: установка

## Назначение

Описывает воспроизводимую установку Cajeer Bots в local/staging/production профилях.

## Local

```bash
cp .env.example .env
./scripts/install.sh
cajeer-bots doctor --offline --profile dev
cajeer-bots run fake
```

## Staging / production-single-node

```bash
./scripts/migrate.sh head
cajeer-bots doctor --profile staging
cajeer-bots run api
cajeer-bots run worker
```

## Проверка

- `GET /livez` — процесс жив.
- `GET /readyz` — зависимости и runtime готовы.
- `cajeer-bots self-test --profile local-memory --offline` — локальная самопроверка.
