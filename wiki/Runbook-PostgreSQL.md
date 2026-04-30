# Runbook: PostgreSQL

## Назначение

Единая база PostgreSQL хранит события, delivery queue, audit, idempotency и scheduled jobs.

## Миграция

```bash
cajeer-bots db upgrade head
cajeer-bots db check
```

## Backup / restore

```bash
cajeer-bots db backup --format custom
cajeer-bots db restore runtime/backups/db/cajeer-bots-YYYYmmddTHHMMSSZ.dump --dry-run
```
