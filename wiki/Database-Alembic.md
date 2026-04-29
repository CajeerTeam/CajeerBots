# SQLAlchemy async и Alembic

Cajeer Bots использует PostgreSQL, SQLAlchemy 2.x async и Alembic.

## Переменные

```env
DATABASE_URL=postgresql://...
DATABASE_ASYNC_URL=postgresql+asyncpg://...
ALEMBIC_CONFIG=alembic.ini
```

## Команды

```bash
cajeer-bots db contract
cajeer-bots db check
alembic upgrade head
```

Базовая ревизия создаёт таблицы:

- `shared.event_bus`
- `shared.delivery_queue`
- `shared.dead_letters`
- `shared.idempotency_keys`
- `shared.audit_log`
