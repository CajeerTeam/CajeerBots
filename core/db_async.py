from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    metadata = MetaData(schema="shared")


@dataclass
class AsyncDatabase:
    dsn: str

    def engine(self) -> AsyncEngine:
        if not self.dsn:
            raise RuntimeError("DATABASE_ASYNC_URL не задан")
        return create_async_engine(self.dsn, pool_pre_ping=True)

    def session_factory(self) -> async_sessionmaker:
        return async_sessionmaker(self.engine(), expire_on_commit=False)

    async def ping(self) -> None:
        engine = self.engine()
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        finally:
            await engine.dispose()


REQUIRED_TABLES = {
    "event_bus": {"event_id", "trace_id", "source", "event_type", "payload", "status", "created_at", "locked_at", "delivered_at"},
    "delivery_queue": {"delivery_id", "adapter", "target", "payload", "status", "created_at"},
    "dead_letters": {"dead_letter_id", "event_id", "payload", "reason", "created_at"},
    "idempotency_keys": {"key", "created_at", "expires_at"},
    "audit_log": {"audit_id", "actor_type", "actor_id", "action", "resource", "result", "created_at"},
}


async def check_schema(async_dsn: str, schema: str = "shared") -> list[str]:
    if not async_dsn:
        return ["DATABASE_ASYNC_URL не задан"]
    db = AsyncDatabase(async_dsn)
    engine = db.engine()
    problems: list[str] = []
    try:
        async with engine.connect() as conn:
            for table, columns in REQUIRED_TABLES.items():
                result = await conn.execute(
                    text(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = :schema AND table_name = :table
                        """
                    ),
                    {"schema": schema, "table": table},
                )
                found = {row[0] for row in result.fetchall()}
                if not found:
                    problems.append(f"таблица {schema}.{table} не найдена")
                    continue
                missing = sorted(columns - found)
                if missing:
                    problems.append(f"таблица {schema}.{table} не содержит поля: {', '.join(missing)}")
    finally:
        await engine.dispose()
    return problems
