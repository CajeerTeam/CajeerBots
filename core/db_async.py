from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from core.contracts import DB_CONTRACT_VERSION
from core.db_models import Base


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
    "platform_schema": {"component", "version", "updated_at"},
    "event_bus": {"event_id", "trace_id", "source", "event_type", "payload", "status", "created_at", "locked_at", "locked_by", "delivered_at", "attempts", "next_attempt_at", "last_error"},
    "delivery_queue": {"delivery_id", "adapter", "target", "payload", "status", "attempts", "max_attempts", "trace_id", "created_at", "locked_at", "locked_by", "next_attempt_at", "sent_at", "failed_at", "last_error", "rate_limit_bucket"},
    "dead_letters": {"dead_letter_id", "event_id", "trace_id", "payload", "reason", "created_at", "retried_at"},
    "idempotency_keys": {"key", "created_at", "expires_at"},
    "audit_log": {"audit_id", "actor_type", "actor_id", "action", "resource", "result", "trace_id", "ip", "user_agent", "message", "created_at"},
    "adapter_state": {"adapter", "instance_id", "state", "last_error", "updated_at"},
    "users": {"user_id", "display_name", "workspace_user_id", "created_at", "updated_at"},
    "platform_accounts": {"platform", "platform_user_id", "user_id", "username", "display_name", "profile", "created_at", "updated_at"},
    "roles": {"role_id", "title", "source", "created_at"},
    "role_permissions": {"role_id", "permission"},
    "user_roles": {"user_id", "role_id", "granted_at"},
    "support_tickets": {"ticket_id", "user_id", "platform", "platform_chat_id", "status", "subject", "assigned_to", "history", "created_at", "updated_at"},
    "moderation_actions": {"action_id", "platform", "target_id", "action", "reason", "actor_id", "trace_id", "created_at"},
    "announcements": {"announcement_id", "status", "title", "body", "targets", "scheduled_at", "created_at"},
    "user_profiles": {"user_id", "profile", "updated_at"},
    "workspace_links": {"link_id", "user_id", "workspace_user_id", "source", "created_at"},
    "scheduled_jobs": {"job_id", "job_type", "payload", "status", "run_at", "locked_at", "locked_by", "last_error", "created_at"},
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
            schema_result = await conn.execute(
                text(f"SELECT version FROM {schema}.platform_schema WHERE component = 'cajeer-bots-db' LIMIT 1")
            )
            row = schema_result.first()
            if row is not None and row[0] != DB_CONTRACT_VERSION:
                problems.append(f"версия DB contract {row[0]!r} не совпадает с ожидаемой {DB_CONTRACT_VERSION!r}")
    finally:
        await engine.dispose()
    return problems
