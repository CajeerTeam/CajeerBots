from __future__ import annotations

import asyncio
import json
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def _sql_text(statement: str):
    from sqlalchemy import text

    return text(statement)


@dataclass(frozen=True)
class AuditRecord:
    audit_id: str
    actor_type: str
    actor_id: str
    action: str
    resource: str
    result: str
    trace_id: str | None
    ip: str | None
    user_agent: str | None
    message: str
    created_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class AuditLog:
    def __init__(self, max_size: int = 2000, *, backend: str = "memory") -> None:
        self._records: deque[AuditRecord] = deque(maxlen=max_size)
        self.backend = backend

    async def write_async(self, record: AuditRecord) -> None:
        self._records.append(record)

    def write(self, *, actor_type: str, actor_id: str, action: str, resource: str, result: str = "ok", trace_id: str | None = None, ip: str | None = None, user_agent: str | None = None, message: str = "") -> AuditRecord:
        record = AuditRecord(str(uuid4()), actor_type, actor_id, action, resource, result, trace_id, ip, user_agent, message, datetime.now(timezone.utc).isoformat())
        self._records.append(record)
        return record

    def snapshot(self) -> list[AuditRecord]:
        return list(self._records)


class RedisAuditLog(AuditLog):
    def __init__(self, redis_url: str, prefix: str, max_size: int = 2000) -> None:
        super().__init__(max_size=max_size, backend="redis")
        self.redis_url = redis_url
        self.key = f"{prefix}:audit"
        self.max_size = max_size
        self._redis: Any | None = None

    async def _client(self) -> Any:
        if self._redis is None:
            from redis.asyncio import Redis  # type: ignore

            self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    async def write_async(self, record: AuditRecord) -> None:
        await super().write_async(record)
        redis = await self._client()
        await redis.lpush(self.key, json.dumps(record.to_dict(), ensure_ascii=False))
        await redis.ltrim(self.key, 0, self.max_size - 1)

    def write(self, **kwargs: Any) -> AuditRecord:
        record = AuditRecord(str(uuid4()), kwargs.get("actor_type", "system"), kwargs.get("actor_id", "unknown"), kwargs.get("action", "unknown"), kwargs.get("resource", "unknown"), kwargs.get("result", "ok"), kwargs.get("trace_id"), kwargs.get("ip"), kwargs.get("user_agent"), kwargs.get("message", ""), datetime.now(timezone.utc).isoformat())
        self._records.append(record)
        try:
            asyncio.get_running_loop().create_task(self.write_async(record))
        except RuntimeError:
            try:
                asyncio.run(self.write_async(record))
            except Exception:
                pass
        return record


class PostgresAuditLog(AuditLog):
    def __init__(self, async_dsn: str, schema: str = "shared", max_size: int = 2000) -> None:
        super().__init__(max_size=max_size, backend="postgres")
        self.async_dsn = async_dsn
        self.schema = schema
        self._engine: Any | None = None

    def _engine_obj(self) -> Any:
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine

            self._engine = create_async_engine(self.async_dsn, pool_pre_ping=True)
        return self._engine

    async def write_async(self, record: AuditRecord) -> None:
        await super().write_async(record)
        async with self._engine_obj().begin() as conn:
            await conn.execute(
                _sql_text(
                    f"""INSERT INTO {self.schema}.audit_log
                    (audit_id, actor_type, actor_id, action, resource, result, trace_id, ip, user_agent, message, created_at)
                    VALUES (:audit_id,:actor_type,:actor_id,:action,:resource,:result,:trace_id,:ip,:user_agent,:message,NOW())
                    ON CONFLICT (audit_id) DO NOTHING"""
                ),
                record.to_dict(),
            )

    def write(self, **kwargs: Any) -> AuditRecord:
        record = AuditRecord(str(uuid4()), kwargs.get("actor_type", "system"), kwargs.get("actor_id", "unknown"), kwargs.get("action", "unknown"), kwargs.get("resource", "unknown"), kwargs.get("result", "ok"), kwargs.get("trace_id"), kwargs.get("ip"), kwargs.get("user_agent"), kwargs.get("message", ""), datetime.now(timezone.utc).isoformat())
        self._records.append(record)
        try:
            asyncio.get_running_loop().create_task(self.write_async(record))
        except RuntimeError:
            try:
                asyncio.run(self.write_async(record))
            except Exception:
                pass
        return record


def build_audit_log(settings: Any) -> AuditLog:
    if "postgres" in {settings.storage.delivery_backend, settings.storage.dead_letter_backend, settings.storage.idempotency_backend} and settings.storage.async_database_url:
        return PostgresAuditLog(settings.storage.async_database_url, settings.shared_schema)
    if "redis" in {settings.storage.delivery_backend, settings.storage.dead_letter_backend, settings.storage.idempotency_backend} and settings.redis_url:
        return RedisAuditLog(settings.redis_url, settings.storage.redis_queue_prefix)
    return AuditLog()
