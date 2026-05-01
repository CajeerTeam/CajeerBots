from __future__ import annotations

import asyncio
import json
from collections import Counter, deque
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


STRICT_AUDIT_ACTION_PREFIXES = (
    "rbac.",
    "webhook.",
    "token.",
    "runtime.stop",
    "updates.",
    "catalog.",
)


class AuditLog:
    def __init__(self, max_size: int = 2000, *, backend: str = "memory", mode: str = "best_effort") -> None:
        self._records: deque[AuditRecord] = deque(maxlen=max_size)
        self._counters: Counter[str] = Counter()
        self.backend = backend
        self.mode = mode

    def _strict_for(self, action: str) -> bool:
        return self.mode == "strict" and any(action.startswith(prefix) for prefix in STRICT_AUDIT_ACTION_PREFIXES)

    def _append_memory(self, record: AuditRecord) -> None:
        self._records.append(record)
        self._counters[f"action:{record.action}"] += 1
        if record.action == "rbac.denied":
            self._counters["rbac_denied_total"] += 1
        if record.action.startswith("webhook.") and record.result == "denied":
            self._counters["webhook_rejected_total"] += 1

    async def _write_backend_async(self, record: AuditRecord) -> None:
        return None

    async def write_async(self, record: AuditRecord) -> None:
        self._append_memory(record)
        await self._write_backend_async(record)

    def _record_from_kwargs(self, **kwargs: Any) -> AuditRecord:
        return AuditRecord(
            str(uuid4()),
            kwargs.get("actor_type", "system"),
            kwargs.get("actor_id", "unknown"),
            kwargs.get("action", "unknown"),
            kwargs.get("resource", "unknown"),
            kwargs.get("result", "ok"),
            kwargs.get("trace_id"),
            kwargs.get("ip"),
            kwargs.get("user_agent"),
            kwargs.get("message", ""),
            datetime.now(timezone.utc).isoformat(),
        )

    def write(self, *, actor_type: str, actor_id: str, action: str, resource: str, result: str = "ok", trace_id: str | None = None, ip: str | None = None, user_agent: str | None = None, message: str = "") -> AuditRecord:
        record = self._record_from_kwargs(actor_type=actor_type, actor_id=actor_id, action=action, resource=resource, result=result, trace_id=trace_id, ip=ip, user_agent=user_agent, message=message)
        self._append_memory(record)
        return record

    def snapshot(self) -> list[AuditRecord]:
        return list(self._records)

    def counter(self, name: str) -> int:
        return int(self._counters.get(name, 0))


class RedisAuditLog(AuditLog):
    def __init__(self, redis_url: str, prefix: str, max_size: int = 2000, *, mode: str = "best_effort") -> None:
        super().__init__(max_size=max_size, backend="redis", mode=mode)
        self.redis_url = redis_url
        self.key = f"{prefix}:audit"
        self.max_size = max_size
        self._redis: Any | None = None

    async def _client(self) -> Any:
        if self._redis is None:
            from redis.asyncio import Redis  # type: ignore

            self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    async def _write_backend_async(self, record: AuditRecord) -> None:
        redis = await self._client()
        await redis.lpush(self.key, json.dumps(record.to_dict(), ensure_ascii=False))
        await redis.ltrim(self.key, 0, self.max_size - 1)

    def write(self, **kwargs: Any) -> AuditRecord:
        record = self._record_from_kwargs(**kwargs)
        self._append_memory(record)
        try:
            asyncio.get_running_loop().create_task(self._write_backend_async(record))
        except RuntimeError:
            try:
                asyncio.run(self._write_backend_async(record))
            except Exception:
                if self._strict_for(record.action):
                    raise
        return record


class PostgresAuditLog(AuditLog):
    def __init__(self, async_dsn: str, schema: str = "shared", max_size: int = 2000, *, mode: str = "best_effort", engine: Any | None = None) -> None:
        super().__init__(max_size=max_size, backend="postgres", mode=mode)
        self.async_dsn = async_dsn
        self.schema = schema
        self._engine: Any | None = engine

    def _engine_obj(self) -> Any:
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine

            self._engine = create_async_engine(self.async_dsn, pool_pre_ping=True)
        return self._engine

    async def _write_backend_async(self, record: AuditRecord) -> None:
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
        record = self._record_from_kwargs(**kwargs)
        self._append_memory(record)
        try:
            asyncio.get_running_loop().create_task(self._write_backend_async(record))
        except RuntimeError:
            try:
                asyncio.run(self._write_backend_async(record))
            except Exception:
                if self._strict_for(record.action):
                    raise
        return record


def build_audit_log(settings: Any, db_resources: Any | None = None) -> AuditLog:
    mode = getattr(settings, "audit_mode", "best_effort")
    if "postgres" in {settings.storage.delivery_backend, settings.storage.dead_letter_backend, settings.storage.idempotency_backend} and settings.storage.async_database_url:
        return PostgresAuditLog(settings.storage.async_database_url, settings.shared_schema, mode=mode, engine=(db_resources.async_engine() if db_resources is not None else None))
    if "redis" in {settings.storage.delivery_backend, settings.storage.dead_letter_backend, settings.storage.idempotency_backend} and settings.redis_url:
        return RedisAuditLog(settings.redis_url, settings.storage.redis_queue_prefix, mode=mode)
    return AuditLog(mode=mode)
