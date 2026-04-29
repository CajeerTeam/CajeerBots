from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from core.events import CajeerEvent


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sql_text(statement: str):
    from sqlalchemy import text

    return text(statement)


@dataclass(frozen=True)
class DeadLetter:
    event: CajeerEvent
    reason: str
    created_at: str
    dead_letter_id: str

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["event"] = self.event.to_dict()
        return data

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "DeadLetter":
        payload = data.get("event") or data.get("payload") or {}
        if isinstance(payload, str):
            payload = json.loads(payload)
        return cls(CajeerEvent.from_dict(payload), str(data.get("reason") or ""), str(data.get("created_at") or _now()), str(data.get("dead_letter_id") or uuid4()))


@dataclass
class DeadLetterQueue:
    max_size: int = 1000
    _items: list[DeadLetter] = field(default_factory=list)
    backend: str = "memory"

    def add(self, event: CajeerEvent, reason: str) -> None:
        self._items.append(DeadLetter(event, reason, _now(), str(uuid4())))
        self._items = self._items[-self.max_size :]

    async def add_async(self, event: CajeerEvent, reason: str) -> None:
        self.add(event, reason)

    def snapshot(self) -> list[DeadLetter]:
        return list(self._items)

    def retry_all(self) -> list[CajeerEvent]:
        events = [item.event for item in self._items]
        self._items.clear()
        return events

    async def retry_all_async(self) -> list[CajeerEvent]:
        return self.retry_all()

    def count(self) -> int:
        return len(self._items)


class RedisDeadLetterQueue(DeadLetterQueue):
    def __init__(self, redis_url: str, prefix: str, max_size: int = 1000) -> None:
        super().__init__(max_size=max_size, backend="redis")
        self.redis_url = redis_url
        self.key = f"{prefix}:dead_letters"
        self._redis: Any | None = None

    async def _client(self) -> Any:
        if self._redis is None:
            from redis.asyncio import Redis  # type: ignore

            self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    async def add_async(self, event: CajeerEvent, reason: str) -> None:
        item = DeadLetter(event, reason, _now(), str(uuid4()))
        self._items.append(item)
        self._items = self._items[-self.max_size :]
        redis = await self._client()
        await redis.lpush(self.key, json.dumps(item.to_dict(), ensure_ascii=False))
        await redis.ltrim(self.key, 0, self.max_size - 1)

    def add(self, event: CajeerEvent, reason: str) -> None:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.add_async(event, reason))
        except RuntimeError:
            asyncio.run(self.add_async(event, reason))

    async def retry_all_async(self) -> list[CajeerEvent]:
        redis = await self._client()
        raw = await redis.lrange(self.key, 0, -1)
        await redis.delete(self.key)
        self._items.clear()
        return [DeadLetter.from_dict(json.loads(x)).event for x in raw]

    def retry_all(self) -> list[CajeerEvent]:
        try:
            return asyncio.run(self.retry_all_async())
        except RuntimeError:
            return super().retry_all()

    def count(self) -> int:
        return len(self._items)


class PostgresDeadLetterQueue(DeadLetterQueue):
    def __init__(self, async_dsn: str, schema: str = "shared", max_size: int = 1000) -> None:
        super().__init__(max_size=max_size, backend="postgres")
        self.async_dsn = async_dsn
        self.schema = schema
        self._engine: Any | None = None

    def _engine_obj(self) -> Any:
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine

            self._engine = create_async_engine(self.async_dsn, pool_pre_ping=True)
        return self._engine

    async def add_async(self, event: CajeerEvent, reason: str) -> None:
        item = DeadLetter(event, reason, _now(), str(uuid4()))
        self._items.append(item)
        self._items = self._items[-self.max_size :]
        async with self._engine_obj().begin() as conn:
            await conn.execute(
                _sql_text(
                    f"""INSERT INTO {self.schema}.dead_letters(dead_letter_id,event_id,trace_id,payload,reason,created_at)
                    VALUES (:dead_letter_id,:event_id,:trace_id,CAST(:payload AS jsonb),:reason,NOW())
                    ON CONFLICT (dead_letter_id) DO NOTHING"""
                ),
                {"dead_letter_id": item.dead_letter_id, "event_id": item.event.event_id, "trace_id": item.event.trace_id, "payload": item.event.to_json(), "reason": item.reason},
            )

    def add(self, event: CajeerEvent, reason: str) -> None:
        try:
            asyncio.get_running_loop().create_task(self.add_async(event, reason))
        except RuntimeError:
            try:
                asyncio.run(self.add_async(event, reason))
            except Exception:
                super().add(event, reason)

    async def retry_all_async(self) -> list[CajeerEvent]:
        async with self._engine_obj().begin() as conn:
            rows = (await conn.execute(_sql_text(f"SELECT dead_letter_id,payload FROM {self.schema}.dead_letters WHERE retried_at IS NULL ORDER BY created_at"))).fetchall()
            ids = [r[0] for r in rows]
            if ids:
                await conn.execute(_sql_text(f"UPDATE {self.schema}.dead_letters SET retried_at=NOW() WHERE dead_letter_id = ANY(:ids)"), {"ids": ids})
        return [CajeerEvent.from_dict(r[1] if isinstance(r[1], dict) else json.loads(str(r[1]))) for r in rows]

    def retry_all(self) -> list[CajeerEvent]:
        try:
            return asyncio.run(self.retry_all_async())
        except Exception:
            return super().retry_all()


def build_dead_letter_queue(settings: Any) -> DeadLetterQueue:
    if settings.storage.dead_letter_backend == "redis":
        if not settings.redis_url:
            raise RuntimeError("DEAD_LETTER_BACKEND=redis требует REDIS_URL")
        return RedisDeadLetterQueue(settings.redis_url, settings.storage.redis_queue_prefix)
    if settings.storage.dead_letter_backend == "postgres":
        if not settings.storage.async_database_url:
            raise RuntimeError("DEAD_LETTER_BACKEND=postgres требует DATABASE_ASYNC_URL")
        return PostgresDeadLetterQueue(settings.storage.async_database_url, settings.shared_schema)
    return DeadLetterQueue()
