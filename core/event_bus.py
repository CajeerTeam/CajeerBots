from __future__ import annotations

import abc
import json
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any

from core.config import Settings
from core.events import CajeerEvent, validate_event


def _sql_text(statement: str):
    from sqlalchemy import text

    return text(statement)


@dataclass(frozen=True)
class EventBusMetrics:
    backend: str
    published: int
    delivered: int
    failed: int
    stored: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class EventBusBackend(abc.ABC):
    name: str = "abstract"

    @abc.abstractmethod
    async def publish(self, event: CajeerEvent) -> None: ...

    @abc.abstractmethod
    async def drain(self, limit: int = 100) -> list[CajeerEvent]: ...

    @abc.abstractmethod
    def snapshot(self) -> list[CajeerEvent]: ...

    @abc.abstractmethod
    def metrics(self) -> EventBusMetrics: ...


@dataclass
class InMemoryEventBus(EventBusBackend):
    max_size: int = 1000
    name: str = "memory"
    _events: deque[CajeerEvent] = field(default_factory=deque)
    _drain_cursor: int = 0
    _published: int = 0
    _delivered: int = 0
    _failed: int = 0

    async def publish(self, event: CajeerEvent) -> None:
        errors = validate_event(event)
        if errors:
            self._failed += 1
            raise ValueError("; ".join(errors))
        self._events.append(event)
        self._published += 1
        while len(self._events) > self.max_size:
            self._events.popleft()
            self._drain_cursor = max(0, self._drain_cursor - 1)

    async def drain(self, limit: int = 100) -> list[CajeerEvent]:
        events = list(self._events)
        chunk = events[self._drain_cursor : self._drain_cursor + limit]
        self._drain_cursor += len(chunk)
        self._delivered += len(chunk)
        return chunk

    def snapshot(self) -> list[CajeerEvent]:
        return list(self._events)

    def metrics(self) -> EventBusMetrics:
        return EventBusMetrics(self.name, self._published, self._delivered, self._failed, len(self._events))


class PostgresEventBus(EventBusBackend):
    name = "postgres"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._snapshot: deque[CajeerEvent] = deque(maxlen=1000)
        self._published = 0
        self._delivered = 0
        self._failed = 0
        self._engine: Any | None = None

    def _engine_obj(self) -> Any:
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine

            if not self.settings.storage.async_database_url:
                raise RuntimeError("DATABASE_ASYNC_URL не задан для EVENT_BUS_BACKEND=postgres")
            self._engine = create_async_engine(self.settings.storage.async_database_url, pool_pre_ping=True)
        return self._engine

    async def publish(self, event: CajeerEvent) -> None:
        errors = validate_event(event)
        if errors:
            self._failed += 1
            raise ValueError("; ".join(errors))
        try:
            async with self._engine_obj().begin() as conn:
                await conn.execute(_sql_text(f"""INSERT INTO {self.settings.shared_schema}.event_bus
                    (event_id, trace_id, source, event_type, payload, status, created_at)
                    VALUES (:event_id, :trace_id, :source, :event_type, CAST(:payload AS jsonb), 'new', NOW())
                    ON CONFLICT (event_id) DO NOTHING"""),
                    {"event_id": event.event_id, "trace_id": event.trace_id, "source": event.source, "event_type": event.type, "payload": event.to_json()},
                )
            self._snapshot.append(event)
            self._published += 1
        except Exception:
            self._failed += 1
            raise

    async def drain(self, limit: int = 100) -> list[CajeerEvent]:
        try:
            async with self._engine_obj().begin() as conn:
                rows = (await conn.execute(_sql_text(f"""WITH picked AS (
                    SELECT event_id FROM {self.settings.shared_schema}.event_bus WHERE status='new'
                    ORDER BY created_at LIMIT :limit FOR UPDATE SKIP LOCKED)
                    UPDATE {self.settings.shared_schema}.event_bus bus
                    SET status='processing', locked_at=NOW()
                    FROM picked WHERE bus.event_id=picked.event_id
                    RETURNING bus.event_id, bus.payload"""), {"limit": limit})).fetchall()
                ids: list[str] = []
                events: list[CajeerEvent] = []
                for event_id, payload in rows:
                    data = payload if isinstance(payload, dict) else json.loads(str(payload))
                    events.append(CajeerEvent.from_dict(data))
                    ids.append(str(event_id))
                if ids:
                    await conn.execute(_sql_text(f"UPDATE {self.settings.shared_schema}.event_bus SET status='delivered', delivered_at=NOW() WHERE event_id = ANY(:ids)"), {"ids": ids})
            for event in events:
                self._snapshot.append(event)
            self._delivered += len(events)
            return events
        except Exception:
            self._failed += 1
            raise

    def snapshot(self) -> list[CajeerEvent]:
        return list(self._snapshot)

    def metrics(self) -> EventBusMetrics:
        return EventBusMetrics(self.name, self._published, self._delivered, self._failed, len(self._snapshot))


class RedisEventBus(EventBusBackend):
    name = "redis"

    def __init__(self, settings: Settings) -> None:
        if not settings.redis_url:
            raise RuntimeError("REDIS_URL не задан для EVENT_BUS_BACKEND=redis")
        self.settings = settings
        self._redis: Any | None = None
        self._stream = f"{settings.storage.redis_queue_prefix}:events"
        self._dlq = f"{settings.storage.redis_queue_prefix}:events:dlq"
        self._group = "cajeer-bots-bridge"
        self._consumer = settings.instance_id or "cajeer-bots-local"
        self._snapshot: deque[CajeerEvent] = deque(maxlen=1000)
        self._published = 0
        self._delivered = 0
        self._failed = 0
        self._group_ready = False

    async def _client(self) -> Any:
        if self._redis is None:
            from redis.asyncio import Redis  # type: ignore

            self._redis = Redis.from_url(self.settings.redis_url, decode_responses=True)
        if not self._group_ready:
            try:
                await self._redis.xgroup_create(self._stream, self._group, id="0-0", mkstream=True)
            except Exception:
                pass
            self._group_ready = True
        return self._redis

    async def publish(self, event: CajeerEvent) -> None:
        errors = validate_event(event)
        if errors:
            self._failed += 1
            raise ValueError("; ".join(errors))
        await (await self._client()).xadd(self._stream, {"payload": event.to_json(), "event_id": event.event_id, "trace_id": event.trace_id})
        self._snapshot.append(event)
        self._published += 1

    async def drain(self, limit: int = 100) -> list[CajeerEvent]:
        redis = await self._client()
        records = await redis.xreadgroup(self._group, self._consumer, {self._stream: ">"}, count=limit, block=1)
        result: list[CajeerEvent] = []
        ack_ids: list[str] = []
        for _, items in records:
            for item_id, fields in items:
                try:
                    event = CajeerEvent.from_dict(json.loads(fields["payload"]))
                    result.append(event)
                    self._snapshot.append(event)
                    ack_ids.append(item_id)
                except Exception as exc:  # pragma: no cover
                    self._failed += 1
                    await redis.xadd(self._dlq, {"source_id": item_id, "error": str(exc), **fields})
                    ack_ids.append(item_id)
        if ack_ids:
            await redis.xack(self._stream, self._group, *ack_ids)
        self._delivered += len(result)
        return result

    def snapshot(self) -> list[CajeerEvent]:
        return list(self._snapshot)

    def metrics(self) -> EventBusMetrics:
        return EventBusMetrics(self.name, self._published, self._delivered, self._failed, len(self._snapshot))


def build_event_bus(settings: Settings) -> EventBusBackend:
    if settings.event_bus_backend == "postgres":
        return PostgresEventBus(settings)
    if settings.event_bus_backend == "redis":
        return RedisEventBus(settings)
    return InMemoryEventBus()
