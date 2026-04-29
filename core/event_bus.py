from __future__ import annotations

import abc
import json
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from core.config import Settings
from core.events import CajeerEvent, validate_event


def _sql_text(statement: str):
    from sqlalchemy import text

    return text(statement)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class ClaimedEvent:
    event: CajeerEvent
    lease_id: str | None = None
    backend_meta: dict[str, object] = field(default_factory=dict)


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
    async def claim(self, limit: int = 100, *, consumer: str | None = None, lease_seconds: int = 60) -> list[ClaimedEvent]: ...

    async def ack(self, claimed: ClaimedEvent | CajeerEvent | str) -> None:
        """Подтвердить обработку события после успешного router/bridge handling."""

    async def nack(self, claimed: ClaimedEvent | CajeerEvent | str, error: str, *, retry: bool = True) -> None:
        """Вернуть событие в очередь или пометить failed после ошибки обработки."""

    async def drain(self, limit: int = 100) -> list[CajeerEvent]:
        # Compatibility mode. Новый код должен использовать claim/ack/nack, чтобы не ACK-ать до обработки.
        return [item.event for item in await self.claim(limit=limit)]

    @abc.abstractmethod
    def snapshot(self) -> list[CajeerEvent]: ...

    @abc.abstractmethod
    def metrics(self) -> EventBusMetrics: ...

    def _event_id(self, claimed: ClaimedEvent | CajeerEvent | str) -> str:
        if isinstance(claimed, ClaimedEvent):
            return claimed.event.event_id
        if isinstance(claimed, CajeerEvent):
            return claimed.event_id
        return str(claimed)


@dataclass
class InMemoryEventBus(EventBusBackend):
    max_size: int = 1000
    name: str = "memory"
    _events: deque[CajeerEvent] = field(default_factory=deque)
    _processing: dict[str, CajeerEvent] = field(default_factory=dict)
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

    async def claim(self, limit: int = 100, *, consumer: str | None = None, lease_seconds: int = 60) -> list[ClaimedEvent]:
        claimed: list[ClaimedEvent] = []
        while self._events and len(claimed) < limit:
            event = self._events.popleft()
            self._processing[event.event_id] = event
            claimed.append(ClaimedEvent(event, lease_id=event.event_id, backend_meta={"consumer": consumer or "memory"}))
        return claimed

    async def ack(self, claimed: ClaimedEvent | CajeerEvent | str) -> None:
        event_id = self._event_id(claimed)
        if self._processing.pop(event_id, None) is not None:
            self._delivered += 1

    async def nack(self, claimed: ClaimedEvent | CajeerEvent | str, error: str, *, retry: bool = True) -> None:
        event_id = self._event_id(claimed)
        event = self._processing.pop(event_id, None)
        if retry and event is not None:
            self._events.appendleft(event)
        else:
            self._failed += 1

    def snapshot(self) -> list[CajeerEvent]:
        return [*list(self._events), *self._processing.values()]

    def metrics(self) -> EventBusMetrics:
        return EventBusMetrics(self.name, self._published, self._delivered, self._failed, len(self._events) + len(self._processing))


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
                await conn.execute(
                    _sql_text(
                        f"""INSERT INTO {self.settings.shared_schema}.event_bus
                        (event_id, trace_id, source, event_type, payload, status, attempts, created_at)
                        VALUES (:event_id, :trace_id, :source, :event_type, CAST(:payload AS jsonb), 'new', 0, NOW())
                        ON CONFLICT (event_id) DO NOTHING"""
                    ),
                    {
                        "event_id": event.event_id,
                        "trace_id": event.trace_id,
                        "source": event.source,
                        "event_type": event.type,
                        "payload": event.to_json(),
                    },
                )
            self._snapshot.append(event)
            self._published += 1
        except Exception:
            self._failed += 1
            raise

    async def claim(self, limit: int = 100, *, consumer: str | None = None, lease_seconds: int = 60) -> list[ClaimedEvent]:
        consumer = consumer or self.settings.instance_id or "cajeer-bots-bridge"
        lease_cutoff = _utcnow() - timedelta(seconds=lease_seconds)
        try:
            async with self._engine_obj().begin() as conn:
                rows = (
                    await conn.execute(
                        _sql_text(
                            f"""WITH picked AS (
                            SELECT event_id FROM {self.settings.shared_schema}.event_bus
                            WHERE status='new'
                               OR (status='processing' AND locked_at IS NOT NULL AND locked_at < :lease_cutoff)
                               OR (status='failed' AND next_attempt_at IS NOT NULL AND next_attempt_at <= NOW())
                            ORDER BY created_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE {self.settings.shared_schema}.event_bus bus
                           SET status='processing', locked_at=NOW(), locked_by=:consumer, attempts=bus.attempts+1
                          FROM picked
                         WHERE bus.event_id=picked.event_id
                     RETURNING bus.event_id, bus.payload"""
                        ),
                        {"limit": limit, "consumer": consumer, "lease_cutoff": lease_cutoff},
                    )
                ).fetchall()
            result: list[ClaimedEvent] = []
            for event_id, payload in rows:
                data = payload if isinstance(payload, dict) else json.loads(str(payload))
                event = CajeerEvent.from_dict(data)
                self._snapshot.append(event)
                result.append(ClaimedEvent(event, lease_id=str(event_id), backend_meta={"consumer": consumer}))
            return result
        except Exception:
            self._failed += 1
            raise

    async def ack(self, claimed: ClaimedEvent | CajeerEvent | str) -> None:
        event_id = self._event_id(claimed)
        async with self._engine_obj().begin() as conn:
            await conn.execute(
                _sql_text(f"UPDATE {self.settings.shared_schema}.event_bus SET status='delivered', delivered_at=NOW(), locked_at=NULL, locked_by=NULL WHERE event_id=:event_id"),
                {"event_id": event_id},
            )
        self._delivered += 1

    async def nack(self, claimed: ClaimedEvent | CajeerEvent | str, error: str, *, retry: bool = True) -> None:
        event_id = self._event_id(claimed)
        base = max(0, int(self.settings.storage.event_bus_retry_backoff_seconds))
        max_backoff = max(1, int(self.settings.storage.event_bus_retry_backoff_max_seconds))
        max_attempts = max(1, int(self.settings.storage.event_bus_max_attempts))
        async with self._engine_obj().begin() as conn:
            row = (
                await conn.execute(
                    _sql_text(f"SELECT attempts, payload, trace_id FROM {self.settings.shared_schema}.event_bus WHERE event_id=:event_id"),
                    {"event_id": event_id},
                )
            ).mappings().first()
            attempts = int(row["attempts"] or 0) if row else 0
            delay = min(max_backoff, base * max(1, 2 ** max(0, attempts - 1)))
            terminal = (not retry) or attempts >= max_attempts
            await conn.execute(
                _sql_text(
                    f"""UPDATE {self.settings.shared_schema}.event_bus
                       SET status='failed',
                           last_error=:error,
                           locked_at=NULL,
                           locked_by=NULL,
                           next_attempt_at=CASE WHEN :terminal THEN NULL ELSE NOW() + (:delay || ' seconds')::interval END
                     WHERE event_id=:event_id"""
                ),
                {"event_id": event_id, "error": error, "delay": delay, "terminal": terminal},
            )
            if terminal and row is not None:
                from uuid import uuid4

                await conn.execute(
                    _sql_text(
                        f"""INSERT INTO {self.settings.shared_schema}.dead_letters(dead_letter_id,event_id,trace_id,payload,reason,created_at)
                            VALUES (:dead_letter_id,:event_id,:trace_id,CAST(:payload AS jsonb),:reason,NOW())
                            ON CONFLICT (dead_letter_id) DO NOTHING"""
                    ),
                    {"dead_letter_id": str(uuid4()), "event_id": event_id, "trace_id": row.get("trace_id"), "payload": json.dumps(row.get("payload") if isinstance(row.get("payload"), dict) else json.loads(str(row.get("payload") or "{}")), ensure_ascii=False), "reason": error},
                )
        self._failed += 1

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

    async def claim(self, limit: int = 100, *, consumer: str | None = None, lease_seconds: int = 60) -> list[ClaimedEvent]:
        redis = await self._client()
        consumer = consumer or self._consumer
        # First reclaim stuck pending messages from crashed consumers, then read fresh ones.
        claimed_records: list[tuple[str, dict[str, str]]] = []
        try:
            reclaimed = await redis.xautoclaim(self._stream, self._group, consumer, lease_seconds * 1000, "0-0", count=limit)
            for item_id, fields in reclaimed[1]:
                claimed_records.append((item_id, fields))
        except Exception:
            pass
        remaining = max(0, limit - len(claimed_records))
        if remaining:
            records = await redis.xreadgroup(self._group, consumer, {self._stream: ">"}, count=remaining, block=1)
            for _, items in records:
                for item_id, fields in items:
                    claimed_records.append((item_id, fields))
        result: list[ClaimedEvent] = []
        for item_id, fields in claimed_records:
            try:
                event = CajeerEvent.from_dict(json.loads(fields["payload"]))
                self._snapshot.append(event)
                result.append(ClaimedEvent(event, lease_id=item_id, backend_meta={"consumer": consumer}))
            except Exception as exc:  # pragma: no cover
                self._failed += 1
                await redis.xadd(self._dlq, {"source_id": item_id, "error": str(exc), **fields})
                await redis.xack(self._stream, self._group, item_id)
        return result

    async def ack(self, claimed: ClaimedEvent | CajeerEvent | str) -> None:
        if isinstance(claimed, ClaimedEvent):
            item_id = claimed.lease_id
        else:
            item_id = str(claimed)
        if item_id:
            await (await self._client()).xack(self._stream, self._group, item_id)
            self._delivered += 1

    async def nack(self, claimed: ClaimedEvent | CajeerEvent | str, error: str, *, retry: bool = True) -> None:
        if isinstance(claimed, ClaimedEvent):
            item_id = claimed.lease_id
            fields = {"event_id": claimed.event.event_id, "payload": claimed.event.to_json(), "error": error}
        else:
            item_id = None
            fields = {"event_id": self._event_id(claimed), "error": error}
        redis = await self._client()
        if not retry:
            await redis.xadd(self._dlq, fields)
        if item_id:
            await redis.xack(self._stream, self._group, item_id)
        self._failed += 1

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
