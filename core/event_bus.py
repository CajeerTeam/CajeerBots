from __future__ import annotations

import abc
import json
from collections import deque
from dataclasses import asdict, dataclass, field

from core.config import Settings
from core.events import CajeerEvent, validate_event


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
    async def publish(self, event: CajeerEvent) -> None:
        """Опубликовать событие в шину."""

    @abc.abstractmethod
    async def drain(self, limit: int = 100) -> list[CajeerEvent]:
        """Получить неполученные события для режима bridge."""

    @abc.abstractmethod
    def snapshot(self) -> list[CajeerEvent]:
        """Вернуть последние известные события для API/диагностики."""

    @abc.abstractmethod
    def metrics(self) -> EventBusMetrics:
        """Вернуть диагностические счётчики шины."""


@dataclass
class InMemoryEventBus(EventBusBackend):
    """Локальная шина событий для одиночного процесса и тестов."""

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
    """PostgreSQL backend по внешнему контракту БД."""

    name = "postgres"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._snapshot: deque[CajeerEvent] = deque(maxlen=1000)
        self._published = 0
        self._delivered = 0
        self._failed = 0

    def _connect(self):
        if not self.settings.database_url:
            raise RuntimeError("DATABASE_URL не задан для EVENT_BUS_BACKEND=postgres")
        import psycopg
        return psycopg.connect(self.settings.database_url, sslmode=self.settings.database_sslmode)

    async def publish(self, event: CajeerEvent) -> None:
        errors = validate_event(event)
        if errors:
            self._failed += 1
            raise ValueError("; ".join(errors))
        payload = event.to_json()
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {self.settings.shared_schema}.event_bus
                          (event_id, trace_id, source, event_type, payload, status, created_at)
                        VALUES (%s, %s, %s, %s, %s::jsonb, 'new', NOW())
                        ON CONFLICT (event_id) DO NOTHING
                        """,
                        (event.event_id, event.trace_id, event.source, event.type, payload),
                    )
            self._snapshot.append(event)
            self._published += 1
        except Exception:
            self._failed += 1
            raise

    async def drain(self, limit: int = 100) -> list[CajeerEvent]:
        events: list[CajeerEvent] = []
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        WITH picked AS (
                          SELECT event_id, payload
                          FROM {self.settings.shared_schema}.event_bus
                          WHERE status = 'new'
                          ORDER BY created_at
                          LIMIT %s
                          FOR UPDATE SKIP LOCKED
                        )
                        UPDATE {self.settings.shared_schema}.event_bus AS bus
                        SET status = 'processing', locked_at = NOW()
                        FROM picked
                        WHERE bus.event_id = picked.event_id
                        RETURNING picked.event_id, picked.payload
                        """,
                        (limit,),
                    )
                    rows = cur.fetchall()
                    ids: list[str] = []
                    for event_id, payload in rows:
                        data = payload if isinstance(payload, dict) else json.loads(str(payload))
                        event = CajeerEvent.from_dict(data)
                        events.append(event)
                        ids.append(str(event_id))
                    if ids:
                        cur.execute(
                            f"""
                            UPDATE {self.settings.shared_schema}.event_bus
                            SET status = 'delivered', delivered_at = NOW()
                            WHERE event_id = ANY(%s)
                            """,
                            (ids,),
                        )
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
    """Redis Streams backend через redis.asyncio + consumer groups."""

    name = "redis"

    def __init__(self, settings: Settings) -> None:
        if not settings.redis_url:
            raise RuntimeError("REDIS_URL не задан для EVENT_BUS_BACKEND=redis")
        self.settings = settings
        self._redis = None
        self._stream = f"{settings.storage.redis_queue_prefix}:events"
        self._group = "cajeer-bots-bridge"
        self._consumer = settings.instance_id or "cajeer-bots-local"
        self._snapshot: deque[CajeerEvent] = deque(maxlen=1000)
        self._published = 0
        self._delivered = 0
        self._failed = 0
        self._group_ready = False

    async def _client(self):
        if self._redis is None:
            try:
                from redis.asyncio import Redis  # type: ignore
            except ImportError as exc:
                raise RuntimeError("для EVENT_BUS_BACKEND=redis установите пакет redis") from exc
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
        redis = await self._client()
        await redis.xadd(self._stream, {"payload": event.to_json(), "event_id": event.event_id, "trace_id": event.trace_id})
        self._snapshot.append(event)
        self._published += 1

    async def drain(self, limit: int = 100) -> list[CajeerEvent]:
        redis = await self._client()
        records = await redis.xreadgroup(self._group, self._consumer, {self._stream: ">"}, count=limit, block=1)
        result: list[CajeerEvent] = []
        ack_ids: list[str] = []
        for _, items in records:
            for item_id, fields in items:
                data = json.loads(fields["payload"])
                event = CajeerEvent.from_dict(data)
                result.append(event)
                self._snapshot.append(event)
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
