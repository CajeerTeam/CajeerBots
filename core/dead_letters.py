from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from core.events import CajeerEvent


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


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


@dataclass
class DeadLetterQueue:
    max_size: int = 1000
    _items: list[DeadLetter] = field(default_factory=list)
    backend: str = "memory"

    def add(self, event: CajeerEvent, reason: str) -> None:
        self._items.append(DeadLetter(event, reason, _now(), str(uuid4())))
        self._items = self._items[-self.max_size :]

    def snapshot(self) -> list[DeadLetter]:
        return list(self._items)

    def retry_all(self) -> list[CajeerEvent]:
        events = [item.event for item in self._items]
        self._items.clear()
        return events

    def count(self) -> int:
        return len(self._items)


class RedisDeadLetterQueue(DeadLetterQueue):
    def __init__(self, redis_url: str, prefix: str, max_size: int = 1000) -> None:
        super().__init__(max_size=max_size, backend="redis")
        self.redis_url = redis_url
        self.key = f"{prefix}:dead_letters"


class PostgresDeadLetterQueue(DeadLetterQueue):
    def __init__(self, async_dsn: str, schema: str = "shared", max_size: int = 1000) -> None:
        super().__init__(max_size=max_size, backend="postgres")
        self.async_dsn = async_dsn
        self.schema = schema


def build_dead_letter_queue(settings: Any) -> DeadLetterQueue:
    backend = settings.storage.dead_letter_backend
    if backend == "redis":
        if not settings.redis_url:
            raise RuntimeError("DEAD_LETTER_BACKEND=redis требует REDIS_URL")
        return RedisDeadLetterQueue(settings.redis_url, settings.storage.redis_queue_prefix)
    if backend == "postgres":
        if not settings.storage.async_database_url:
            raise RuntimeError("DEAD_LETTER_BACKEND=postgres требует DATABASE_ASYNC_URL")
        return PostgresDeadLetterQueue(settings.storage.async_database_url, settings.shared_schema)
    return DeadLetterQueue()
