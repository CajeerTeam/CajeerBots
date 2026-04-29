from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class IdempotencyStore:
    """Локальное хранилище ключей идемпотентности для одиночного процесса."""

    _seen: set[str] = field(default_factory=set)
    backend: str = "memory"

    def seen(self, key: str) -> bool:
        if key in self._seen:
            return True
        self._seen.add(key)
        return False

    def count(self) -> int:
        return len(self._seen)


class RedisIdempotencyStore(IdempotencyStore):
    def __init__(self, redis_url: str, prefix: str) -> None:
        super().__init__(backend="redis")
        self.redis_url = redis_url
        self.prefix = f"{prefix}:idempotency"


class PostgresIdempotencyStore(IdempotencyStore):
    def __init__(self, async_dsn: str, schema: str = "shared") -> None:
        super().__init__(backend="postgres")
        self.async_dsn = async_dsn
        self.schema = schema


def build_idempotency_store(settings: Any) -> IdempotencyStore:
    backend = settings.storage.idempotency_backend
    if backend == "redis":
        if not settings.redis_url:
            raise RuntimeError("IDEMPOTENCY_BACKEND=redis требует REDIS_URL")
        return RedisIdempotencyStore(settings.redis_url, settings.storage.redis_cache_prefix)
    if backend == "postgres":
        if not settings.storage.async_database_url:
            raise RuntimeError("IDEMPOTENCY_BACKEND=postgres требует DATABASE_ASYNC_URL")
        return PostgresIdempotencyStore(settings.storage.async_database_url, settings.shared_schema)
    return IdempotencyStore()
