from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any


def _sql_text(statement: str):
    from sqlalchemy import text
    return text(statement)


@dataclass
class IdempotencyStore:
    _seen: set[str] = field(default_factory=set)
    backend: str = "memory"
    ttl_seconds: int = 86400
    def seen(self, key: str) -> bool:
        if key in self._seen: return True
        self._seen.add(key); return False
    def count(self) -> int: return len(self._seen)

class RedisIdempotencyStore(IdempotencyStore):
    def __init__(self, redis_url: str, prefix: str, ttl_seconds: int = 86400) -> None:
        super().__init__(backend="redis", ttl_seconds=ttl_seconds); self.redis_url=redis_url; self.prefix=f"{prefix}:idempotency"
    def seen(self, key: str) -> bool:
        try:
            from redis import Redis  # type: ignore
            return not bool(Redis.from_url(self.redis_url, decode_responses=True).set(f"{self.prefix}:{key}", "1", nx=True, ex=self.ttl_seconds))
        except Exception: return super().seen(key)

class PostgresIdempotencyStore(IdempotencyStore):
    def __init__(self, async_dsn: str, schema: str = "shared", ttl_seconds: int = 86400) -> None:
        super().__init__(backend="postgres", ttl_seconds=ttl_seconds); self.async_dsn=async_dsn; self.schema=schema; self._engine: Any|None=None
    def _engine_obj(self) -> Any:
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine
            self._engine=create_async_engine(self.async_dsn, pool_pre_ping=True)
        return self._engine
    async def seen_async(self, key: str) -> bool:
        async with self._engine_obj().begin() as conn:
            row=(await conn.execute(_sql_text(f"""INSERT INTO {self.schema}.idempotency_keys(key, created_at, expires_at)
            VALUES (:key, NOW(), NOW() + (:ttl || ' seconds')::interval)
            ON CONFLICT (key) DO NOTHING RETURNING key"""), {"key":key,"ttl":self.ttl_seconds})).first()
            return row is None
    def seen(self, key: str) -> bool:
        try: return asyncio.run(self.seen_async(key))
        except RuntimeError:
            if key in self._seen: return True
            self._seen.add(key); asyncio.get_running_loop().create_task(self.seen_async(key)); return False
        except Exception: return super().seen(key)

def build_idempotency_store(settings: Any) -> IdempotencyStore:
    ttl=getattr(settings.storage,"idempotency_ttl_seconds",86400)
    if settings.storage.idempotency_backend=="redis":
        if not settings.redis_url: raise RuntimeError("IDEMPOTENCY_BACKEND=redis требует REDIS_URL")
        return RedisIdempotencyStore(settings.redis_url, settings.storage.redis_cache_prefix, ttl)
    if settings.storage.idempotency_backend=="postgres":
        if not settings.storage.async_database_url: raise RuntimeError("IDEMPOTENCY_BACKEND=postgres требует DATABASE_ASYNC_URL")
        return PostgresIdempotencyStore(settings.storage.async_database_url, settings.shared_schema, ttl)
    return IdempotencyStore(ttl_seconds=ttl)
