from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from core.schema import validate_schema_name


def _sql_text(statement: str):
    from sqlalchemy import text

    return text(statement)


@dataclass
class IdempotencyStore:
    _seen: set[str] = field(default_factory=set)
    backend: str = "memory"
    ttl_seconds: int = 86400

    async def seen_async(self, key: str) -> bool:
        return self.seen(key)

    def seen(self, key: str) -> bool:
        if key in self._seen:
            return True
        self._seen.add(key)
        return False

    def count(self) -> int:
        return len(self._seen)


class RedisIdempotencyStore(IdempotencyStore):
    def __init__(self, redis_url: str, prefix: str, ttl_seconds: int = 86400) -> None:
        super().__init__(backend="redis", ttl_seconds=ttl_seconds)
        self.redis_url = redis_url
        self.prefix = f"{prefix}:idempotency"
        self._redis: Any | None = None

    async def _client(self) -> Any:
        if self._redis is None:
            from redis.asyncio import Redis  # type: ignore

            self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    async def seen_async(self, key: str) -> bool:
        ok = await (await self._client()).set(f"{self.prefix}:{key}", "1", nx=True, ex=self.ttl_seconds)
        return not bool(ok)

    def seen(self, key: str) -> bool:
        try:
            return asyncio.run(self.seen_async(key))
        except RuntimeError:
            if key in self._seen:
                return True
            self._seen.add(key)
            try:
                asyncio.get_running_loop().create_task(self.seen_async(key))
            except RuntimeError:
                pass
            return False
        except Exception:
            return super().seen(key)


class PostgresIdempotencyStore(IdempotencyStore):
    def __init__(self, async_dsn: str, schema: str = "shared", ttl_seconds: int = 86400) -> None:
        super().__init__(backend="postgres", ttl_seconds=ttl_seconds)
        self.async_dsn = async_dsn
        self.schema = validate_schema_name(schema)
        self._engine: Any | None = None

    def _engine_obj(self) -> Any:
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine

            self._engine = create_async_engine(self.async_dsn, pool_pre_ping=True)
        return self._engine

    async def seen_async(self, key: str) -> bool:
        async with self._engine_obj().begin() as conn:
            row = (
                await conn.execute(
                    _sql_text(
                        f"""INSERT INTO {self.schema}.idempotency_keys(key, created_at, expires_at)
                        VALUES (:key, NOW(), NOW() + (:ttl || ' seconds')::interval)
                        ON CONFLICT (key) DO NOTHING RETURNING key"""
                    ),
                    {"key": key, "ttl": self.ttl_seconds},
                )
            ).first()
            return row is None

    def seen(self, key: str) -> bool:
        try:
            return asyncio.run(self.seen_async(key))
        except RuntimeError:
            if key in self._seen:
                return True
            self._seen.add(key)
            try:
                asyncio.get_running_loop().create_task(self.seen_async(key))
            except RuntimeError:
                pass
            return False
        except Exception:
            return super().seen(key)


def build_idempotency_store(settings: Any) -> IdempotencyStore:
    ttl = getattr(settings.storage, "idempotency_ttl_seconds", 86400)
    if settings.storage.idempotency_backend == "redis":
        if not settings.redis_url:
            raise RuntimeError("IDEMPOTENCY_BACKEND=redis требует REDIS_URL")
        return RedisIdempotencyStore(settings.redis_url, settings.storage.redis_cache_prefix, ttl)
    if settings.storage.idempotency_backend == "postgres":
        if not settings.storage.async_database_url:
            raise RuntimeError("IDEMPOTENCY_BACKEND=postgres требует DATABASE_ASYNC_URL")
        return PostgresIdempotencyStore(settings.storage.async_database_url, settings.shared_schema, ttl)
    return IdempotencyStore(ttl_seconds=ttl)



def _payload_value(raw: dict[str, Any], *keys: str) -> Any:
    current: Any = raw
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def platform_idempotency_key(event: Any) -> str | None:
    """Build a stable cross-adapter idempotency key for webhook/polling/longpoll events."""
    source = str(getattr(event, "source", "") or "").lower()
    raw = getattr(event, "raw", None) or getattr(event, "payload", {}) or {}
    if not isinstance(raw, dict):
        raw = {}
    payload = getattr(event, "payload", {}) or {}
    if not isinstance(payload, dict):
        payload = {}

    if source == "telegram":
        update_id = raw.get("update_id") or payload.get("update_id")
        if update_id is not None:
            return f"telegram:update:{update_id}"
        message = raw.get("message") if isinstance(raw.get("message"), dict) else raw
        message_id = (message.get("message_id") if isinstance(message, dict) else None) or payload.get("message_id")
        chat = message.get("chat") if isinstance(message, dict) and isinstance(message.get("chat"), dict) else {}
        chat_id = chat.get("id") or payload.get("chat_id") or getattr(getattr(event, "chat", None), "platform_chat_id", None)
        if chat_id is not None and message_id is not None:
            return f"telegram:message:{chat_id}:{message_id}"

    if source == "discord":
        interaction_id = raw.get("id") or payload.get("interaction_id")
        raw_type = raw.get("type")
        if interaction_id and raw_type is not None:
            return f"discord:interaction:{interaction_id}"
        message_id = raw.get("message_id") or raw.get("id") or payload.get("message_id")
        channel_id = raw.get("channel_id") or payload.get("channel_id") or getattr(getattr(event, "chat", None), "platform_chat_id", None)
        if channel_id is not None and message_id is not None:
            return f"discord:message:{channel_id}:{message_id}"

    if source in {"vkontakte", "vk"}:
        event_id = raw.get("event_id") or raw.get("id") or payload.get("event_id")
        if event_id is not None:
            return f"vk:event:{event_id}"
        obj = raw.get("object") if isinstance(raw.get("object"), dict) else raw
        msg = obj.get("message") if isinstance(obj, dict) and isinstance(obj.get("message"), dict) else obj
        message_id = ((msg.get("id") or msg.get("message_id")) if isinstance(msg, dict) else None) or payload.get("message_id")
        peer_id = (msg.get("peer_id") if isinstance(msg, dict) else None) or payload.get("peer_id") or getattr(getattr(event, "chat", None), "platform_chat_id", None)
        if peer_id is not None and message_id is not None:
            return f"vk:message:{peer_id}:{message_id}"
    return None
