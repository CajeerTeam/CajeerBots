from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from core.config import Settings


@dataclass
class RedisState:
    settings: Settings

    def _client(self):
        if not self.settings.redis_url:
            raise RuntimeError("REDIS_URL не задан")
        from redis.asyncio import Redis

        return Redis.from_url(self.settings.redis_url, decode_responses=True)

    async def cache_set(self, key: str, value: object, ttl_seconds: int | None = None) -> None:
        redis = self._client()
        await redis.set(f"{self.settings.storage.redis_cache_prefix}:{key}", json.dumps(value, ensure_ascii=False), ex=ttl_seconds)
        await redis.aclose()

    async def cache_get(self, key: str) -> Any:
        redis = self._client()
        value = await redis.get(f"{self.settings.storage.redis_cache_prefix}:{key}")
        await redis.aclose()
        return json.loads(value) if value else None

    async def fsm_set(self, actor_id: str, state: dict[str, object], ttl_seconds: int | None = None) -> None:
        redis = self._client()
        await redis.set(
            f"{self.settings.storage.redis_fsm_prefix}:{actor_id}",
            json.dumps(state, ensure_ascii=False),
            ex=ttl_seconds,
        )
        await redis.aclose()

    async def fsm_get(self, actor_id: str) -> dict[str, object] | None:
        redis = self._client()
        value = await redis.get(f"{self.settings.storage.redis_fsm_prefix}:{actor_id}")
        await redis.aclose()
        return dict(json.loads(value)) if value else None

    async def queue_push(self, queue: str, item: dict[str, object]) -> None:
        redis = self._client()
        await redis.rpush(f"{self.settings.storage.redis_queue_prefix}:{queue}", json.dumps(item, ensure_ascii=False))
        await redis.aclose()

    async def queue_pop(self, queue: str) -> dict[str, object] | None:
        redis = self._client()
        value = await redis.lpop(f"{self.settings.storage.redis_queue_prefix}:{queue}")
        await redis.aclose()
        return dict(json.loads(value)) if value else None
