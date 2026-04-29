from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TokenBucket:
    capacity: int
    refill_per_second: float
    tokens: float
    updated_at: float


@dataclass
class MemoryRateLimiter:
    default_capacity: int = 30
    default_rate: float = 30.0
    _buckets: dict[str, TokenBucket] = field(default_factory=dict)

    async def acquire(self, key: str, *, capacity: int | None = None, rate: float | None = None) -> None:
        capacity = capacity or self.default_capacity
        rate = rate or self.default_rate
        while True:
            now = time.monotonic()
            bucket = self._buckets.setdefault(key, TokenBucket(capacity, rate, float(capacity), now))
            elapsed = max(0.0, now - bucket.updated_at)
            bucket.tokens = min(float(bucket.capacity), bucket.tokens + elapsed * bucket.refill_per_second)
            bucket.updated_at = now
            if bucket.tokens >= 1.0:
                bucket.tokens -= 1.0
                return
            await asyncio.sleep(max(0.01, (1.0 - bucket.tokens) / bucket.refill_per_second))


class RedisRateLimiter(MemoryRateLimiter):
    def __init__(self, redis_url: str, prefix: str, *, default_capacity: int = 30, default_rate: float = 30.0) -> None:
        super().__init__(default_capacity=default_capacity, default_rate=default_rate)
        self.redis_url = redis_url
        self.prefix = prefix
        self._redis: Any | None = None

    async def _client(self) -> Any:
        if self._redis is None:
            from redis.asyncio import Redis  # type: ignore

            self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    async def acquire(self, key: str, *, capacity: int | None = None, rate: float | None = None) -> None:
        # Lightweight Redis-backed fixed-window limiter. При недоступности Redis fallback на memory.
        capacity = capacity or self.default_capacity
        window_key = f"{self.prefix}:rate:{key}:{int(time.time())}"
        try:
            redis = await self._client()
            count = await redis.incr(window_key)
            if count == 1:
                await redis.expire(window_key, 2)
            if int(count) <= capacity:
                return
            await asyncio.sleep(1.0)
            return
        except Exception:
            await super().acquire(key, capacity=capacity, rate=rate)


def build_rate_limiter(settings: Any) -> MemoryRateLimiter:
    if settings.redis_url:
        return RedisRateLimiter(settings.redis_url, settings.storage.redis_queue_prefix)
    return MemoryRateLimiter()
