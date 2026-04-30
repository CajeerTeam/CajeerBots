from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TokenBucket:
    capacity: int
    refill_per_second: float
    tokens: float
    updated_at: float


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


@dataclass
class MemoryRateLimiter:
    default_capacity: int = 30
    default_rate: float = 30.0
    adapter_rates: dict[str, float] = field(default_factory=dict)
    _buckets: dict[str, TokenBucket] = field(default_factory=dict)

    def _rate_for_key(self, key: str) -> tuple[int, float]:
        adapter = key.split(":", 1)[0].lower()
        rate = self.adapter_rates.get(adapter, self.default_rate)
        return max(1, int(rate)), max(0.1, float(rate))

    async def acquire(self, key: str, *, capacity: int | None = None, rate: float | None = None) -> None:
        default_capacity, default_rate = self._rate_for_key(key)
        capacity = capacity or default_capacity
        rate = rate or default_rate
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
    LUA_TOKEN_BUCKET = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local refill = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local current = redis.call('HMGET', key, 'tokens', 'updated_at')
local tokens = tonumber(current[1]) or capacity
local updated_at = tonumber(current[2]) or now
local delta = math.max(0, now - updated_at)
tokens = math.min(capacity, tokens + (delta * refill))
local allowed = 0
local wait = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  wait = math.ceil(((1 - tokens) / refill) * 1000)
end
redis.call('HMSET', key, 'tokens', tokens, 'updated_at', now)
redis.call('PEXPIRE', key, ttl)
return {allowed, wait}
"""

    def __init__(self, redis_url: str, prefix: str, *, default_capacity: int = 30, default_rate: float = 30.0, adapter_rates: dict[str, float] | None = None) -> None:
        super().__init__(default_capacity=default_capacity, default_rate=default_rate, adapter_rates=adapter_rates or {})
        self.redis_url = redis_url
        self.prefix = prefix
        self._redis: Any | None = None

    async def _client(self) -> Any:
        if self._redis is None:
            from redis.asyncio import Redis  # type: ignore
            self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    async def acquire(self, key: str, *, capacity: int | None = None, rate: float | None = None) -> None:
        default_capacity, default_rate = self._rate_for_key(key)
        capacity = capacity or default_capacity
        rate = rate or default_rate
        redis_key = f"{self.prefix}:rate:{key}"
        try:
            redis = await self._client()
            while True:
                allowed, wait_ms = await redis.eval(self.LUA_TOKEN_BUCKET, 1, redis_key, time.time(), capacity, rate, max(1000, int((capacity / max(rate, 0.1)) * 2000)))
                if int(allowed) == 1:
                    return
                await asyncio.sleep(max(0.01, int(wait_ms) / 1000.0))
        except Exception:
            await super().acquire(key, capacity=capacity, rate=rate)


def build_rate_limiter(settings: Any) -> MemoryRateLimiter:
    adapter_rates = {
        "telegram": _float_env("TELEGRAM_RATE_LIMIT_GLOBAL_PER_SECOND", 30.0),
        "vkontakte": _float_env("VK_RATE_LIMIT_GLOBAL_PER_SECOND", 20.0),
        "discord": _float_env("DISCORD_RATE_LIMIT_PER_CHANNEL_PER_SECOND", 1.0),
    }
    if settings.redis_url:
        return RedisRateLimiter(settings.redis_url, settings.storage.redis_queue_prefix, adapter_rates=adapter_rates)
    return MemoryRateLimiter(adapter_rates=adapter_rates)
