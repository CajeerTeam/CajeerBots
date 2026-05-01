from __future__ import annotations

from core.event_bus import RedisEventBus


def test_redis_event_bus_has_retry_zset_contract(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("EVENT_BUS_BACKEND", "redis")
    from core.config import Settings
    bus = RedisEventBus(Settings.from_env())
    assert bus._retry_zset.endswith(":events:retry")
    assert hasattr(bus, "_drain_due_retries")
