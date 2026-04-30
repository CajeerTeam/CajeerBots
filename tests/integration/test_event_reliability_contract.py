from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from core.config import Settings
from core.dead_letters import DeadLetterQueue, PostgresDeadLetterQueue, RedisDeadLetterQueue
from core.event_bus import InMemoryEventBus, PostgresEventBus, RedisEventBus
from core.events import CajeerEvent
from core.idempotency import IdempotencyStore, PostgresIdempotencyStore, RedisIdempotencyStore
from core.runtime import Runtime
from core.scheduler import PersistentScheduler


def _event(event_type: str = "integration.event") -> CajeerEvent:
    return CajeerEvent.create(source="test", type=event_type, payload={"value": 1})


def _base_env(monkeypatch) -> None:
    monkeypatch.setenv("CAJEER_BOTS_ENV", "test")
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "test-secret")
    monkeypatch.setenv("API_TOKEN", "test-token")
    monkeypatch.setenv("API_TOKEN_READONLY", "readonly")
    monkeypatch.setenv("API_TOKEN_METRICS", "metrics")
    monkeypatch.setenv("TELEGRAM_ENABLED", "false")
    monkeypatch.setenv("DISCORD_ENABLED", "false")
    monkeypatch.setenv("VKONTAKTE_ENABLED", "false")
    monkeypatch.setenv("FAKE_ENABLED", "true")


async def _event_bus_claim_ack_nack(bus) -> None:
    first = _event("integration.first")
    await bus.publish(first)
    claimed = await bus.claim(limit=1, consumer="worker-a", lease_seconds=1)
    assert len(claimed) == 1
    assert claimed[0].event.event_id == first.event_id
    await bus.nack(claimed[0], "temporary", retry=True)
    retried = await bus.claim(limit=1, consumer="worker-b", lease_seconds=1)
    assert retried[0].event.event_id == first.event_id
    await bus.ack(retried[0])
    assert bus.metrics().delivered >= 1

    failed = _event("integration.failed")
    await bus.publish(failed)
    claimed_failed = await bus.claim(limit=1, consumer="worker-c", lease_seconds=1)
    await bus.nack(claimed_failed[0], "permanent", retry=False)
    assert bus.metrics().failed >= 1


def test_memory_event_bus_ack_nack_contract():
    asyncio.run(_event_bus_claim_ack_nack(InMemoryEventBus()))


@pytest.mark.skipif(not os.getenv("DATABASE_ASYNC_URL"), reason="DATABASE_ASYNC_URL не задан")
def test_postgres_event_bus_ack_nack_contract(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("EVENT_BUS_BACKEND", "postgres")
    settings = Settings.from_env()
    asyncio.run(_event_bus_claim_ack_nack(PostgresEventBus(settings)))


@pytest.mark.skipif(not os.getenv("REDIS_URL"), reason="REDIS_URL не задан")
def test_redis_event_bus_ack_nack_contract(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("EVENT_BUS_BACKEND", "redis")
    monkeypatch.setenv("REDIS_URL", os.environ["REDIS_URL"])
    settings = Settings.from_env()
    asyncio.run(_event_bus_claim_ack_nack(RedisEventBus(settings)))


async def _idempotency_duplicate_contract(store) -> None:
    assert await store.seen_async("dup-key") is False
    assert await store.seen_async("dup-key") is True
    assert await store.seen_async("other-key") is False


def test_memory_idempotency_duplicate_contract():
    asyncio.run(_idempotency_duplicate_contract(IdempotencyStore()))


@pytest.mark.skipif(not os.getenv("DATABASE_ASYNC_URL"), reason="DATABASE_ASYNC_URL не задан")
def test_postgres_idempotency_duplicate_contract():
    store = PostgresIdempotencyStore(os.environ["DATABASE_ASYNC_URL"], os.getenv("DATABASE_SCHEMA_SHARED", "shared"), ttl_seconds=60)
    asyncio.run(_idempotency_duplicate_contract(store))


@pytest.mark.skipif(not os.getenv("REDIS_URL"), reason="REDIS_URL не задан")
def test_redis_idempotency_duplicate_contract():
    store = RedisIdempotencyStore(os.environ["REDIS_URL"], "cajeer:test", ttl_seconds=60)
    asyncio.run(_idempotency_duplicate_contract(store))


async def _dead_letter_retry_contract(queue) -> None:
    event = _event("integration.dead_letter")
    await queue.add_async(event, "boom")
    assert queue.count() >= 1 or queue.snapshot()
    retried = await queue.retry_all_async()
    assert any(item.event_id == event.event_id for item in retried)


def test_memory_dead_letter_retry_contract():
    asyncio.run(_dead_letter_retry_contract(DeadLetterQueue()))


@pytest.mark.skipif(not os.getenv("DATABASE_ASYNC_URL"), reason="DATABASE_ASYNC_URL не задан")
def test_postgres_dead_letter_retry_contract():
    queue = PostgresDeadLetterQueue(os.environ["DATABASE_ASYNC_URL"], os.getenv("DATABASE_SCHEMA_SHARED", "shared"))
    asyncio.run(_dead_letter_retry_contract(queue))


@pytest.mark.skipif(not os.getenv("REDIS_URL"), reason="REDIS_URL не задан")
def test_redis_dead_letter_retry_contract():
    queue = RedisDeadLetterQueue(os.environ["REDIS_URL"], "cajeer:test")
    asyncio.run(_dead_letter_retry_contract(queue))


@pytest.mark.skipif(not os.getenv("DATABASE_ASYNC_URL"), reason="DATABASE_ASYNC_URL не задан")
def test_persistent_scheduler_retry_and_failure_contract(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("DATABASE_ASYNC_URL", os.environ["DATABASE_ASYNC_URL"])
    monkeypatch.setenv("DATABASE_SCHEMA_SHARED", os.getenv("DATABASE_SCHEMA_SHARED", "shared"))

    async def scenario():
        runtime = Runtime(Settings.from_env(), Path.cwd())
        scheduler = PersistentScheduler(os.environ["DATABASE_ASYNC_URL"], os.getenv("DATABASE_SCHEMA_SHARED", "shared"), instance_id="test-worker")
        job_id = await scheduler.upsert_plugin_job({
            "plugin_id": "integration",
            "name": "bad-job",
            "job_type": "unknown",
            "payload": {"trace_id": "scheduler-test"},
            "max_attempts": 1,
        })
        processed, failed = await scheduler.process_due(runtime, limit=1)
        assert processed == 0
        assert failed == 1
        claimed = await scheduler.claim_due(limit=1)
        assert all(str(item.get("job_id")) != job_id for item in claimed)

    asyncio.run(scenario())
