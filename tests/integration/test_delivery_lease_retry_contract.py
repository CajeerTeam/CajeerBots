from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest

from core.delivery import DeliveryService, PostgresDeliveryService, RedisDeliveryService


class FlakyAdapter:
    name = "fake"

    def __init__(self) -> None:
        self.sent = 0
        self.settings = type("Settings", (), {"instance_id": "test-worker", "storage": type("Storage", (), {"async_database_url": ""})(), "shared_schema": "shared"})()
        self.context = None

    async def send_message(self, target: str, text: str) -> dict[str, object]:
        self.sent += 1
        if self.sent == 1:
            raise RuntimeError("boom")
        return {"platform_message_id": f"msg-{self.sent}"}


def test_memory_delivery_retry_and_lease_reclaim():
    async def scenario():
        service = DeliveryService(retry_backoff_seconds=0, lease_seconds=1)
        task = await service.enqueue_async("fake", "chat", "hello", max_attempts=3)
        first = await service.claim("fake", consumer="worker-1")
        assert first[0].delivery_id == task.delivery_id
        assert first[0].status == "processing"
        again = await service.claim("fake", consumer="worker-2")
        assert again == []
        first[0].locked_at = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        reclaimed = await service.claim("fake", consumer="worker-2")
        assert reclaimed[0].locked_by == "worker-2"
        await service.mark_failed(task.delivery_id, "boom", retry=True)
        retry = await service.claim("fake", consumer="worker-3")
        assert retry[0].attempts == 3
        await service.mark_sent(task.delivery_id)
        assert service.delivered_total == 1

    asyncio.run(scenario())


def test_memory_delivery_process_retries_after_failure():
    async def scenario():
        service = DeliveryService(retry_backoff_seconds=0, lease_seconds=1)
        adapter = FlakyAdapter()
        await service.enqueue_async("fake", "chat", "hello", max_attempts=3)
        assert await service.process_for_adapter(adapter) == 0
        assert service.snapshot()[0].status == "pending"
        assert await service.process_for_adapter(adapter) == 1
        assert service.delivered_total == 1

    asyncio.run(scenario())


@pytest.mark.skipif(not os.getenv("DATABASE_ASYNC_URL"), reason="DATABASE_ASYNC_URL не задан")
def test_postgres_delivery_retry_and_lease_reclaim(monkeypatch):
    async def scenario():
        monkeypatch.setenv("DELIVERY_LEASE_SECONDS", "1")
        service = PostgresDeliveryService(os.environ["DATABASE_ASYNC_URL"], os.getenv("DATABASE_SCHEMA_SHARED", "shared"), retry_backoff_seconds=0)
        service.lease_seconds = 1
        task = await service.enqueue_async("fake", "chat", "hello", max_attempts=3)
        first = await service.claim("fake", consumer="worker-1")
        assert any(item.delivery_id == task.delivery_id for item in first)
        second = await service.claim("fake", consumer="worker-2")
        assert all(item.delivery_id != task.delivery_id for item in second)
        from sqlalchemy import text
        async with service._engine_obj().begin() as conn:
            await conn.execute(text(f"UPDATE {service.schema}.delivery_queue SET locked_at=NOW() - INTERVAL '10 seconds' WHERE delivery_id=:id"), {"id": task.delivery_id})
        reclaimed = await service.claim("fake", consumer="worker-2")
        assert any(item.delivery_id == task.delivery_id and item.locked_by == "worker-2" for item in reclaimed)
        await service.mark_failed(task.delivery_id, "boom", retry=True)
        retry = await service.claim("fake", consumer="worker-3")
        assert any(item.delivery_id == task.delivery_id for item in retry)

    asyncio.run(scenario())


@pytest.mark.skipif(not os.getenv("REDIS_URL"), reason="REDIS_URL не задан")
def test_redis_delivery_retry_flow():
    async def scenario():
        service = RedisDeliveryService(os.environ["REDIS_URL"], "cajeer:test:delivery", retry_backoff_seconds=0)
        service.lease_seconds = 1
        task = await service.enqueue_async("fake", "chat", "hello", max_attempts=3)
        first = await service.claim("fake", consumer="worker-1")
        assert any(item.delivery_id == task.delivery_id for item in first)
        await service.mark_failed(task.delivery_id, "boom", retry=True)
        retry = await service.claim("fake", consumer="worker-2")
        assert any(item.delivery_id == task.delivery_id for item in retry)

    asyncio.run(scenario())
