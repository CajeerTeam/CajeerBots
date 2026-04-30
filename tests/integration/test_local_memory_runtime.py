from __future__ import annotations

import asyncio
from pathlib import Path

from core.config import Settings
from core.runtime import Runtime


def test_local_memory_runtime_flow(monkeypatch):
    monkeypatch.setenv("CAJEER_BOTS_ENV", "development")
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "test-secret")
    monkeypatch.setenv("API_TOKEN", "test-token")
    monkeypatch.setenv("EVENT_BUS_BACKEND", "memory")
    monkeypatch.setenv("DELIVERY_BACKEND", "memory")
    monkeypatch.setenv("DEAD_LETTER_BACKEND", "memory")
    monkeypatch.setenv("IDEMPOTENCY_BACKEND", "memory")
    monkeypatch.setenv("TELEGRAM_ENABLED", "false")
    monkeypatch.setenv("DISCORD_ENABLED", "false")
    monkeypatch.setenv("VKONTAKTE_ENABLED", "false")
    monkeypatch.setenv("FAKE_ENABLED", "true")

    runtime = Runtime(Settings.from_env(), project_root=Path.cwd())

    async def scenario():
        task = await runtime.delivery.enqueue_async("fake", "fake-chat", "hello", trace_id="test-trace")
        adapter = runtime.build_adapter("fake")
        processed = await runtime.delivery.process_for_adapter(adapter)
        return task, processed, runtime.delivery.delivered_total

    task, processed, delivered = asyncio.run(scenario())
    assert task.adapter == "fake"
    assert processed == 1
    assert delivered == 1
