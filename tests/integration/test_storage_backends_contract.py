from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from core.config import Settings
from core.db_async import check_schema
from core.runtime import Runtime


@pytest.mark.skipif(not os.getenv("DATABASE_ASYNC_URL"), reason="DATABASE_ASYNC_URL не задан")
def test_postgres_schema_contract(monkeypatch):
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "test-secret")
    monkeypatch.setenv("API_TOKEN", "test-token")
    problems = asyncio.run(check_schema(os.environ["DATABASE_ASYNC_URL"], os.getenv("DATABASE_SCHEMA_SHARED", "shared")))
    assert problems == []


@pytest.mark.skipif(not os.getenv("REDIS_URL"), reason="REDIS_URL не задан")
def test_redis_runtime_self_test(monkeypatch):
    monkeypatch.setenv("CAJEER_BOTS_ENV", "development")
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "test-secret")
    monkeypatch.setenv("API_TOKEN", "test-token")
    monkeypatch.setenv("EVENT_BUS_BACKEND", "redis")
    monkeypatch.setenv("DELIVERY_BACKEND", "redis")
    monkeypatch.setenv("DEAD_LETTER_BACKEND", "redis")
    monkeypatch.setenv("IDEMPOTENCY_BACKEND", "redis")
    monkeypatch.setenv("TELEGRAM_ENABLED", "false")
    monkeypatch.setenv("DISCORD_ENABLED", "false")
    monkeypatch.setenv("VKONTAKTE_ENABLED", "false")
    monkeypatch.setenv("FAKE_ENABLED", "true")
    runtime = Runtime(Settings.from_env(), project_root=Path.cwd())
    assert runtime.dependencies_snapshot()["redis_required"] is True
