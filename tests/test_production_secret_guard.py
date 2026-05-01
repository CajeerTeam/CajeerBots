from __future__ import annotations

from pathlib import Path

from core.config import Settings
from core.runtime import Runtime


def test_production_doctor_rejects_placeholder_secrets_and_dsns(monkeypatch, tmp_path):
    monkeypatch.setenv("CAJEER_BOTS_ENV", "production")
    monkeypatch.setenv("CAJEER_BOTS_RUNTIME_DIR", str(tmp_path / "runtime"))
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "dev-event-signing-secret-change-before-production")
    monkeypatch.setenv("API_TOKEN", "dev-admin-token-change-before-production")
    monkeypatch.setenv("API_TOKEN_READONLY", "dev-readonly-token-change-before-production")
    monkeypatch.setenv("API_TOKEN_METRICS", "dev-metrics-token-change-before-production")
    monkeypatch.setenv("DATABASE_ASYNC_URL", "postgresql+asyncpg://cajeerbots:change-me@127.0.0.1:5432/cajeerbots")
    monkeypatch.setenv("REDIS_URL", "redis://:change-me@127.0.0.1:6379/0")
    monkeypatch.setenv("EVENT_BUS_BACKEND", "redis")
    monkeypatch.setenv("DELIVERY_BACKEND", "postgres")
    monkeypatch.setenv("DEAD_LETTER_BACKEND", "postgres")
    monkeypatch.setenv("IDEMPOTENCY_BACKEND", "postgres")
    monkeypatch.setenv("TELEGRAM_ENABLED", "false")
    monkeypatch.setenv("DISCORD_ENABLED", "false")
    monkeypatch.setenv("VKONTAKTE_ENABLED", "false")
    runtime = Runtime(Settings.from_env(), project_root=Path.cwd())

    problems = runtime.doctor(offline=True, profile="production")

    assert any("EVENT_SIGNING_SECRET" in item and "placeholder" in item for item in problems)
    assert any("API_TOKEN" in item and "placeholder" in item for item in problems)
    assert any("DATABASE_ASYNC_URL" in item and "placeholder" in item for item in problems)
    assert any("REDIS_URL" in item and "placeholder" in item for item in problems)
