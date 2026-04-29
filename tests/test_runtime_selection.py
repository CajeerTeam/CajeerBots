from pathlib import Path

from core.config import Settings
from core.runtime import Runtime


def test_runtime_selects_enabled_adapters(monkeypatch):
    monkeypatch.setenv("TELEGRAM_ENABLED", "true")
    monkeypatch.setenv("DISCORD_ENABLED", "false")
    monkeypatch.setenv("VKONTAKTE_ENABLED", "true")
    runtime = Runtime(Settings.from_env(), Path.cwd())
    assert runtime.selected_adapters("all") == ["telegram", "vkontakte"]
    assert runtime.selected_adapters("discord") == ["discord"]


def test_runtime_metrics_are_prometheus_text(monkeypatch):
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "secret")
    monkeypatch.setenv("API_TOKEN", "token")
    runtime = Runtime(Settings.from_env(), Path.cwd())
    metrics = runtime.metrics_text()
    assert "cajeerbots_events_total" in metrics
