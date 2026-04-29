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
