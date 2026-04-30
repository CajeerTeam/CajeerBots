from __future__ import annotations

from core.config import Settings


def test_settings_safe_defaults_disable_real_adapters(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    for name in ["TELEGRAM_ENABLED", "DISCORD_ENABLED", "VKONTAKTE_ENABLED", "FAKE_ENABLED", "CAJEER_BOTS_ENV_FILE"]:
        monkeypatch.delenv(name, raising=False)
    settings = Settings.from_env()
    assert settings.adapters["telegram"].enabled is False
    assert settings.adapters["discord"].enabled is False
    assert settings.adapters["vkontakte"].enabled is False
    assert settings.adapters["fake"].enabled is True
