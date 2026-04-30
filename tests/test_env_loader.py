from __future__ import annotations

import os
from pathlib import Path

from core import config
from core.config import Settings


def test_dotenv_is_loaded_automatically(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("CAJEER_BOTS_ENV=test\nFAKE_ENABLED=true\nTELEGRAM_ENABLED=false\nAPI_TOKEN=from-dotenv\nEVENT_SIGNING_SECRET=dotenv-secret\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("API_TOKEN", raising=False)
    monkeypatch.delenv("EVENT_SIGNING_SECRET", raising=False)
    config._DOTENV_LOADED = False
    settings = Settings.from_env()
    assert settings.env == "test"
    assert settings.api_token == "from-dotenv"
    assert settings.adapters["fake"].enabled is True
    assert settings.adapters["telegram"].enabled is False
