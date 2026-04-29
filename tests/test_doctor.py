from pathlib import Path

from core.config import Settings
from core.runtime import Runtime


def test_doctor_offline_accepts_non_placeholder_secrets(monkeypatch):
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "test-secret")
    monkeypatch.setenv("API_TOKEN", "test-token")
    monkeypatch.setenv("MODULES_ENABLED", "identity,rbac,logs,bridge")
    monkeypatch.setenv("PLUGINS_ENABLED", "example_plugin")
    runtime = Runtime(Settings.from_env(), Path.cwd())
    problems = runtime.doctor(offline=True)
    assert not problems
