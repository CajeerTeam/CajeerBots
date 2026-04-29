from pathlib import Path

from core.api import ApiServer
from core.config import Settings
from core.runtime import Runtime


def build_runtime(monkeypatch):
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "secret")
    monkeypatch.setenv("API_TOKEN", "token")
    monkeypatch.setenv("MODULES_ENABLED", "identity,rbac,logs,bridge")
    monkeypatch.setenv("PLUGINS_ENABLED", "example_plugin")
    return Runtime(Settings.from_env(), Path.cwd())


def test_api_public_health(monkeypatch):
    api = ApiServer(build_runtime(monkeypatch))
    status, payload, content_type = api._payload("/healthz", authorized=False)
    assert status == 200
    assert payload["ok"] is True
    assert content_type == "application/json"


def test_api_requires_token_for_sensitive_paths(monkeypatch):
    api = ApiServer(build_runtime(monkeypatch))
    status, payload, _ = api._payload("/config/summary", authorized=False)
    assert status == 401
    assert payload["ok"] is False


def test_api_metrics(monkeypatch):
    api = ApiServer(build_runtime(monkeypatch))
    status, payload, content_type = api._payload("/metrics", authorized=False)
    assert status == 200
    assert "cajeerbots_runtime_uptime_seconds" in payload
    assert content_type == "text/plain"
