from pathlib import Path

from core.api import ApiServer
from core.config import Settings
from core.runtime import Runtime


def build_runtime(monkeypatch):
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "secret")
    monkeypatch.setenv("API_TOKEN", "token")
    monkeypatch.setenv("API_TOKEN_READONLY", "readonly")
    monkeypatch.setenv("API_TOKEN_METRICS", "metrics")
    monkeypatch.setenv("MODULES_ENABLED", "identity,rbac,logs,bridge")
    monkeypatch.setenv("PLUGINS_ENABLED", "example_plugin")
    return Runtime(Settings.from_env(), Path.cwd())


def test_api_public_health(monkeypatch):
    api = ApiServer(build_runtime(monkeypatch))
    status, payload, content_type = api._payload("/healthz")
    assert status == 200
    assert payload["ok"] is True
    assert content_type == "application/json"


def test_api_sensitive_paths_need_token(monkeypatch):
    api = ApiServer(build_runtime(monkeypatch))
    assert api._can_get("/config/summary", None) is False
    assert api._can_get("/config/summary", "readonly") is True


def test_api_metrics_can_be_protected(monkeypatch):
    api = ApiServer(build_runtime(monkeypatch))
    assert api._can_get("/metrics", None) is False
    assert api._can_get("/metrics", "metrics") is True
    status, payload, content_type = api._payload("/metrics")
    assert status == 200
    assert "cajeerbots_runtime_uptime_seconds" in payload
    assert content_type == "text/plain"
