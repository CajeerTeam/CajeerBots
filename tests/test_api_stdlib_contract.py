from __future__ import annotations

import json
from pathlib import Path

from core.api import ApiServer
from core.config import Settings
from core.runtime import Runtime


def build_runtime(monkeypatch, tmp_path):
    monkeypatch.setenv("CAJEER_BOTS_RUNTIME_DIR", str(tmp_path / "runtime"))
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "cb_evt_0123456789abcdef0123456789abcdef0123456789abcdef")
    monkeypatch.setenv("API_TOKEN", "cb_api_0123456789abcdef0123456789abcdef0123456789abcdef")
    monkeypatch.setenv("API_TOKEN_READONLY", "cb_read_0123456789abcdef0123456789abcdef0123456789abcdef")
    monkeypatch.setenv("API_TOKEN_METRICS", "cb_metrics_0123456789abcdef0123456789abcdef0123456789abcdef")
    monkeypatch.setenv("MODULES_ENABLED", "identity,rbac,logs,bridge")
    monkeypatch.setenv("PLUGINS_ENABLED", "example_plugin")
    monkeypatch.setenv("EVENT_BUS_BACKEND", "memory")
    monkeypatch.setenv("DELIVERY_BACKEND", "memory")
    monkeypatch.setenv("DEAD_LETTER_BACKEND", "memory")
    monkeypatch.setenv("IDEMPOTENCY_BACKEND", "memory")
    return Runtime(Settings.from_env(), Path.cwd())


def test_stdlib_post_payload_decodes_json_object(monkeypatch, tmp_path):
    api = ApiServer(build_runtime(monkeypatch, tmp_path))
    status, payload, content_type = api._post_payload(
        "/events/publish",
        json.dumps({"source": "test", "type": "test.event", "payload": {"ok": True}}).encode(),
    )
    assert status == 202
    assert payload["ok"] is True
    assert content_type == "application/json"


def test_stdlib_post_payload_rejects_invalid_json(monkeypatch, tmp_path):
    api = ApiServer(build_runtime(monkeypatch, tmp_path))
    status, payload, content_type = api._post_payload("/events/publish", b"{not-json")
    assert status == 400
    assert payload["ok"] is False
    assert payload["error"]["code"] == "invalid_json"
    assert content_type == "application/json"


def test_stdlib_vkontakte_confirmation_returns_plain_confirmation(monkeypatch, tmp_path):
    monkeypatch.setenv("VK_CONFIRMATION_CODE", "vk-confirmation-code")
    api = ApiServer(build_runtime(monkeypatch, tmp_path))
    ok, confirmation = api._vkontakte_webhook_authorized(b'{"type":"confirmation"}')
    assert ok is True
    assert confirmation == "vk-confirmation-code"
