from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path

from core.config import Settings
from core.runtime import Runtime
from core.token_registry import ApiTokenRegistry
from core.webhook_security import WebhookReplayGuard, body_digest, verify_optional_hmac


def test_token_registry_stores_hash_only(tmp_path):
    registry = ApiTokenRegistry(tmp_path / "tokens.json")
    token, record = registry.create_token(token_id="test", scopes=["system.read"])
    data = json.loads((tmp_path / "tokens.json").read_text(encoding="utf-8"))
    assert token not in (tmp_path / "tokens.json").read_text(encoding="utf-8")
    assert data["tokens"][0]["sha256"] == record.sha256


def test_webhook_replay_guard_denies_duplicate():
    guard = WebhookReplayGuard(ttl_seconds=60)
    assert guard.check_and_mark("same") is True
    assert guard.check_and_mark("same") is False


def test_optional_hmac_signature():
    secret = "secret"
    body = b'{"ok":true}'
    signature = "sha256=" + hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    assert verify_optional_hmac(secret, {"x-cajeer-signature": signature}, body) is True
    assert verify_optional_hmac(secret, {"x-cajeer-signature": "sha256=bad"}, body) is False
    assert body_digest(body)


def test_production_doctor_blocks_unsafe_defaults(monkeypatch):
    monkeypatch.setenv("CAJEER_BOTS_ENV", "production")
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "prod-secret-value")
    monkeypatch.setenv("API_TOKEN", "prod-admin-token")
    monkeypatch.setenv("API_BIND", "0.0.0.0")
    monkeypatch.setenv("API_BEHIND_REVERSE_PROXY", "false")
    monkeypatch.setenv("METRICS_PUBLIC", "true")
    monkeypatch.setenv("TELEGRAM_ENABLED", "false")
    monkeypatch.setenv("DISCORD_ENABLED", "false")
    monkeypatch.setenv("VKONTAKTE_ENABLED", "false")
    runtime = Runtime(Settings.from_env(), project_root=Path.cwd())
    problems = runtime.doctor(offline=True, profile="production")
    assert any("API_BIND открыт наружу" in item for item in problems)
    assert any("METRICS_PUBLIC=true" in item for item in problems)
