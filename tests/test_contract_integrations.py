from __future__ import annotations

import hashlib
import hmac

from core.config import RemoteLogsSettings
from core.integrations.logs import CajeerLogsClient


def test_cajeer_logs_signature_uses_body_digest(monkeypatch):
    monkeypatch.setattr("time.time", lambda: 1000)
    monkeypatch.setattr("core.integrations.logs.uuid4", lambda: "nonce-1")
    settings = RemoteLogsSettings(
        enabled=True,
        url="https://logs.example.local/api/v1/ingest",
        token="secret-token",
        project="CajeerBots",
        bot="CajeerBots",
        environment="test",
        level="INFO",
        batch_size=25,
        flush_interval=5,
        timeout_seconds=5,
        sign_requests=True,
    )
    body = b'{"events":[]}'
    headers = CajeerLogsClient(settings, "test-instance")._headers(body)
    digest = hashlib.sha256(body).hexdigest()
    expected = hmac.new(b"secret-token", f"1000\nnonce-1\n{digest}".encode(), hashlib.sha256).hexdigest()
    assert headers["X-Log-Body-SHA256"] == digest
    assert headers["X-Log-Signature"] == expected


def test_telegram_webhook_mapper_extracts_message_event():
    from bots.telegram.bot.mapper import update_to_event

    event = update_to_event(
        {
            "update_id": 1,
            "message": {
                "message_id": 10,
                "from": {"id": 42, "first_name": "User"},
                "chat": {"id": 100, "type": "private"},
                "text": "/status",
            },
        }
    )
    assert event.source == "telegram"
    assert event.type == "message.received"
    assert event.chat is not None
    assert event.chat.platform_chat_id == "100"
    assert event.payload["text"] == "/status"
