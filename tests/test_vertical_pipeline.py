from __future__ import annotations

import asyncio
from pathlib import Path

from core.config import Settings
from core.events import message_event
from core.runtime import Runtime


def test_fake_vertical_command_response_pipeline(monkeypatch):
    async def scenario() -> None:
        runtime = Runtime(Settings.from_env(), Path.cwd())
        adapter = runtime.build_adapter("fake")
        runtime.adapters = [adapter]
        event = message_event(
            source="fake",
            platform_user_id="u1",
            platform_chat_id="chat-1",
            chat_type="test",
            text="/status",
        )
        await adapter.handle_incoming_message(event)
        assert adapter.sent_messages
        assert "Cajeer Bots" in adapter.sent_messages[-1]["text"]
        assert runtime.delivery.delivered_total >= 1
        assert any(item.type == "command.response" for item in runtime.event_bus.snapshot())

    monkeypatch.setenv("EVENT_SIGNING_SECRET", "secret")
    monkeypatch.setenv("API_TOKEN", "token")
    monkeypatch.setenv("API_TOKEN_READONLY", "readonly")
    monkeypatch.setenv("API_TOKEN_METRICS", "metrics")
    monkeypatch.setenv("MODULES_ENABLED", "identity,rbac,logs,bridge")
    monkeypatch.setenv("PLUGINS_ENABLED", "example_plugin")
    monkeypatch.setenv("FAKE_ENABLED", "true")
    asyncio.run(scenario())
