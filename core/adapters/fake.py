from __future__ import annotations

import asyncio

from core.adapters.base import AdapterCapabilities, BotAdapter
from core.events import message_event


class FakeAdapter(BotAdapter):
    name = "fake"
    capabilities = AdapterCapabilities()

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self.sent_messages: list[dict[str, str]] = []

    async def on_start(self) -> None:
        await self.report_lifecycle("adapter.started", {"library": "fake"})

    async def run_loop(self) -> None:
        script = self.config.extra.get("script") or "/status"
        for line in [item.strip() for item in script.split("|") if item.strip()]:
            event = message_event(
                source="fake",
                platform_user_id="fake-user",
                platform_chat_id="fake-chat",
                chat_type="test",
                display_name="Fake User",
                text=line,
                raw={},
            )
            await self.handle_incoming_message(event)
        while not self._stopping.is_set():
            await asyncio.sleep(1)

    async def send_message(self, target: str, text: str) -> None:
        self.sent_messages.append({"target": target, "text": text})
        await super().send_message(target, text)
