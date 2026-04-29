from __future__ import annotations

from core.events import CajeerEvent
from bots.telegram.bot.mapper import update_to_event


class TelegramWebhook:
    """Webhook parser для Telegram updates."""

    async def handle(self, payload: dict[str, object]) -> CajeerEvent:
        return update_to_event(payload)
