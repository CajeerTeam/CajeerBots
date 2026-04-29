class TelegramWebhook:
    """Каркас webhook-режима Telegram."""

    async def handle(self, payload: dict[str, object]) -> dict[str, object]:
        return {"ok": True, "payload": payload}
