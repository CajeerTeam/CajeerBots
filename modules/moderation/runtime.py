from __future__ import annotations

from core.events import CajeerEvent


class ModerationModule:
    id = "moderation"

    async def on_start(self, context) -> None:
        context.logger.info("модуль moderation запущен")

    async def on_event(self, event: CajeerEvent, context) -> dict[str, object] | None:
        return None

    async def on_command(self, command: str, event: CajeerEvent, context) -> dict[str, object] | None:
        if command != "moderation":
            return None
        return {"ok": True, "message": "Модерация обработана модулем moderation.", "trace_id": event.trace_id}

    async def on_stop(self, context) -> None:
        context.logger.info("модуль moderation остановлен")
