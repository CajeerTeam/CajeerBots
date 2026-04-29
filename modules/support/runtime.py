from __future__ import annotations

from core.events import CajeerEvent


class SupportModule:
    id = "support"

    async def on_start(self, context) -> None:
        context.logger.info("модуль поддержки запущен")

    async def on_command(self, command: str, event: CajeerEvent, context) -> dict[str, object] | None:
        if command != "support":
            return None
        return {"ok": True, "message": "Обращение принято модулем support", "trace_id": event.trace_id}
