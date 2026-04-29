from __future__ import annotations

from core.events import CajeerEvent


class LogsModule:
    id = "logs"

    async def on_start(self, context) -> None:
        context.logger.info("модуль logs запущен")

    async def on_event(self, event: CajeerEvent, context) -> dict[str, object] | None:
        return None

    async def on_command(self, command: str, event: CajeerEvent, context) -> dict[str, object] | None:
        if command != "logs":
            return None
        return {"ok": True, "message": "Событие отправлено в Cajeer Logs.", "trace_id": event.trace_id}

    async def on_stop(self, context) -> None:
        context.logger.info("модуль logs остановлен")
