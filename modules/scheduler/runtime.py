from __future__ import annotations

from core.events import CajeerEvent


class SchedulerModule:
    id = "scheduler"

    async def on_start(self, context) -> None:
        context.logger.info("модуль scheduler запущен")

    async def on_event(self, event: CajeerEvent, context) -> dict[str, object] | None:
        return None

    async def on_command(self, command: str, event: CajeerEvent, context) -> dict[str, object] | None:
        if command != "scheduler":
            return None
        return {"ok": True, "message": "Планировщик принял задачу.", "trace_id": event.trace_id}

    async def on_stop(self, context) -> None:
        context.logger.info("модуль scheduler остановлен")
