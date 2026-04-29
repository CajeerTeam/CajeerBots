from __future__ import annotations

from core.events import CajeerEvent


class RbacModule:
    id = "rbac"

    async def on_start(self, context) -> None:
        context.logger.info("модуль rbac запущен")

    async def on_event(self, event: CajeerEvent, context) -> dict[str, object] | None:
        return None

    async def on_command(self, command: str, event: CajeerEvent, context) -> dict[str, object] | None:
        if command != "rbac":
            return None
        permission = str(event.payload.get("args") or event.payload.get("permission") or "*").strip()
        decision = context.runtime.rbac_store.decide(event, permission)
        return {"ok": decision.allowed, "message": "Право разрешено." if decision.allowed else "Право не найдено.", "permission": permission, "decision": decision.to_dict(), "trace_id": event.trace_id}

    async def on_stop(self, context) -> None:
        context.logger.info("модуль rbac остановлен")
