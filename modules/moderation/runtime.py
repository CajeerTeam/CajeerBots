from __future__ import annotations

from uuid import uuid4

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
        args = str(event.payload.get("args") or "").strip()
        action_id = "mod_" + uuid4().hex[:12]
        target = args.split()[0] if args else "unknown"
        action = args.split()[1] if len(args.split()) > 1 else "warn"
        context.runtime.audit.write(actor_type="module", actor_id=self.id, action=f"moderation.{action}", resource=target, trace_id=event.trace_id)
        return {"ok": True, "message": f"Модерационное действие {action} зарегистрировано для {target}.", "action_id": action_id, "action": action, "target": target, "trace_id": event.trace_id}

    async def on_stop(self, context) -> None:
        context.logger.info("модуль moderation остановлен")
