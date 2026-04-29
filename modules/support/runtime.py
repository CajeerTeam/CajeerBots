from __future__ import annotations

from uuid import uuid4

from core.events import CajeerEvent


class SupportModule:
    id = "support"

    async def on_start(self, context) -> None:
        context.logger.info("модуль поддержки запущен")

    async def on_event(self, event: CajeerEvent, context) -> dict[str, object] | None:
        return None

    async def on_command(self, command: str, event: CajeerEvent, context) -> dict[str, object] | None:
        if command != "support":
            return None
        ticket_id = "sup_" + uuid4().hex[:12]
        chat_id = event.chat.platform_chat_id if event.chat else "unknown"
        actor = event.actor.platform_user_id if event.actor else "anonymous"
        context.runtime.audit.write(actor_type="module", actor_id=self.id, action="support.ticket.create", resource=ticket_id, trace_id=event.trace_id, message=f"actor={actor};chat={chat_id}")
        await context.runtime.workspace.report_event(context.runtime.make_system_event("cajeer.bots.support.ticket_created", {"ticket_id": ticket_id, "actor": actor, "chat_id": chat_id, "trace_id": event.trace_id}))
        return {"ok": True, "message": f"Обращение создано: {ticket_id}. Опишите проблему следующим сообщением.", "ticket_id": ticket_id, "status": "open", "trace_id": event.trace_id}
