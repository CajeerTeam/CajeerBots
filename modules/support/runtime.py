from __future__ import annotations

from uuid import uuid4

from core.events import CajeerEvent


class SupportModule:
    id = "support"

    async def on_start(self, context) -> None:
        context.logger.info("модуль поддержки запущен")

    async def on_event(self, event: CajeerEvent, context) -> dict[str, object] | None:
        return None

    def _deny(self, event: CajeerEvent, context, permission: str) -> dict[str, object]:
        actor = event.actor.platform_user_id if event.actor else "anonymous"
        context.runtime.audit.write(
            actor_type="user",
            actor_id=actor,
            action="rbac.denied",
            resource=permission,
            result="denied",
            trace_id=event.trace_id,
            message=f"module={self.id}",
        )
        return {"ok": False, "error": "permission_denied", "permission": permission, "trace_id": event.trace_id}

    async def on_command(self, command: str, event: CajeerEvent, context) -> dict[str, object] | None:
        if command != "support":
            return None
        args = str(event.payload.get("args") or "").strip()
        parts = args.split()
        subcommand = parts[0] if parts else "create"
        actor = event.actor.platform_user_id if event.actor else "anonymous"
        chat_id = event.chat.platform_chat_id if event.chat else "unknown"
        if subcommand in {"assign", "status"} and len(parts) >= 3:
            permission = "bots.support.assign" if subcommand == "assign" else "bots.support.manage"
            decision = context.runtime.rbac_store.decide(event, permission)
            if not decision.allowed:
                return self._deny(event, context, permission)
            ticket_id = parts[1]
            value = parts[2]
            action = "support.ticket.assign" if subcommand == "assign" else "support.ticket.status"
            context.runtime.audit.write(actor_type="module", actor_id=self.id, action=action, resource=ticket_id, trace_id=event.trace_id, message=f"value={value};actor={actor}")
            try:
                if context.runtime.settings.storage.async_database_url:
                    from core.repositories.business import BusinessStateRepository
                    await BusinessStateRepository(context.runtime.settings.storage.async_database_url, context.runtime.settings.shared_schema).update_support_ticket(ticket_id=ticket_id, status=value if subcommand == "status" else None, assigned_to=value if subcommand == "assign" else None, event={"action": subcommand, "value": value, "actor": actor})
            except Exception as exc:
                context.logger.warning("ошибка записи состояния в БД: %s", exc)
                context.runtime.audit.write(actor_type="module", actor_id=self.id, action=f"{self.id}.db_write_failed", resource=ticket_id, result="error", trace_id=event.trace_id, message=str(exc))
                if context.runtime.settings.support_strict_persistence:
                    return {"ok": False, "error": "persistence_failed", "message": str(exc), "ticket_id": ticket_id, "trace_id": event.trace_id}
            await context.runtime.workspace.report_event(context.runtime.make_system_event(f"cajeer.bots.{action}", {"ticket_id": ticket_id, "value": value, "actor": actor, "trace_id": event.trace_id}))
            return {"ok": True, "message": f"Обращение {ticket_id}: {subcommand}={value}.", "ticket_id": ticket_id, "trace_id": event.trace_id}

        decision = context.runtime.rbac_store.decide(event, "bots.support.create")
        if not decision.allowed and decision.source != "none":
            return self._deny(event, context, "bots.support.create")
        ticket_id = "sup_" + uuid4().hex[:12]
        subject = args if args else "Без темы"
        context.runtime.audit.write(actor_type="module", actor_id=self.id, action="support.ticket.create", resource=ticket_id, trace_id=event.trace_id, message=f"actor={actor};chat={chat_id};subject={subject}")
        try:
            if context.runtime.settings.storage.async_database_url:
                from core.repositories.business import BusinessStateRepository
                await BusinessStateRepository(context.runtime.settings.storage.async_database_url, context.runtime.settings.shared_schema).create_support_ticket(ticket_id=ticket_id, user_id=actor, platform=event.source, platform_chat_id=chat_id, subject=subject, history={"events": [{"action": "create", "actor": actor, "trace_id": event.trace_id}]})
        except Exception as exc:
            context.logger.warning("ошибка записи состояния в БД: %s", exc)
            context.runtime.audit.write(actor_type="module", actor_id=self.id, action=f"{self.id}.db_write_failed", resource=event.trace_id, result="error", trace_id=event.trace_id, message=str(exc))
            if context.runtime.settings.support_strict_persistence:
                return {"ok": False, "error": "persistence_failed", "message": str(exc), "ticket_id": ticket_id, "trace_id": event.trace_id}
        await context.runtime.workspace.report_event(context.runtime.make_system_event("cajeer.bots.support.ticket_created", {"ticket_id": ticket_id, "actor": actor, "chat_id": chat_id, "subject": subject, "trace_id": event.trace_id}))
        return {"ok": True, "message": f"Обращение создано: {ticket_id}. Тема: {subject}", "ticket_id": ticket_id, "status": "open", "subject": subject, "trace_id": event.trace_id}
