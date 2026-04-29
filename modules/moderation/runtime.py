from __future__ import annotations

from uuid import uuid4

from core.events import CajeerEvent


class ModerationModule:
    id = "moderation"
    allowed_actions = {"warn", "mute", "ban", "kick", "unmute", "unban"}

    async def on_start(self, context) -> None:
        context.logger.info("модуль moderation запущен")

    async def on_event(self, event: CajeerEvent, context) -> dict[str, object] | None:
        return None

    async def on_command(self, command: str, event: CajeerEvent, context) -> dict[str, object] | None:
        if command not in {"moderation", "mod"}:
            return None
        parts = str(event.payload.get("args") or "").strip().split()
        action = parts[0] if parts and parts[0] in self.allowed_actions else "warn"
        target = parts[1] if len(parts) > 1 and parts[0] in self.allowed_actions else (parts[0] if parts else "unknown")
        reason = " ".join(parts[2:] if parts and parts[0] in self.allowed_actions else parts[1:]) or "не указана"
        action_id = "mod_" + uuid4().hex[:12]
        actor = event.actor.platform_user_id if event.actor else "anonymous"
        context.runtime.audit.write(actor_type="module", actor_id=self.id, action=f"moderation.{action}", resource=target, trace_id=event.trace_id, message=f"reason={reason};actor={actor}")
        await context.runtime.workspace.report_event(context.runtime.make_system_event("cajeer.bots.moderation.action", {"action_id": action_id, "action": action, "target": target, "reason": reason, "actor": actor, "trace_id": event.trace_id}))
        return {"ok": True, "message": f"Модерационное действие {action} зарегистрировано для {target}. Причина: {reason}", "action_id": action_id, "action": action, "target": target, "reason": reason, "trace_id": event.trace_id}

    async def on_stop(self, context) -> None:
        context.logger.info("модуль moderation остановлен")
