from __future__ import annotations

from uuid import uuid4

from core.events import CajeerEvent


class AnnouncementsModule:
    id = "announcements"

    async def on_start(self, context) -> None:
        context.logger.info("модуль announcements запущен")

    async def on_event(self, event: CajeerEvent, context) -> dict[str, object] | None:
        return None

    async def on_command(self, command: str, event: CajeerEvent, context) -> dict[str, object] | None:
        if command != "announce":
            return None
        args = str(event.payload.get("args") or event.payload.get("text") or "").strip()
        scheduled_at = None
        targets = []
        text = args
        if args.startswith("at "):
            _, _, rest = args.partition(" ")
            scheduled_at, _, text = rest.partition(" ")
        if " --to " in text:
            text, _, raw_targets = text.partition(" --to ")
            targets = [item.strip() for item in raw_targets.split(",") if item.strip()]
        announcement_id = "ann_" + uuid4().hex[:12]
        status = "scheduled" if scheduled_at else "draft"
        context.runtime.audit.write(actor_type="module", actor_id=self.id, action="announcement.create", resource=announcement_id, trace_id=event.trace_id, message=f"status={status};targets={targets}")
        await context.runtime.workspace.report_event(context.runtime.make_system_event("cajeer.bots.announcement.created", {"announcement_id": announcement_id, "status": status, "targets": targets, "scheduled_at": scheduled_at, "trace_id": event.trace_id}))
        return {"ok": True, "message": f"Объявление {announcement_id} создано со статусом {status}.", "announcement_id": announcement_id, "status": status, "text": text, "targets": targets, "scheduled_at": scheduled_at, "trace_id": event.trace_id}

    async def on_stop(self, context) -> None:
        context.logger.info("модуль announcements остановлен")
