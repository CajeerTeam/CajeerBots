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
        announcement_id = "ann_" + uuid4().hex[:12]
        text = str(event.payload.get("args") or event.payload.get("text") or "").strip()
        context.runtime.audit.write(actor_type="module", actor_id=self.id, action="announcement.create", resource=announcement_id, trace_id=event.trace_id)
        return {"ok": True, "message": f"Объявление создано как черновик: {announcement_id}.", "announcement_id": announcement_id, "status": "draft", "text": text, "trace_id": event.trace_id}

    async def on_stop(self, context) -> None:
        context.logger.info("модуль announcements остановлен")
