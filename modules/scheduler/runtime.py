from __future__ import annotations

from uuid import uuid4

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
        args = str(event.payload.get("args") or "").strip()
        job_id = "job_" + uuid4().hex[:12]
        context.runtime.audit.write(actor_type="module", actor_id=self.id, action="scheduler.job.create", resource=job_id, trace_id=event.trace_id, message=args)
        try:
            if context.runtime.settings.storage.async_database_url:
                from datetime import datetime, timezone
                from core.repositories.business import BusinessStateRepository
                await BusinessStateRepository(context.runtime.settings.storage.async_database_url, context.runtime.settings.shared_schema).create_scheduled_job(job_id=job_id, job_type="manual", payload={"args": args, "trace_id": event.trace_id}, run_at=datetime.now(timezone.utc).isoformat())
        except Exception as exc:
            context.logger.warning("ошибка записи состояния в БД: %s", exc)
            context.runtime.audit.write(
                actor_type="module",
                actor_id=self.id,
                action=f"{self.id}.db_write_failed",
                resource=event.trace_id,
                result="error",
                trace_id=event.trace_id,
                message=str(exc),
            )
        return {"ok": True, "message": f"Планировщик принял задачу {job_id}.", "job_id": job_id, "args": args, "trace_id": event.trace_id}

    async def on_stop(self, context) -> None:
        context.logger.info("модуль scheduler остановлен")
