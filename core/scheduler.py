from __future__ import annotations

import asyncio
import inspect
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Awaitable, Callable

JobCallable = Callable[[], Awaitable[None] | None]


@dataclass
class ScheduledJob:
    name: str
    interval_seconds: int
    callback: JobCallable
    next_run_at: float = field(default_factory=lambda: time.time())
    last_error: str | None = None
    runs: int = 0


class Scheduler:
    """Лёгкий in-process scheduler для dev/local и фоновых maintenance-задач."""

    def __init__(self) -> None:
        self.jobs: list[ScheduledJob] = []

    def every(self, name: str, interval_seconds: int, callback: JobCallable) -> None:
        self.jobs.append(ScheduledJob(name=name, interval_seconds=max(1, interval_seconds), callback=callback))

    async def run_once(self) -> int:
        now = time.time()
        executed = 0
        for job in self.jobs:
            if job.next_run_at > now:
                continue
            try:
                result = job.callback()
                if inspect.isawaitable(result):
                    await result
                job.runs += 1
                job.last_error = None
                executed += 1
            except Exception as exc:  # pragma: no cover - scheduler must never crash runtime
                job.last_error = str(exc)
            finally:
                job.next_run_at = now + job.interval_seconds
        return executed

    async def run_forever(self, stop_event: asyncio.Event, tick_seconds: int = 1) -> None:
        while not stop_event.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=max(1, tick_seconds))
            except asyncio.TimeoutError:
                pass

    def snapshot(self) -> list[dict[str, object]]:
        return [{"name": job.name, "interval_seconds": job.interval_seconds, "next_run_at": job.next_run_at, "runs": job.runs, "last_error": job.last_error} for job in self.jobs]


class PersistentScheduler:
    """PostgreSQL-backed scheduler loop for production jobs.

    It uses shared.scheduled_jobs and leases due jobs so several workers can run
    without executing the same job concurrently.
    """

    def __init__(self, async_dsn: str, schema: str = "shared", instance_id: str = "cajeer-bots") -> None:
        self.async_dsn = async_dsn
        self.schema = schema
        self.instance_id = instance_id
        self._engine = None

    def _engine_obj(self):
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine
            self._engine = create_async_engine(self.async_dsn, pool_pre_ping=True)
        return self._engine

    async def claim_due(self, limit: int = 10) -> list[dict[str, object]]:
        from sqlalchemy import text
        async with self._engine_obj().begin() as conn:
            rows = (await conn.execute(
                text(
                    f"""WITH picked AS (
                        SELECT job_id FROM {self.schema}.scheduled_jobs
                         WHERE status='pending' AND run_at <= NOW()
                         ORDER BY run_at
                         LIMIT :limit
                         FOR UPDATE SKIP LOCKED
                    )
                    UPDATE {self.schema}.scheduled_jobs j
                       SET status='processing', locked_at=NOW(), locked_by=:instance, attempts=attempts + 1, updated_at=NOW()
                      FROM picked
                     WHERE j.job_id=picked.job_id
                 RETURNING j.job_id, j.job_type, j.payload, j.attempts, j.max_attempts"""
                ),
                {"limit": limit, "instance": self.instance_id},
            )).mappings().all()
        return [dict(row) for row in rows]

    async def mark_completed(self, job_id: str) -> None:
        from sqlalchemy import text
        async with self._engine_obj().begin() as conn:
            await conn.execute(text(f"UPDATE {self.schema}.scheduled_jobs SET status='completed', locked_at=NULL, locked_by=NULL, completed_at=NOW(), updated_at=NOW() WHERE job_id=:job_id"), {"job_id": job_id})

    async def mark_failed(self, job_id: str, error: str, *, retry: bool = True, retry_delay_seconds: int = 60) -> None:
        from sqlalchemy import text
        status = "pending" if retry else "failed"
        failed_at_sql = "NULL" if retry else "NOW()"
        async with self._engine_obj().begin() as conn:
            await conn.execute(
                text(
                    f"""UPDATE {self.schema}.scheduled_jobs
                           SET status=:status, last_error=:error, locked_at=NULL, locked_by=NULL,
                               run_at=CASE WHEN :retry THEN NOW() + (:delay * INTERVAL '1 second') ELSE run_at END,
                               failed_at={failed_at_sql}, updated_at=NOW()
                         WHERE job_id=:job_id"""
                ),
                {"job_id": job_id, "status": status, "error": error, "retry": retry, "delay": retry_delay_seconds},
            )

    async def _dispatch_job(self, runtime, job: dict[str, object]) -> None:
        from core.events import CajeerEvent

        payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
        payload = dict(payload or {})
        job_type = str(job.get("job_type") or "")
        trace_id = str(payload.get("trace_id") or job.get("job_id") or "")
        if job_type in {"delivery.enqueue", "delivery"}:
            await runtime.delivery.enqueue_async(
                adapter=str(payload.get("adapter") or ""),
                target=str(payload.get("target") or ""),
                text=str(payload.get("text") or ""),
                max_attempts=int(payload.get("max_attempts") or 3),
                trace_id=trace_id or None,
            )
            return
        if job_type in {"event.publish", "event"}:
            event = CajeerEvent.create(source=str(payload.get("source") or "scheduler"), type=str(payload.get("type") or "scheduler.event"), payload=dict(payload.get("payload") or {}))
            await runtime.event_bus.publish(event)
            await runtime.router.route(event)
            return
        if job_type in {"command.dispatch", "command", "manual"}:
            args = str(payload.get("args") or payload.get("command") or "").strip()
            command = args.lstrip("/").split(maxsplit=1)[0] if args else "scheduler"
            command_args = args.split(maxsplit=1)[1] if len(args.split(maxsplit=1)) > 1 else ""
            event = CajeerEvent.create(source="scheduler", type="command.received", payload={"command": command, "args": command_args, "job_id": str(job.get("job_id") or "")})
            await runtime.event_bus.publish(event)
            await runtime.router.route(event)
            return
        raise ValueError(f"неизвестный job_type: {job_type}")

    async def process_due(self, runtime, limit: int = 10) -> tuple[int, int]:
        processed = 0
        failed = 0
        for job in await self.claim_due(limit=limit):
            job_id = str(job.get("job_id") or "")
            try:
                await self._dispatch_job(runtime, job)
                await self.mark_completed(job_id)
                processed += 1
                runtime.audit.write(actor_type="system", actor_id="worker", action="scheduler.job.completed", resource=job_id, result="ok")
            except Exception as exc:  # pragma: no cover - worker must continue other jobs
                attempts = int(job.get("attempts") or 1)
                max_attempts = int(job.get("max_attempts") or 3)
                retry = attempts < max_attempts
                await self.mark_failed(job_id, str(exc), retry=retry)
                failed += 1
                runtime.audit.write(actor_type="system", actor_id="worker", action="scheduler.job.failed", resource=job_id, result="error", message=str(exc))
        return processed, failed
