from __future__ import annotations

import asyncio
import inspect
import time
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
