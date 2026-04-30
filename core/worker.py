from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class WorkerStatus:
    ticks: int = 0
    last_tick_at: str | None = None
    delivery_processed_total: int = 0
    scheduler_processed_total: int = 0
    scheduler_failed_total: int = 0
    dead_letters_retry_total: int = 0
    delivery_adapters: list[str] | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class WorkerService:
    def __init__(self, runtime: object) -> None:
        self.runtime = runtime
        self.status = WorkerStatus()
        self._delivery_adapters: dict[str, object] | None = None
        self._persistent_scheduler = None

    def _delivery_adapter_map(self) -> dict[str, object]:
        if self.runtime.adapter_map():
            return self.runtime.adapter_map()
        if self._delivery_adapters is None:
            adapters: dict[str, object] = {}
            for name in self.runtime.selected_adapters("all"):
                adapter = self.runtime.build_adapter(name)
                if not getattr(adapter.capabilities, "headless_send", False):
                    continue
                if name != "fake" and not adapter.config.token:
                    continue
                adapters[name] = adapter
            self._delivery_adapters = adapters
            self.status.delivery_adapters = sorted(adapters)
        return self._delivery_adapters

    def _scheduler(self):
        if self._persistent_scheduler is None and self.runtime.settings.storage.async_database_url:
            from core.scheduler import PersistentScheduler

            self._persistent_scheduler = PersistentScheduler(
                self.runtime.settings.storage.async_database_url,
                self.runtime.settings.shared_schema,
                self.runtime.settings.instance_id,
            )
        return self._persistent_scheduler

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("режим worker запущен: фоновые задачи активны")
        while not stop_event.is_set():
            await self.tick()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.runtime.settings.worker_tick_seconds)
            except asyncio.TimeoutError:
                continue

    async def tick(self) -> None:
        self.status.ticks += 1
        self.status.last_tick_at = _now()
        self.status.delivery_processed_total += await self.runtime.delivery.process_once(self._delivery_adapter_map())
        scheduler = self._scheduler()
        if scheduler is not None:
            processed, failed = await scheduler.process_due(self.runtime, limit=self.runtime.settings.storage.delivery_claim_limit)
            self.status.scheduler_processed_total += processed
            self.status.scheduler_failed_total += failed
        logger.debug("worker tick выполнен: %s", self.status.to_dict())
