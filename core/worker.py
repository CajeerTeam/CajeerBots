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
    dead_letters_retry_total: int = 0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class WorkerService:
    def __init__(self, runtime: object) -> None:
        self.runtime = runtime
        self.status = WorkerStatus()

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
        self.status.delivery_processed_total += await self.runtime.delivery.process_once(self.runtime.adapter_map())
        logger.debug("worker tick выполнен: %s", self.status.to_dict())
