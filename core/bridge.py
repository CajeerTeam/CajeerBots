from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass

logger = logging.getLogger(__name__)


@dataclass
class BridgeStatus:
    processed_events: int = 0
    failed_events: int = 0

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


class BridgeService:
    def __init__(self, runtime: object) -> None:
        self.runtime = runtime
        self.status = BridgeStatus()

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("режим bridge запущен: маршрутизация событий активна")
        while not stop_event.is_set():
            await self.process_once()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

    async def process_once(self) -> int:
        events = await self.runtime.event_bus.drain(limit=100)
        for event in events:
            try:
                result = await self.runtime.router.route(event)
                if not result.handled:
                    logger.info("событие не обработано окончательно: %s", result.to_dict())
                self.status.processed_events += 1
            except Exception as exc:  # pragma: no cover - защитный контур
                self.status.failed_events += 1
                self.runtime.dead_letters.add(event, str(exc))
        return len(events)
