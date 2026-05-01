from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass

logger = logging.getLogger(__name__)


@dataclass
class BridgeStatus:
    processed_events: int = 0
    failed_events: int = 0
    skipped_events: int = 0

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


class BridgeService:
    def __init__(self, runtime: object) -> None:
        self.runtime = runtime
        self.status = BridgeStatus()

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("режим bridge запущен: claim/ack маршрутизация событий активна")
        while not stop_event.is_set():
            await self.process_once()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

    async def process_once(self) -> int:
        if not self.runtime.settings.bridge_routing:
            return 0
        claimed_events = await self.runtime.event_bus.claim(limit=100, consumer=self.runtime.settings.instance_id)
        for claimed in claimed_events:
            event = claimed.event
            try:
                result = await self.runtime.router.route(event)
                if result.handler == "idempotency":
                    self.status.skipped_events += 1
                elif not result.handled:
                    logger.info("событие не обработано окончательно: %s", result.to_dict())
                self.status.processed_events += 1
                await self.runtime.event_bus.ack(claimed)
            except Exception as exc:  # pragma: no cover - защитный контур
                self.status.failed_events += 1
                await self.runtime.event_bus.nack(claimed, str(exc), retry=True)
        return len(claimed_events)
