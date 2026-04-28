from __future__ import annotations
import asyncio, logging
from cajeer_bots.adapters.base import AdapterCapabilities, BotAdapter
from cajeer_bots.events import CajeerEvent

logger = logging.getLogger(__name__)

class TelegramAdapter(BotAdapter):
    name = "telegram"
    capabilities = AdapterCapabilities(files_receive=True, webhooks=True)

    async def start(self) -> None:
        if not self.config.token:
            logger.warning("telegram token is empty; adapter starts in dry-run mode")
        logger.info("telegram adapter started")
        await self.publish_event(CajeerEvent.create(source="telegram", type="adapter.started", payload={"mode": self.config.extra.get("mode", "polling")}))
        while not self._stopping.is_set():
            await asyncio.sleep(5)
