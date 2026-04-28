from __future__ import annotations
import asyncio, logging
from cajeer_bots.adapters.base import AdapterCapabilities, BotAdapter
from cajeer_bots.events import CajeerEvent

logger = logging.getLogger(__name__)

class VkontakteAdapter(BotAdapter):
    name = "vkontakte"
    capabilities = AdapterCapabilities(files_receive=True)

    async def start(self) -> None:
        if not self.config.token:
            logger.warning("vkontakte token is empty; adapter starts in dry-run mode")
        logger.info("vkontakte adapter started")
        await self.publish_event(CajeerEvent.create(source="vkontakte", type="adapter.started", payload={"group_id": self.config.extra.get("group_id", "")}))
        while not self._stopping.is_set():
            await asyncio.sleep(5)
