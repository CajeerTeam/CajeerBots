from __future__ import annotations
import asyncio, logging
from cajeer_bots.adapters.base import AdapterCapabilities, BotAdapter
from cajeer_bots.events import CajeerEvent

logger = logging.getLogger(__name__)

class DiscordAdapter(BotAdapter):
    name = "discord"
    capabilities = AdapterCapabilities(files_receive=True, roles=True, reactions=True)

    async def start(self) -> None:
        if not self.config.token:
            logger.warning("discord token is empty; adapter starts in dry-run mode")
        logger.info("discord adapter started")
        await self.publish_event(CajeerEvent.create(source="discord", type="adapter.started", payload={"guild_id": self.config.extra.get("guild_id", "")}))
        while not self._stopping.is_set():
            await asyncio.sleep(5)
