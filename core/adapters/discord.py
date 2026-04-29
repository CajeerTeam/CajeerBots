from __future__ import annotations

import logging

from core.adapters.base import AdapterCapabilities, BotAdapter
from core.events import CajeerEvent

logger = logging.getLogger(__name__)


class DiscordAdapter(BotAdapter):
    name = "discord"
    capabilities = AdapterCapabilities(files_receive=True, roles=True, reactions=True)

    async def on_start(self) -> None:
        if not self.config.token:
            logger.warning("токен Discord не задан; адаптер запущен в демонстрационном режиме")
        logger.info("адаптер Discord запущен")
        await self.publish_event(
            CajeerEvent.create(
                source="discord",
                type="adapter.started",
                payload={"guild_id": self.config.extra.get("guild_id", "")},
            )
        )
