from __future__ import annotations

import logging

from core.adapters.base import AdapterCapabilities, BotAdapter
from core.events import CajeerEvent

logger = logging.getLogger(__name__)


class VkontakteAdapter(BotAdapter):
    name = "vkontakte"
    capabilities = AdapterCapabilities(files_receive=True)

    async def on_start(self) -> None:
        if not self.config.token:
            logger.warning("токен ВКонтакте не задан; адаптер запущен в демонстрационном режиме")
        logger.info("адаптер ВКонтакте запущен")
        await self.publish_event(
            CajeerEvent.create(
                source="vkontakte",
                type="adapter.started",
                payload={"group_id": self.config.extra.get("group_id", "")},
            )
        )
