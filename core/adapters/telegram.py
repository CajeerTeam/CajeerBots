from __future__ import annotations

import logging

from core.adapters.base import AdapterCapabilities, BotAdapter
from core.events import CajeerEvent

logger = logging.getLogger(__name__)


class TelegramAdapter(BotAdapter):
    name = "telegram"
    capabilities = AdapterCapabilities(files_receive=True, webhooks=True)

    async def on_start(self) -> None:
        if not self.config.token:
            logger.warning("токен Telegram не задан; адаптер запущен в демонстрационном режиме")
        logger.info("адаптер Telegram запущен")
        await self.publish_event(
            CajeerEvent.create(
                source="telegram",
                type="adapter.started",
                payload={"mode": self.config.extra.get("mode", "polling")},
            )
        )
