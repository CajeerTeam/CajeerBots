from __future__ import annotations

import logging

from bots.vkontakte.bot.thin import VkontakteThinWrapper
from core.adapters.base import AdapterCapabilities, BotAdapter

logger = logging.getLogger(__name__)


class VkontakteAdapter(BotAdapter):
    name = "vkontakte"
    capabilities = AdapterCapabilities(files_receive=True, webhooks=True)

    def _wrapper(self) -> VkontakteThinWrapper:
        return VkontakteThinWrapper(self.config.token, self.config.extra.get("api_version", "5.199"))

    async def on_start(self) -> None:
        if not self.config.token:
            logger.warning("токен ВКонтакте не задан; адаптер запущен в демонстрационном режиме")
        logger.info("адаптер ВКонтакте запущен через thin-wrapper поверх vkbottle")
        await self.report_lifecycle(
            "adapter.started",
            {"group_id": self.config.extra.get("group_id", ""), "library": "vkbottle", "wrapper": "thin"},
        )

    async def run_loop(self) -> None:
        if not self.config.token:
            return await super().run_loop()
        try:
            wrapper = self._wrapper()
            await wrapper.run_longpoll(self.handle_incoming_message)
        except ImportError as exc:
            raise RuntimeError("для ВКонтакте установите пакет vkbottle: pip install cajeer-bots[adapters]") from exc

    async def send_message(self, target: str, text: str) -> None:
        if not self.config.token:
            return await super().send_message(target, text)
        try:
            await self._wrapper().send_message(int(target), text)
        except ImportError as exc:
            raise RuntimeError("для ВКонтакте установите пакет vkbottle") from exc
        await super().send_message(target, text)
