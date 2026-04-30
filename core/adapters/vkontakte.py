from __future__ import annotations

import logging

from typing import Any

from core.imports import import_symbol
from core.adapters.base import AdapterCapabilities, BotAdapter, SendResult

logger = logging.getLogger(__name__)


class VkontakteAdapter(BotAdapter):
    name = "vkontakte"
    capabilities = AdapterCapabilities(files_receive=True, webhooks=True, headless_send=True)

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self._vk_wrapper: Any | None = None

    def _wrapper(self) -> Any:
        if self._vk_wrapper is None:
            wrapper_cls = import_symbol("bots.vkontakte.bot.thin:VkontakteThinWrapper")
            self._vk_wrapper = wrapper_cls(self.config.token, self.config.extra.get("api_version", "5.199"))
        return self._vk_wrapper

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

    async def send_message(self, target: str, text: str) -> SendResult:
        if not self.config.token:
            return await super().send_message(target, text)
        try:
            raw = await self._wrapper().send_message(int(target), text)
        except ImportError as exc:
            raise RuntimeError("для ВКонтакте установите пакет vkbottle") from exc
        await super().send_message(target, text)
        return SendResult(ok=True, platform_message_id=str(raw.get("message_id") or ""), raw=dict(raw))
