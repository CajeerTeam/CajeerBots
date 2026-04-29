from __future__ import annotations

import logging
from typing import Any

from core.adapters.base import AdapterCapabilities, BotAdapter
from core.events import message_event

logger = logging.getLogger(__name__)


class VkontakteAdapter(BotAdapter):
    name = "vkontakte"
    capabilities = AdapterCapabilities(files_receive=True)

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
            from vkbottle.bot import Bot, Message
        except ImportError as exc:
            raise RuntimeError("для ВКонтакте установите пакет vkbottle: pip install cajeer-bots[adapters]") from exc

        bot = Bot(token=self.config.token)

        @bot.on.message()
        async def on_message(message: Message) -> None:
            event = message_event(
                source="vkontakte",
                platform_user_id=str(message.from_id),
                platform_chat_id=str(message.peer_id),
                chat_type="conversation",
                text=str(message.text or ""),
                raw={"message_id": message.id, "peer_id": message.peer_id},
            )
            await self.handle_incoming_message(event)

        await bot.run_polling()

    async def send_message(self, target: str, text: str) -> None:
        if not self.config.token:
            return await super().send_message(target, text)
        try:
            from vkbottle.bot import Bot
        except ImportError as exc:
            raise RuntimeError("для ВКонтакте установите пакет vkbottle") from exc
        bot = Bot(token=self.config.token)
        await bot.api.messages.send(peer_id=int(target), message=text, random_id=0)
        await super().send_message(target, text)


class VkontakteThinWrapper:
    """Минимальный seam над vkbottle для тестируемой изоляции API-операций."""

    def __init__(self, token: str, api_version: str = "5.199") -> None:
        self.token = token
        self.api_version = api_version

    async def send_message(self, peer_id: int, text: str) -> dict[str, Any]:
        from vkbottle.bot import Bot

        bot = Bot(token=self.token)
        message_id = await bot.api.messages.send(peer_id=peer_id, message=text, random_id=0)
        return {"ok": True, "message_id": message_id, "peer_id": peer_id}
