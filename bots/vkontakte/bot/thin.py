from __future__ import annotations

import random
from typing import Any, Awaitable, Callable

from core.events import CajeerEvent, message_event

MessageHandler = Callable[[CajeerEvent], Awaitable[None]]


class VkontakteThinWrapper:
    """Thin-wrapper поверх vkbottle.

    Core adapter зависит только от этого seam-класса. Вся специфика vkbottle скрыта здесь,
    чтобы позже можно было заменить библиотеку без переписывания core/adapters/vkontakte.py.
    """

    def __init__(self, token: str, api_version: str = "5.199") -> None:
        self.token = token
        self.api_version = api_version

    async def run_longpoll(self, handler: MessageHandler) -> None:
        from vkbottle.bot import Bot, Message

        bot = Bot(token=self.token)

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
            await handler(event)

        await bot.run_polling()

    async def send_message(self, peer_id: int, text: str) -> dict[str, Any]:
        from vkbottle.bot import Bot

        bot = Bot(token=self.token)
        message_id = await bot.api.messages.send(peer_id=peer_id, message=text, random_id=random.randint(1, 2_147_483_647))
        return {"ok": True, "message_id": message_id, "peer_id": peer_id}

    async def callback_event(self, payload: dict[str, object]) -> CajeerEvent:
        obj = payload.get("object") or {}
        if not isinstance(obj, dict):
            obj = {}
        message = obj.get("message") or obj
        if not isinstance(message, dict):
            message = {}
        return message_event(
            source="vkontakte",
            platform_user_id=str(message.get("from_id") or ""),
            platform_chat_id=str(message.get("peer_id") or ""),
            chat_type="conversation",
            text=str(message.get("text") or ""),
            raw={"callback": payload},
        )
