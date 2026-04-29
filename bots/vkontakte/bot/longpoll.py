from __future__ import annotations

from bots.vkontakte.bot.thin import MessageHandler, VkontakteThinWrapper


class VkontakteLongPoll:
    """Long Poll runtime через thin-wrapper."""

    def __init__(self, token: str, api_version: str = "5.199") -> None:
        self.wrapper = VkontakteThinWrapper(token, api_version)

    async def run(self, handler: MessageHandler) -> None:
        await self.wrapper.run_longpoll(handler)
