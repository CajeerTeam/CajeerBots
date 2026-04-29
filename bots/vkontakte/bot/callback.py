from __future__ import annotations

from bots.vkontakte.bot.thin import VkontakteThinWrapper
from core.events import CajeerEvent


class VkontakteCallback:
    """Callback API parser для ВКонтакте."""

    def __init__(self, token: str = "") -> None:
        self.wrapper = VkontakteThinWrapper(token)

    async def handle(self, payload: dict[str, object]) -> CajeerEvent:
        return await self.wrapper.callback_event(payload)
