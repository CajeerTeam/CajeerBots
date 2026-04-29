from __future__ import annotations

from core.events import CajeerEvent


def vk_update_to_event(update: dict[str, object]) -> CajeerEvent:
    return CajeerEvent.create(source="vkontakte", type="message.received", payload={"update": update})
