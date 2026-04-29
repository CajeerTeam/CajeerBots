from __future__ import annotations

from core.events import CajeerEvent, message_event


def vk_update_to_event(update: dict[str, object]) -> CajeerEvent:
    message = update.get("message") or update.get("object") or update
    if not isinstance(message, dict):
        message = {}
    return message_event(
        source="vkontakte",
        platform_user_id=str(message.get("from_id") or ""),
        platform_chat_id=str(message.get("peer_id") or ""),
        chat_type="conversation",
        text=str(message.get("text") or ""),
        raw={"update": update},
    )
