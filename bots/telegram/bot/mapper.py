from __future__ import annotations

from typing import Any

from core.events import CajeerEvent, message_event


def update_to_event(update: dict[str, object]) -> CajeerEvent:
    message = update.get("message") or update.get("edited_message") or update.get("channel_post") or {}
    if not isinstance(message, dict):
        message = {}
    user = message.get("from") or {}
    chat = message.get("chat") or {}
    if not isinstance(user, dict):
        user = {}
    if not isinstance(chat, dict):
        chat = {}
    text = str(message.get("text") or message.get("caption") or "")
    return message_event(
        source="telegram",
        platform_user_id=str(user.get("id") or ""),
        platform_chat_id=str(chat.get("id") or ""),
        chat_type=str(chat.get("type") or "unknown"),
        display_name=" ".join(item for item in [str(user.get("first_name") or ""), str(user.get("last_name") or "")] if item).strip() or None,
        text=text,
        raw={"update": update},
    )


def command_to_payload(command: dict[str, object]) -> dict[str, object]:
    return dict(command.get("payload") or {})
