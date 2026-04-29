from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal

EVENT_CONTRACT_VERSION = 1
EventSource = Literal["telegram", "discord", "vkontakte", "system", "module", "plugin"]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Actor:
    platform: str
    platform_user_id: str
    identity_id: str | None = None
    display_name: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Actor":
        return cls(
            platform=str(data.get("platform", "")),
            platform_user_id=str(data.get("platform_user_id", "")),
            identity_id=data.get("identity_id"),
            display_name=data.get("display_name"),
        )


@dataclass(frozen=True)
class ChatRef:
    platform: str
    platform_chat_id: str
    type: str = "unknown"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChatRef":
        return cls(
            platform=str(data.get("platform", "")),
            platform_chat_id=str(data.get("platform_chat_id", "")),
            type=str(data.get("type", "unknown")),
        )


@dataclass(frozen=True)
class CajeerEvent:
    event_id: str
    contract_version: int
    source: EventSource
    type: str
    actor: Actor | None
    chat: ChatRef | None
    payload: dict[str, Any]
    trace_id: str
    created_at: str
    module_id: str | None = None
    plugin_id: str | None = None

    @classmethod
    def create(
        cls,
        *,
        source: EventSource,
        type: str,
        payload: dict[str, Any] | None = None,
        actor: Actor | None = None,
        chat: ChatRef | None = None,
        trace_id: str | None = None,
        module_id: str | None = None,
        plugin_id: str | None = None,
    ) -> "CajeerEvent":
        return cls(
            str(uuid.uuid4()),
            EVENT_CONTRACT_VERSION,
            source,
            type,
            actor,
            chat,
            payload or {},
            trace_id or str(uuid.uuid4()),
            utcnow().isoformat(),
            module_id,
            plugin_id,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CajeerEvent":
        actor = data.get("actor")
        chat = data.get("chat")
        return cls(
            event_id=str(data["event_id"]),
            contract_version=int(data["contract_version"]),
            source=data["source"],
            type=str(data["type"]),
            actor=Actor.from_dict(actor) if isinstance(actor, dict) else None,
            chat=ChatRef.from_dict(chat) if isinstance(chat, dict) else None,
            payload=dict(data.get("payload") or {}),
            trace_id=str(data["trace_id"]),
            created_at=str(data["created_at"]),
            module_id=data.get("module_id"),
            plugin_id=data.get("plugin_id"),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sign_event(event: CajeerEvent, secret: str) -> str:
    return hmac.new(secret.encode(), event.to_json().encode(), hashlib.sha256).hexdigest()


def verify_event_signature(event: CajeerEvent, secret: str, signature: str) -> bool:
    return hmac.compare_digest(sign_event(event, secret), signature)


def validate_event(event: CajeerEvent) -> list[str]:
    errors = []
    if event.contract_version != EVENT_CONTRACT_VERSION:
        errors.append(f"неподдерживаемая версия контракта события: {event.contract_version}")
    if not event.event_id:
        errors.append("event_id обязателен")
    if not event.type or "." not in event.type:
        errors.append("тип события должен быть именованным, например message.created")
    if not event.trace_id:
        errors.append("trace_id обязателен")
    return errors
