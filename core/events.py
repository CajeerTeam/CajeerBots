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


@dataclass(frozen=True)
class ChatRef:
    platform: str
    platform_chat_id: str
    type: str = "unknown"


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
