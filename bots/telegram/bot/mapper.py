from __future__ import annotations

from core.events import CajeerEvent


def update_to_event(update: dict[str, object]) -> CajeerEvent:
    return CajeerEvent.create(source="telegram", type="message.received", payload={"update": update})


def command_to_payload(command: dict[str, object]) -> dict[str, object]:
    return dict(command.get("payload") or {})
