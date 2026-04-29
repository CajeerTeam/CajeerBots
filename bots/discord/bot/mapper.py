from __future__ import annotations

from core.events import CajeerEvent


def interaction_to_event(interaction: dict[str, object]) -> CajeerEvent:
    return CajeerEvent.create(source="discord", type="message.received", payload={"interaction": interaction})
