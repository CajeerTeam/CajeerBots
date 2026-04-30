from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DeliveryMessage:
    adapter: str
    target: str
    text: str
    trace_id: str | None = None
    max_attempts: int = 3
