from __future__ import annotations

from typing import Any, Protocol


class RuntimeHandle(Protocol):
    version: str
    settings: Any
    event_bus: Any
    router: Any
    delivery: Any
    audit: Any
