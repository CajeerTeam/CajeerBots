from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from uuid import uuid4

SCHEMA = "cajeer.bots.event.v1"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class RuntimeEvent:
    node_id: str
    bot_id: str
    platform: str
    type: str
    payload: dict[str, object] = field(default_factory=dict)
    schema: str = SCHEMA
    event_id: str = field(default_factory=lambda: f"evt_{uuid4().hex}")
    trace_id: str = field(default_factory=lambda: f"trc_{uuid4().hex}")
    timestamp: str = field(default_factory=now_iso)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
