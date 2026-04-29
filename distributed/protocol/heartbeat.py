from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

SCHEMA = "cajeer.bots.heartbeat.v1"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class NodeHeartbeat:
    node_id: str
    status: str
    bots_running: int
    queue_lag: int = 0
    schema: str = SCHEMA
    timestamp: str = field(default_factory=now_iso)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
