from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RuntimeAgentStatus:
    node_id: str
    connected: bool = False
    degraded: bool = False
    last_heartbeat_at: str = ""
    last_error: str = ""
    capabilities: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class RuntimeAgent:
    def __init__(self, node_id: str, *, capabilities: list[str] | None = None) -> None:
        self.status = RuntimeAgentStatus(node_id=node_id, capabilities=capabilities or [])

    def heartbeat_payload(self) -> dict[str, object]:
        self.status.last_heartbeat_at = now_iso()
        return self.status.to_dict()

    def mark_connected(self) -> None:
        self.status.connected = True
        self.status.degraded = False
        self.status.last_error = ""
        self.status.last_heartbeat_at = now_iso()

    def mark_degraded(self, error: str) -> None:
        self.status.degraded = True
        self.status.last_error = error
        self.status.last_heartbeat_at = now_iso()
