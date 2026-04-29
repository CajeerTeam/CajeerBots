from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass
class RuntimeAgentStatus:
    node_id: str
    connected: bool = False
    degraded: bool = False

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class RuntimeAgent:
    def __init__(self, node_id: str) -> None:
        self.status = RuntimeAgentStatus(node_id=node_id)
