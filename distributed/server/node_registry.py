from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class NodeRegistry:
    nodes: dict[str, dict[str, object]] = field(default_factory=dict)

    def register(self, node_id: str, payload: dict[str, object]) -> None:
        self.nodes[node_id] = payload

    def snapshot(self) -> dict[str, dict[str, object]]:
        return dict(self.nodes)
