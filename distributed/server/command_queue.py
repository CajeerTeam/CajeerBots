from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class CommandQueue:
    queues: dict[str, list[dict[str, object]]] = field(default_factory=dict)

    def push(self, node_id: str, command: dict[str, object]) -> None:
        self.queues.setdefault(node_id, []).append(command)

    def pop_all(self, node_id: str) -> list[dict[str, object]]:
        return self.queues.pop(node_id, [])
