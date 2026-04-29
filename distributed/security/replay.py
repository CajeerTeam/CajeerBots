from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ReplayGuard:
    seen_ids: set[str] = field(default_factory=set)

    def accept(self, item_id: str) -> bool:
        if item_id in self.seen_ids:
            return False
        self.seen_ids.add(item_id)
        return True
