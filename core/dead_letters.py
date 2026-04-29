from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from core.events import CajeerEvent


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class DeadLetter:
    event: CajeerEvent
    reason: str
    created_at: str

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["event"] = self.event.to_dict()
        return data


@dataclass
class DeadLetterQueue:
    max_size: int = 1000
    _items: list[DeadLetter] = field(default_factory=list)

    def add(self, event: CajeerEvent, reason: str) -> None:
        self._items.append(DeadLetter(event, reason, _now()))
        self._items = self._items[-self.max_size :]

    def snapshot(self) -> list[DeadLetter]:
        return list(self._items)

    def retry_all(self) -> list[CajeerEvent]:
        events = [item.event for item in self._items]
        self._items.clear()
        return events

    def count(self) -> int:
        return len(self._items)
