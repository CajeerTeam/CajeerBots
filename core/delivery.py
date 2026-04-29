from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class DeliveryTask:
    adapter: str
    target: str
    text: str
    created_at: str
    attempts: int = 0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class DeliveryService:
    """Минимальная очередь исходящей доставки для каркаса платформы."""

    _tasks: list[DeliveryTask] = field(default_factory=list)
    delivered_total: int = 0
    failed_total: int = 0

    def enqueue(self, adapter: str, target: str, text: str) -> None:
        self._tasks.append(DeliveryTask(adapter=adapter, target=target, text=text, created_at=_now()))

    def snapshot(self) -> list[DeliveryTask]:
        return list(self._tasks)

    async def process_once(self) -> int:
        # Боевые адаптеры подключаются на следующем уровне реализации.
        processed = len(self._tasks)
        self.delivered_total += processed
        self._tasks.clear()
        return processed
