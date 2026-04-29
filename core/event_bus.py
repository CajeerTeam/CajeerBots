from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

from core.events import CajeerEvent, validate_event


@dataclass
class InMemoryEventBus:
    """Минимальная локальная шина событий для каркаса платформы.

    Боевой транспорт можно заменить на PostgreSQL LISTEN/NOTIFY, Redis Streams,
    Kafka или другой брокер без изменения контрактов адаптеров.
    """

    max_size: int = 1000
    _events: deque[CajeerEvent] = field(default_factory=deque)

    async def publish(self, event: CajeerEvent) -> None:
        errors = validate_event(event)
        if errors:
            raise ValueError("; ".join(errors))
        self._events.append(event)
        while len(self._events) > self.max_size:
            self._events.popleft()

    def snapshot(self) -> list[CajeerEvent]:
        return list(self._events)
