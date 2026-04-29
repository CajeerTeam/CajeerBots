from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class IdempotencyStore:
    """Локальное хранилище ключей идемпотентности для одиночного процесса."""

    _seen: set[str] = field(default_factory=set)

    def seen(self, key: str) -> bool:
        if key in self._seen:
            return True
        self._seen.add(key)
        return False

    def count(self) -> int:
        return len(self._seen)
