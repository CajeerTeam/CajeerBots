from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

DeliveryStatus = Literal["pending", "sent", "failed"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class DeliveryTask:
    adapter: str
    target: str
    text: str
    created_at: str
    attempts: int = 0
    max_attempts: int = 3
    status: DeliveryStatus = "pending"
    last_error: str | None = None
    retry_after: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class DeliveryService:
    """Очередь исходящей доставки через запущенные адаптеры."""

    _tasks: list[DeliveryTask] = field(default_factory=list)
    delivered_total: int = 0
    failed_total: int = 0

    def enqueue(self, adapter: str, target: str, text: str, *, max_attempts: int = 3) -> DeliveryTask:
        task = DeliveryTask(adapter=adapter, target=target, text=text, created_at=_now(), max_attempts=max_attempts)
        self._tasks.append(task)
        return task

    def snapshot(self) -> list[DeliveryTask]:
        return list(self._tasks)

    async def process_once(self, adapters: dict[str, Any] | None = None) -> int:
        adapters = adapters or {}
        processed = 0
        remaining: list[DeliveryTask] = []
        for task in self._tasks:
            if task.status != "pending":
                remaining.append(task)
                continue
            adapter = adapters.get(task.adapter)
            if adapter is None:
                task.attempts += 1
                task.last_error = f"адаптер не запущен: {task.adapter}"
                if task.attempts >= task.max_attempts:
                    task.status = "failed"
                    self.failed_total += 1
                else:
                    remaining.append(task)
                continue
            try:
                task.attempts += 1
                await adapter.send_message(task.target, task.text)
                task.status = "sent"
                self.delivered_total += 1
                processed += 1
            except Exception as exc:  # pragma: no cover - защитный контур доставки
                task.last_error = str(exc)
                if task.attempts >= task.max_attempts:
                    task.status = "failed"
                    self.failed_total += 1
                else:
                    remaining.append(task)
        self._tasks = remaining
        return processed
