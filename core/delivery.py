from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

DeliveryStatus = Literal["pending", "sent", "failed"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class DeliveryTask:
    adapter: str
    target: str
    text: str
    created_at: str
    delivery_id: str = field(default_factory=lambda: str(uuid4()))
    trace_id: str | None = None
    attempts: int = 0
    max_attempts: int = 3
    status: DeliveryStatus = "pending"
    last_error: str | None = None
    retry_after: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "DeliveryTask":
        return cls(
            adapter=str(data.get("adapter") or ""),
            target=str(data.get("target") or ""),
            text=str(data.get("text") or ""),
            created_at=str(data.get("created_at") or _now()),
            delivery_id=str(data.get("delivery_id") or uuid4()),
            trace_id=data.get("trace_id") if data.get("trace_id") is None else str(data.get("trace_id")),
            attempts=int(data.get("attempts") or 0),
            max_attempts=int(data.get("max_attempts") or 3),
            status=str(data.get("status") or "pending"),  # type: ignore[arg-type]
            last_error=data.get("last_error") if data.get("last_error") is None else str(data.get("last_error")),
            retry_after=data.get("retry_after") if data.get("retry_after") is None else str(data.get("retry_after")),
        )


@dataclass
class DeliveryService:
    """Локальная очередь исходящей доставки через запущенные адаптеры."""

    _tasks: list[DeliveryTask] = field(default_factory=list)
    delivered_total: int = 0
    failed_total: int = 0
    backend: str = "memory"

    def enqueue(self, adapter: str, target: str, text: str, *, max_attempts: int = 3, trace_id: str | None = None) -> DeliveryTask:
        task = DeliveryTask(adapter=adapter, target=target, text=text, created_at=_now(), max_attempts=max_attempts, trace_id=trace_id)
        self._tasks.append(task)
        return task

    def snapshot(self) -> list[DeliveryTask]:
        return list(self._tasks)

    async def process_for_adapter(self, adapter: Any) -> int:
        return await self.process_once({adapter.name: adapter})

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


class RedisDeliveryService(DeliveryService):
    def __init__(self, redis_url: str, prefix: str) -> None:
        super().__init__(backend="redis")
        self.redis_url = redis_url
        self.stream = f"{prefix}:delivery"
        self.group = "cajeer-bots-delivery"
        self.consumer = f"worker-{uuid4()}"
        self._redis: Any | None = None

    async def _client(self) -> Any:
        if self._redis is None:
            try:
                from redis.asyncio import Redis  # type: ignore
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError("для DELIVERY_BACKEND=redis установите пакет redis") from exc
            self._redis = Redis.from_url(self.redis_url, decode_responses=True)
            try:
                await self._redis.xgroup_create(self.stream, self.group, id="0-0", mkstream=True)
            except Exception:
                pass
        return self._redis


class PostgresDeliveryService(DeliveryService):
    def __init__(self, async_dsn: str, schema: str = "shared") -> None:
        super().__init__(backend="postgres")
        self.async_dsn = async_dsn
        self.schema = schema


def build_delivery_service(settings: Any) -> DeliveryService:
    backend = settings.storage.delivery_backend
    if backend == "redis":
        if not settings.redis_url:
            raise RuntimeError("DELIVERY_BACKEND=redis требует REDIS_URL")
        return RedisDeliveryService(settings.redis_url, settings.storage.redis_queue_prefix)
    if backend == "postgres":
        dsn = settings.storage.async_database_url
        if not dsn:
            raise RuntimeError("DELIVERY_BACKEND=postgres требует DATABASE_ASYNC_URL")
        return PostgresDeliveryService(dsn, settings.shared_schema)
    return DeliveryService()
