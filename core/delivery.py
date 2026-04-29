from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import uuid4

DeliveryStatus = Literal["pending", "processing", "sent", "failed"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sql_text(statement: str):
    from sqlalchemy import text

    return text(statement)


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
        payload = data.get("payload")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        if isinstance(payload, dict):
            data = {**payload, **{k: v for k, v in data.items() if k != "payload"}}
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
    _tasks: list[DeliveryTask] = field(default_factory=list)
    delivered_total: int = 0
    failed_total: int = 0
    backend: str = "memory"
    retry_backoff_seconds: int = 5

    def enqueue(self, adapter: str, target: str, text: str, *, max_attempts: int = 3, trace_id: str | None = None) -> DeliveryTask:
        task = DeliveryTask(adapter=adapter, target=target, text=text, created_at=_now(), max_attempts=max_attempts, trace_id=trace_id)
        self._tasks.append(task)
        return task

    async def enqueue_async(self, adapter: str, target: str, text: str, *, max_attempts: int = 3, trace_id: str | None = None) -> DeliveryTask:
        return self.enqueue(adapter, target, text, max_attempts=max_attempts, trace_id=trace_id)

    def snapshot(self) -> list[DeliveryTask]:
        return list(self._tasks)

    async def claim(self, adapter: str, limit: int = 50) -> list[DeliveryTask]:
        now = datetime.now(timezone.utc)
        claimed: list[DeliveryTask] = []
        for task in self._tasks:
            if task.adapter != adapter or task.status != "pending":
                continue
            if task.retry_after:
                try:
                    if datetime.fromisoformat(task.retry_after) > now:
                        continue
                except ValueError:
                    pass
            task.status = "processing"
            task.attempts += 1
            claimed.append(task)
            if len(claimed) >= limit:
                break
        return claimed

    async def mark_sent(self, delivery_id: str) -> None:
        self._tasks = [task for task in self._tasks if task.delivery_id != delivery_id]
        self.delivered_total += 1

    async def mark_failed(self, delivery_id: str, error: str, *, retry: bool = True) -> None:
        for task in self._tasks:
            if task.delivery_id != delivery_id:
                continue
            task.last_error = error
            if retry and task.attempts < task.max_attempts:
                task.status = "pending"
                delay = self.retry_backoff_seconds * max(1, 2 ** max(0, task.attempts - 1))
                task.retry_after = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
            else:
                task.status = "failed"
                self.failed_total += 1
            return

    async def process_for_adapter(self, adapter: Any) -> int:
        processed = 0
        for task in await self.claim(adapter.name, limit=50):
            try:
                await adapter.send_message(task.target, task.text)
                await self.mark_sent(task.delivery_id)
                processed += 1
            except Exception as exc:  # pragma: no cover
                await self.mark_failed(task.delivery_id, str(exc), retry=True)
        return processed

    async def process_once(self, adapters: dict[str, Any] | None = None) -> int:
        return sum([await self.process_for_adapter(adapter) for adapter in (adapters or {}).values()])


class RedisDeliveryService(DeliveryService):
    def __init__(self, redis_url: str, prefix: str, *, retry_backoff_seconds: int = 5) -> None:
        super().__init__(backend="redis", retry_backoff_seconds=retry_backoff_seconds)
        self.redis_url = redis_url
        self.prefix = prefix
        self._redis: Any | None = None

    def _key(self, adapter: str) -> str:
        return f"{self.prefix}:delivery:{adapter}"

    async def _client(self) -> Any:
        if self._redis is None:
            from redis.asyncio import Redis  # type: ignore

            self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    async def enqueue_async(self, adapter: str, target: str, text: str, *, max_attempts: int = 3, trace_id: str | None = None) -> DeliveryTask:
        task = DeliveryTask(adapter=adapter, target=target, text=text, created_at=_now(), max_attempts=max_attempts, trace_id=trace_id)
        await (await self._client()).rpush(self._key(adapter), json.dumps(task.to_dict(), ensure_ascii=False))
        self._tasks.append(task)
        return task

    async def claim(self, adapter: str, limit: int = 50) -> list[DeliveryTask]:
        redis = await self._client()
        result: list[DeliveryTask] = []
        for _ in range(limit):
            raw = await redis.lpop(self._key(adapter))
            if raw is None:
                break
            task = DeliveryTask.from_dict(json.loads(raw))
            task.status = "processing"
            task.attempts += 1
            result.append(task)
        return result

    async def mark_failed(self, delivery_id: str, error: str, *, retry: bool = True) -> None:
        task = next((item for item in self._tasks if item.delivery_id == delivery_id), None)
        if task and retry and task.attempts < task.max_attempts:
            task.last_error = error
            task.status = "pending"
            await (await self._client()).rpush(self._key(task.adapter), json.dumps(task.to_dict(), ensure_ascii=False))
            return
        self.failed_total += 1


class PostgresDeliveryService(DeliveryService):
    def __init__(self, async_dsn: str, schema: str = "shared", *, retry_backoff_seconds: int = 5) -> None:
        super().__init__(backend="postgres", retry_backoff_seconds=retry_backoff_seconds)
        self.async_dsn = async_dsn
        self.schema = schema
        self._engine: Any | None = None

    def _engine_obj(self) -> Any:
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine

            self._engine = create_async_engine(self.async_dsn, pool_pre_ping=True)
        return self._engine

    async def enqueue_async(self, adapter: str, target: str, text: str, *, max_attempts: int = 3, trace_id: str | None = None) -> DeliveryTask:
        task = DeliveryTask(adapter=adapter, target=target, text=text, created_at=_now(), max_attempts=max_attempts, trace_id=trace_id)
        async with self._engine_obj().begin() as conn:
            await conn.execute(
                _sql_text(f"""INSERT INTO {self.schema}.delivery_queue
                (delivery_id, adapter, target, payload, status, attempts, max_attempts, trace_id, created_at)
                VALUES (:delivery_id, :adapter, :target, CAST(:payload AS jsonb), 'pending', 0, :max_attempts, :trace_id, NOW())
                ON CONFLICT (delivery_id) DO NOTHING"""),
                {"delivery_id": task.delivery_id, "adapter": adapter, "target": target, "payload": json.dumps(task.to_dict(), ensure_ascii=False), "max_attempts": max_attempts, "trace_id": trace_id},
            )
        return task

    async def claim(self, adapter: str, limit: int = 50) -> list[DeliveryTask]:
        async with self._engine_obj().begin() as conn:
            rows = (await conn.execute(_sql_text(f"""WITH picked AS (
                SELECT delivery_id FROM {self.schema}.delivery_queue
                WHERE adapter=:adapter AND status='pending'
                ORDER BY created_at LIMIT :limit FOR UPDATE SKIP LOCKED)
                UPDATE {self.schema}.delivery_queue q
                SET status='processing', attempts=q.attempts+1, locked_at=NOW()
                FROM picked WHERE q.delivery_id=picked.delivery_id
                RETURNING q.delivery_id, q.adapter, q.target, q.payload, q.attempts, q.max_attempts, q.trace_id, q.created_at, q.last_error"""), {"adapter": adapter, "limit": limit})).mappings().all()
        result: list[DeliveryTask] = []
        for row in rows:
            payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(str(row["payload"]))
            result.append(DeliveryTask.from_dict({**payload, **dict(row), "status": "processing"}))
        return result

    async def mark_sent(self, delivery_id: str) -> None:
        async with self._engine_obj().begin() as conn:
            await conn.execute(_sql_text(f"UPDATE {self.schema}.delivery_queue SET status='sent', sent_at=NOW() WHERE delivery_id=:delivery_id"), {"delivery_id": delivery_id})
        self.delivered_total += 1

    async def mark_failed(self, delivery_id: str, error: str, *, retry: bool = True) -> None:
        status_expr = "CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END" if retry else "'failed'"
        async with self._engine_obj().begin() as conn:
            await conn.execute(_sql_text(f"UPDATE {self.schema}.delivery_queue SET status={status_expr}, last_error=:error, locked_at=NULL WHERE delivery_id=:delivery_id"), {"delivery_id": delivery_id, "error": error})
        self.failed_total += 1


def build_delivery_service(settings: Any) -> DeliveryService:
    backend = settings.storage.delivery_backend
    backoff = getattr(settings.storage, "delivery_retry_backoff_seconds", 5)
    if backend == "redis":
        if not settings.redis_url:
            raise RuntimeError("DELIVERY_BACKEND=redis требует REDIS_URL")
        return RedisDeliveryService(settings.redis_url, settings.storage.redis_queue_prefix, retry_backoff_seconds=backoff)
    if backend == "postgres":
        if not settings.storage.async_database_url:
            raise RuntimeError("DELIVERY_BACKEND=postgres требует DATABASE_ASYNC_URL")
        return PostgresDeliveryService(settings.storage.async_database_url, settings.shared_schema, retry_backoff_seconds=backoff)
    return DeliveryService(retry_backoff_seconds=backoff)
