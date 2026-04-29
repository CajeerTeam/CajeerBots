from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import uuid4

DeliveryStatus = Literal["pending", "processing", "sent", "failed"]


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _now() -> str:
    return _now_dt().isoformat()


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
    next_attempt_at: str | None = None
    locked_by: str | None = None
    locked_at: str | None = None
    sent_at: str | None = None
    failed_at: str | None = None
    rate_limit_bucket: str | None = None

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
            next_attempt_at=data.get("next_attempt_at") if data.get("next_attempt_at") is None else str(data.get("next_attempt_at")),
            locked_by=data.get("locked_by") if data.get("locked_by") is None else str(data.get("locked_by")),
            locked_at=data.get("locked_at") if data.get("locked_at") is None else str(data.get("locked_at")),
            sent_at=data.get("sent_at") if data.get("sent_at") is None else str(data.get("sent_at")),
            failed_at=data.get("failed_at") if data.get("failed_at") is None else str(data.get("failed_at")),
            rate_limit_bucket=data.get("rate_limit_bucket") if data.get("rate_limit_bucket") is None else str(data.get("rate_limit_bucket")),
        )

    def due(self) -> bool:
        marker = self.next_attempt_at or self.retry_after
        if not marker:
            return True
        try:
            return datetime.fromisoformat(marker) <= _now_dt()
        except ValueError:
            return True


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

    async def claim(self, adapter: str, limit: int = 50, *, consumer: str | None = None) -> list[DeliveryTask]:
        claimed: list[DeliveryTask] = []
        for task in self._tasks:
            if task.adapter != adapter or task.status != "pending" or not task.due():
                continue
            task.status = "processing"
            task.attempts += 1
            task.locked_by = consumer or adapter
            task.locked_at = _now()
            claimed.append(task)
            if len(claimed) >= limit:
                break
        return claimed

    async def mark_sent(self, delivery_id: str) -> None:
        for task in self._tasks:
            if task.delivery_id == delivery_id:
                task.status = "sent"
                task.sent_at = _now()
                break
        self._tasks = [task for task in self._tasks if task.delivery_id != delivery_id]
        self.delivered_total += 1

    async def mark_failed(self, delivery_id: str, error: str, *, retry: bool = True) -> None:
        for task in self._tasks:
            if task.delivery_id != delivery_id:
                continue
            task.last_error = error
            task.locked_at = None
            task.locked_by = None
            if retry and task.attempts < task.max_attempts:
                task.status = "pending"
                delay = self.retry_backoff_seconds * max(1, 2 ** max(0, task.attempts - 1))
                next_time = _now_dt() + timedelta(seconds=delay)
                task.next_attempt_at = next_time.isoformat()
                task.retry_after = task.next_attempt_at
            else:
                task.status = "failed"
                task.failed_at = _now()
                self.failed_total += 1
            return

    async def process_for_adapter(self, adapter: Any) -> int:
        processed = 0
        for task in await self.claim(adapter.name, limit=50, consumer=getattr(adapter.settings, "instance_id", adapter.name)):
            try:
                if getattr(adapter, "context", None) is not None and getattr(adapter.context, "rate_limiter", None) is not None:
                    await adapter.context.rate_limiter.acquire(f"{task.adapter}:{task.target}")
                await adapter.send_message(task.target, task.text)
                await self.mark_sent(task.delivery_id)
                processed += 1
            except Exception as exc:  # pragma: no cover
                await self.mark_failed(task.delivery_id, str(exc), retry=True)
        return processed

    async def process_once(self, adapters: dict[str, Any] | None = None) -> int:
        return sum([await self.process_for_adapter(adapter) for adapter in (adapters or {}).values()])


class RedisDeliveryService(DeliveryService):
    def __init__(self, redis_url: str, prefix: str, *, retry_backoff_seconds: int = 5, group: str = "cajeer-bots-delivery") -> None:
        super().__init__(backend="redis", retry_backoff_seconds=retry_backoff_seconds)
        self.redis_url = redis_url
        self.prefix = prefix
        self.group = group
        self._redis: Any | None = None
        self._groups_ready: set[str] = set()

    def _stream(self, adapter: str) -> str:
        return f"{self.prefix}:delivery:{adapter}"

    def _dlq(self, adapter: str) -> str:
        return f"{self.prefix}:delivery:{adapter}:dlq"

    async def _client(self) -> Any:
        if self._redis is None:
            from redis.asyncio import Redis  # type: ignore

            self._redis = Redis.from_url(self.redis_url, decode_responses=True)
        return self._redis

    async def _ensure_group(self, adapter: str) -> None:
        stream = self._stream(adapter)
        if stream in self._groups_ready:
            return
        redis = await self._client()
        try:
            await redis.xgroup_create(stream, self.group, id="0-0", mkstream=True)
        except Exception:
            pass
        self._groups_ready.add(stream)

    async def enqueue_async(self, adapter: str, target: str, text: str, *, max_attempts: int = 3, trace_id: str | None = None) -> DeliveryTask:
        task = DeliveryTask(adapter=adapter, target=target, text=text, created_at=_now(), max_attempts=max_attempts, trace_id=trace_id)
        await self._ensure_group(adapter)
        await (await self._client()).xadd(self._stream(adapter), {"payload": json.dumps(task.to_dict(), ensure_ascii=False), "delivery_id": task.delivery_id})
        self._tasks.append(task)
        return task

    async def claim(self, adapter: str, limit: int = 50, *, consumer: str | None = None) -> list[DeliveryTask]:
        await self._ensure_group(adapter)
        redis = await self._client()
        consumer = consumer or adapter
        claimed: list[tuple[str, dict[str, str]]] = []
        try:
            reclaimed = await redis.xautoclaim(self._stream(adapter), self.group, consumer, 60_000, "0-0", count=limit)
            claimed.extend(reclaimed[1])
        except Exception:
            pass
        remaining = max(0, limit - len(claimed))
        if remaining:
            records = await redis.xreadgroup(self.group, consumer, {self._stream(adapter): ">"}, count=remaining, block=1)
            for _, items in records:
                claimed.extend(items)
        tasks: list[DeliveryTask] = []
        for item_id, fields in claimed:
            task = DeliveryTask.from_dict(json.loads(fields.get("payload") or "{}"))
            if not task.due():
                # Not due yet: re-enqueue and ACK the premature claim.
                await redis.xadd(self._stream(adapter), {"payload": json.dumps(task.to_dict(), ensure_ascii=False), "delivery_id": task.delivery_id})
                await redis.xack(self._stream(adapter), self.group, item_id)
                continue
            task.status = "processing"
            task.attempts += 1
            task.locked_by = consumer
            task.locked_at = _now()
            task.rate_limit_bucket = task.rate_limit_bucket or f"{adapter}:{task.target}"
            task.locked_at = item_id  # lease id for ack; stored separately from DB semantics.
            tasks.append(task)
        return tasks

    async def mark_sent(self, delivery_id: str) -> None:
        # Redis Streams ACK needs stream id. For compatibility, delivery_id is also accepted for in-memory metrics.
        self.delivered_total += 1

    async def mark_failed(self, delivery_id: str, error: str, *, retry: bool = True) -> None:
        task = next((item for item in self._tasks if item.delivery_id == delivery_id), None)
        if task and retry and task.attempts < task.max_attempts:
            delay = self.retry_backoff_seconds * max(1, 2 ** max(0, task.attempts - 1))
            task.next_attempt_at = (_now_dt() + timedelta(seconds=delay)).isoformat()
            task.retry_after = task.next_attempt_at
            task.last_error = error
            task.status = "pending"
            await (await self._client()).xadd(self._stream(task.adapter), {"payload": json.dumps(task.to_dict(), ensure_ascii=False), "delivery_id": task.delivery_id})
            return
        self.failed_total += 1

    async def process_for_adapter(self, adapter: Any) -> int:
        processed = 0
        redis = await self._client()
        stream = self._stream(adapter.name)
        await self._ensure_group(adapter.name)
        for task in await self.claim(adapter.name, limit=50, consumer=getattr(adapter.settings, "instance_id", adapter.name)):
            stream_id = task.locked_at
            try:
                if getattr(adapter, "context", None) is not None and getattr(adapter.context, "rate_limiter", None) is not None:
                    await adapter.context.rate_limiter.acquire(f"{task.adapter}:{task.target}")
                await adapter.send_message(task.target, task.text)
                if stream_id:
                    await redis.xack(stream, self.group, stream_id)
                self.delivered_total += 1
                processed += 1
            except Exception as exc:  # pragma: no cover
                if stream_id:
                    await redis.xack(stream, self.group, stream_id)
                await self.mark_failed(task.delivery_id, str(exc), retry=True)
        return processed


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
        task = DeliveryTask(adapter=adapter, target=target, text=text, created_at=_now(), max_attempts=max_attempts, trace_id=trace_id, rate_limit_bucket=f"{adapter}:{target}")
        async with self._engine_obj().begin() as conn:
            await conn.execute(
                _sql_text(
                    f"""INSERT INTO {self.schema}.delivery_queue
                    (delivery_id, adapter, target, payload, status, attempts, max_attempts, trace_id, created_at, next_attempt_at, rate_limit_bucket)
                    VALUES (:delivery_id, :adapter, :target, CAST(:payload AS jsonb), 'pending', 0, :max_attempts, :trace_id, NOW(), NULL, :rate_limit_bucket)
                    ON CONFLICT (delivery_id) DO NOTHING"""
                ),
                {"delivery_id": task.delivery_id, "adapter": adapter, "target": target, "payload": json.dumps(task.to_dict(), ensure_ascii=False), "max_attempts": max_attempts, "trace_id": trace_id, "rate_limit_bucket": task.rate_limit_bucket},
            )
        return task

    async def claim(self, adapter: str, limit: int = 50, *, consumer: str | None = None) -> list[DeliveryTask]:
        consumer = consumer or adapter
        async with self._engine_obj().begin() as conn:
            rows = (
                await conn.execute(
                    _sql_text(
                        f"""WITH picked AS (
                        SELECT delivery_id FROM {self.schema}.delivery_queue
                         WHERE adapter=:adapter
                           AND status='pending'
                           AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
                         ORDER BY created_at
                         LIMIT :limit
                         FOR UPDATE SKIP LOCKED
                    )
                    UPDATE {self.schema}.delivery_queue q
                       SET status='processing', attempts=q.attempts+1, locked_at=NOW(), locked_by=:consumer
                      FROM picked
                     WHERE q.delivery_id=picked.delivery_id
                 RETURNING q.delivery_id, q.adapter, q.target, q.payload, q.attempts, q.max_attempts, q.trace_id, q.created_at, q.last_error, q.next_attempt_at, q.locked_by, q.locked_at, q.rate_limit_bucket"""
                    ),
                    {"adapter": adapter, "limit": limit, "consumer": consumer},
                )
            ).mappings().all()
        result: list[DeliveryTask] = []
        for row in rows:
            payload = row["payload"] if isinstance(row["payload"], dict) else json.loads(str(row["payload"]))
            result.append(DeliveryTask.from_dict({**payload, **dict(row), "status": "processing"}))
        return result

    async def mark_sent(self, delivery_id: str) -> None:
        async with self._engine_obj().begin() as conn:
            await conn.execute(_sql_text(f"UPDATE {self.schema}.delivery_queue SET status='sent', sent_at=NOW(), locked_at=NULL, locked_by=NULL WHERE delivery_id=:delivery_id"), {"delivery_id": delivery_id})
        self.delivered_total += 1

    async def mark_failed(self, delivery_id: str, error: str, *, retry: bool = True) -> None:
        status_expr = "CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'failed' END" if retry else "'failed'"
        backoff = max(0, int(self.retry_backoff_seconds))
        async with self._engine_obj().begin() as conn:
            await conn.execute(
                _sql_text(
                    f"""UPDATE {self.schema}.delivery_queue
                       SET status={status_expr},
                           last_error=:error,
                           locked_at=NULL,
                           locked_by=NULL,
                           failed_at=CASE WHEN attempts >= max_attempts OR :retry = false THEN NOW() ELSE failed_at END,
                           next_attempt_at=CASE WHEN :retry = true AND attempts < max_attempts THEN NOW() + (:backoff || ' seconds')::interval ELSE NULL END
                     WHERE delivery_id=:delivery_id"""
                ),
                {"delivery_id": delivery_id, "error": error, "retry": retry, "backoff": backoff},
            )
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
