from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


def _sql_text(statement: str):
    from sqlalchemy import text
    return text(statement)


@dataclass
class OutboundMessageRepository:
    async_dsn: str
    schema: str = "shared"
    _engine: Any | None = None

    def _engine_obj(self) -> Any:
        if self._engine is None:
            from sqlalchemy.ext.asyncio import create_async_engine
            self._engine = create_async_engine(self.async_dsn, pool_pre_ping=True)
        return self._engine

    async def mark_sending(self, *, delivery_id: str, adapter: str, target: str, text: str, trace_id: str | None = None) -> None:
        text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
        async with self._engine_obj().begin() as conn:
            await conn.execute(
                _sql_text(
                    f"""INSERT INTO {self.schema}.outbound_messages
                    (message_id, delivery_id, adapter, target, text_hash, status, attempts, trace_id, created_at)
                    VALUES (:message_id, :delivery_id, :adapter, :target, :text_hash, 'sending', 1, :trace_id, NOW())
                    ON CONFLICT (delivery_id) DO UPDATE
                       SET status='sending', attempts={self.schema}.outbound_messages.attempts + 1, last_error=NULL"""
                ),
                {"message_id": delivery_id, "delivery_id": delivery_id, "adapter": adapter, "target": target, "text_hash": text_hash, "trace_id": trace_id},
            )

    async def mark_sent(self, *, delivery_id: str, platform_message_id: str | None = None) -> None:
        async with self._engine_obj().begin() as conn:
            await conn.execute(
                _sql_text(f"UPDATE {self.schema}.outbound_messages SET status='sent', platform_message_id=:platform_message_id, sent_at=NOW(), last_error=NULL WHERE delivery_id=:delivery_id"),
                {"delivery_id": delivery_id, "platform_message_id": platform_message_id or ""},
            )

    async def mark_failed(self, *, delivery_id: str, error: str) -> None:
        async with self._engine_obj().begin() as conn:
            await conn.execute(
                _sql_text(f"UPDATE {self.schema}.outbound_messages SET status='failed', last_error=:error WHERE delivery_id=:delivery_id"),
                {"delivery_id": delivery_id, "error": error},
            )
