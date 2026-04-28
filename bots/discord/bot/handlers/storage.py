from __future__ import annotations

import json
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiosqlite
import asyncpg
from redis.asyncio import Redis


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


SQLITE_MIGRATIONS: list[tuple[int, str]] = [
    (
        1,
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS key_value_store (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS relay_history (
            kind TEXT NOT NULL,
            item_id TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (kind, item_id)
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            action TEXT NOT NULL,
            actor_user_id TEXT,
            target_user_id TEXT,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS verification_sessions (
            discord_user_id TEXT NOT NULL,
            code TEXT NOT NULL,
            discord_username TEXT NOT NULL,
            status TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            expires_at TEXT,
            PRIMARY KEY (discord_user_id, code)
        );
        CREATE TABLE IF NOT EXISTS discord_links (
            discord_user_id TEXT PRIMARY KEY,
            minecraft_username TEXT NOT NULL,
            minecraft_uuid TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            linked_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """,
    ),
    (
        2,
        """
        CREATE INDEX IF NOT EXISTS idx_relay_history_created_at ON relay_history(created_at);
        CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON audit_log(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_log_action_created_at ON audit_log(action, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_discord_links_minecraft_uuid ON discord_links(minecraft_uuid);
        CREATE INDEX IF NOT EXISTS idx_verification_sessions_status_created_at ON verification_sessions(status, created_at DESC);
        """,
    ),
]

POSTGRES_MIGRATIONS: list[tuple[int, str]] = [
    (
        1,
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS key_value_store (
            key TEXT PRIMARY KEY,
            value_json JSONB NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS relay_history (
            kind TEXT NOT NULL,
            item_id TEXT NOT NULL,
            payload_json JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (kind, item_id)
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL,
            action TEXT NOT NULL,
            actor_user_id TEXT,
            target_user_id TEXT,
            status TEXT NOT NULL,
            payload_json JSONB NOT NULL
        );
        CREATE TABLE IF NOT EXISTS verification_sessions (
            discord_user_id TEXT NOT NULL,
            code TEXT NOT NULL,
            discord_username TEXT NOT NULL,
            status TEXT NOT NULL,
            metadata_json JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL,
            expires_at TIMESTAMPTZ,
            PRIMARY KEY (discord_user_id, code)
        );
        CREATE TABLE IF NOT EXISTS discord_links (
            discord_user_id TEXT PRIMARY KEY,
            minecraft_username TEXT NOT NULL,
            minecraft_uuid TEXT NOT NULL,
            metadata_json JSONB NOT NULL,
            linked_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );
        """,
    ),
    (
        2,
        """
        CREATE INDEX IF NOT EXISTS idx_relay_history_created_at ON relay_history(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON audit_log(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_audit_log_action_created_at ON audit_log(action, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_discord_links_minecraft_uuid ON discord_links(minecraft_uuid);
        CREATE INDEX IF NOT EXISTS idx_verification_sessions_status_created_at ON verification_sessions(status, created_at DESC);
        """,
    ),
]


class DatabaseBackend(ABC):
    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def healthcheck(self) -> None: ...

    @abstractmethod
    async def get_key_value(self, key: str) -> Any | None: ...

    @abstractmethod
    async def set_key_value(self, key: str, value: Any) -> None: ...

    @abstractmethod
    async def relay_item_exists(self, kind: str, item_id: str) -> bool: ...

    @abstractmethod
    async def remember_relay_item(self, kind: str, item_id: str, payload: dict[str, Any]) -> None: ...

    @abstractmethod
    async def append_audit_log(
        self,
        *,
        action: str,
        actor_user_id: int | None,
        target_user_id: int | None,
        status: str,
        payload: dict[str, Any],
    ) -> None: ...

    @abstractmethod
    async def list_recent_audit_entries(self, *, limit: int = 10, action: str | None = None) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def search_audit_entries(
        self,
        *,
        actor_user_id: str | None = None,
        target_user_id: str | None = None,
        status: str | None = None,
        category: str | None = None,
        hours: int | None = None,
        action: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]: ...

    @abstractmethod
    async def create_verification_session(
        self,
        *,
        discord_user_id: int,
        discord_username: str,
        code: str,
        expires_at: str,
        metadata: dict[str, Any],
    ) -> None: ...

    @abstractmethod
    async def complete_verification_session(
        self,
        *,
        discord_user_id: int,
        code: str,
        status: str,
        metadata: dict[str, Any],
    ) -> None: ...

    @abstractmethod
    async def upsert_link(
        self,
        *,
        discord_user_id: int,
        minecraft_username: str,
        minecraft_uuid: str,
        metadata: dict[str, Any],
    ) -> None: ...

    @abstractmethod
    async def get_link(self, discord_user_id: int) -> dict[str, Any] | None: ...

    @abstractmethod
    async def get_link_by_minecraft_uuid(self, minecraft_uuid: str) -> dict[str, Any] | None: ...

    @abstractmethod
    async def unlink(self, discord_user_id: int) -> None: ...


class SQLiteBackend(DatabaseBackend):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self._apply_migrations()

    async def _apply_migrations(self) -> None:
        assert self.conn is not None
        await self.conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
        cursor = await self.conn.execute("SELECT version FROM schema_migrations")
        applied = {int(row[0]) for row in await cursor.fetchall()}
        for version, sql in SQLITE_MIGRATIONS:
            if version in applied:
                continue
            await self.conn.executescript(sql)
            await self.conn.execute(
                "INSERT OR REPLACE INTO schema_migrations(version, applied_at) VALUES(?, ?)",
                (version, utc_now_iso()),
            )
            await self.conn.commit()

    async def close(self) -> None:
        if self.conn is not None:
            await self.conn.close()
            self.conn = None

    async def healthcheck(self) -> None:
        assert self.conn is not None
        await self.conn.execute("SELECT 1")

    async def get_key_value(self, key: str) -> Any | None:
        assert self.conn is not None
        cursor = await self.conn.execute("SELECT value_json FROM key_value_store WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return None if row is None else loads(row["value_json"], None)

    async def set_key_value(self, key: str, value: Any) -> None:
        assert self.conn is not None
        await self.conn.execute(
            "INSERT INTO key_value_store(key, value_json, updated_at) VALUES(?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
            (key, dumps(value), utc_now_iso()),
        )
        await self.conn.commit()

    async def relay_item_exists(self, kind: str, item_id: str) -> bool:
        assert self.conn is not None
        cursor = await self.conn.execute(
            "SELECT 1 FROM relay_history WHERE kind = ? AND item_id = ? LIMIT 1",
            (kind, item_id),
        )
        row = await cursor.fetchone()
        return row is not None

    async def remember_relay_item(self, kind: str, item_id: str, payload: dict[str, Any]) -> None:
        assert self.conn is not None
        await self.conn.execute(
            "INSERT OR IGNORE INTO relay_history(kind, item_id, payload_json, created_at) VALUES(?, ?, ?, ?)",
            (kind, item_id, dumps(payload), utc_now_iso()),
        )
        await self.conn.commit()

    async def append_audit_log(self, *, action: str, actor_user_id: int | None, target_user_id: int | None, status: str, payload: dict[str, Any]) -> None:
        assert self.conn is not None
        await self.conn.execute(
            "INSERT INTO audit_log(created_at, action, actor_user_id, target_user_id, status, payload_json) VALUES(?, ?, ?, ?, ?, ?)",
            (utc_now_iso(), action, str(actor_user_id) if actor_user_id is not None else None, str(target_user_id) if target_user_id is not None else None, status, dumps(payload)),
        )
        await self.conn.commit()

    async def list_recent_audit_entries(self, *, limit: int = 10, action: str | None = None) -> list[dict[str, Any]]:
        return await self.search_audit_entries(action=action, limit=limit)

    async def search_audit_entries(
        self,
        *,
        actor_user_id: str | None = None,
        target_user_id: str | None = None,
        status: str | None = None,
        category: str | None = None,
        hours: int | None = None,
        action: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        assert self.conn is not None
        sql = "SELECT created_at, action, actor_user_id, target_user_id, status, payload_json FROM audit_log"
        clauses: list[str] = []
        params: list[Any] = []
        if action:
            clauses.append("action = ?")
            params.append(action)
        if actor_user_id:
            clauses.append("actor_user_id = ?")
            params.append(actor_user_id)
        if target_user_id:
            clauses.append("target_user_id = ?")
            params.append(target_user_id)
        if status:
            clauses.append("LOWER(status) = LOWER(?)")
            params.append(status)
        if category:
            like_parts = {
                'security': ("verify%", "%security%", "sync_verified_role%"),
                'business': ("announce%", "relay_announcement%", "relay_event%"),
                'ops': tuple(),
            }
            patterns = like_parts.get(category.lower(), tuple())
            if patterns:
                clauses.append("(" + " OR ".join(["LOWER(action) LIKE LOWER(?)" for _ in patterns]) + ")")
                params.extend(patterns)
        if hours and hours > 0:
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
            clauses.append("created_at >= ?")
            params.append(cutoff)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        cursor = await self.conn.execute(sql, tuple(params))
        rows = await cursor.fetchall()
        result = []
        for row in rows:
            item = {
                "created_at": row["created_at"],
                "action": row["action"],
                "actor_user_id": row["actor_user_id"],
                "target_user_id": row["target_user_id"],
                "status": row["status"],
                "payload": loads(row["payload_json"], {}),
            }
            if category and category.lower() == 'ops':
                normalized = str(item.get('action') or '').lower()
                if normalized.startswith('verify') or 'security' in normalized or normalized.startswith('sync_verified_role') or normalized.startswith('announce') or normalized.startswith('relay_announcement') or normalized.startswith('relay_event'):
                    continue
            result.append(item)
        return result

    async def create_verification_session(self, *, discord_user_id: int, discord_username: str, code: str, expires_at: str, metadata: dict[str, Any]) -> None:
        assert self.conn is not None
        now = utc_now_iso()
        await self.conn.execute(
            "INSERT OR REPLACE INTO verification_sessions(discord_user_id, code, discord_username, status, metadata_json, created_at, updated_at, expires_at) "
            "VALUES(?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM verification_sessions WHERE discord_user_id = ? AND code = ?), ?), ?, ?)",
            (str(discord_user_id), code, discord_username, "started", dumps(metadata), str(discord_user_id), code, now, now, expires_at),
        )
        await self.conn.commit()

    async def complete_verification_session(self, *, discord_user_id: int, code: str, status: str, metadata: dict[str, Any]) -> None:
        assert self.conn is not None
        await self.conn.execute(
            "UPDATE verification_sessions SET status = ?, metadata_json = ?, updated_at = ? WHERE discord_user_id = ? AND code = ?",
            (status, dumps(metadata), utc_now_iso(), str(discord_user_id), code),
        )
        await self.conn.commit()

    async def upsert_link(self, *, discord_user_id: int, minecraft_username: str, minecraft_uuid: str, metadata: dict[str, Any]) -> None:
        assert self.conn is not None
        now = utc_now_iso()
        await self.conn.execute(
            "INSERT INTO discord_links(discord_user_id, minecraft_username, minecraft_uuid, metadata_json, linked_at, updated_at) VALUES(?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(discord_user_id) DO UPDATE SET minecraft_username=excluded.minecraft_username, minecraft_uuid=excluded.minecraft_uuid, "
            "metadata_json=excluded.metadata_json, updated_at=excluded.updated_at",
            (str(discord_user_id), minecraft_username, minecraft_uuid, dumps(metadata), now, now),
        )
        await self.conn.commit()

    async def get_link(self, discord_user_id: int) -> dict[str, Any] | None:
        assert self.conn is not None
        cursor = await self.conn.execute(
            "SELECT discord_user_id, minecraft_username, minecraft_uuid, metadata_json, linked_at, updated_at FROM discord_links WHERE discord_user_id = ?",
            (str(discord_user_id),),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return {
            "discord_user_id": row["discord_user_id"],
            "minecraft_username": row["minecraft_username"],
            "minecraft_uuid": row["minecraft_uuid"],
            "metadata": loads(row["metadata_json"], {}),
            "linked_at": row["linked_at"],
            "updated_at": row["updated_at"],
        }

    async def get_link_by_minecraft_uuid(self, minecraft_uuid: str) -> dict[str, Any] | None:
        assert self.conn is not None
        cursor = await self.conn.execute(
            "SELECT discord_user_id, minecraft_username, minecraft_uuid, metadata_json, linked_at, updated_at FROM discord_links WHERE minecraft_uuid = ? LIMIT 1",
            (minecraft_uuid,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        return {
            "discord_user_id": row["discord_user_id"],
            "minecraft_username": row["minecraft_username"],
            "minecraft_uuid": row["minecraft_uuid"],
            "metadata": loads(row["metadata_json"], {}),
            "linked_at": row["linked_at"],
            "updated_at": row["updated_at"],
        }

    async def unlink(self, discord_user_id: int) -> None:
        assert self.conn is not None
        await self.conn.execute("DELETE FROM discord_links WHERE discord_user_id = ?", (str(discord_user_id),))
        await self.conn.commit()

    async def optimize(self, *, vacuum: bool = False, analyze: bool = True) -> list[str]:
        assert self.conn is not None
        actions: list[str] = []
        await self.conn.execute("PRAGMA optimize")
        actions.append("PRAGMA optimize")
        if analyze:
            await self.conn.execute("ANALYZE")
            actions.append("ANALYZE")
        if vacuum:
            await self.conn.execute("VACUUM")
            actions.append("VACUUM")
        await self.conn.commit()
        return actions


class PostgresBackend(DatabaseBackend):
    def __init__(self, database_url: str, *, min_size: int, max_size: int) -> None:
        self.database_url = database_url
        self.min_size = min_size
        self.max_size = max_size
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(self.database_url, min_size=self.min_size, max_size=self.max_size)
        await self._apply_migrations()

    async def _apply_migrations(self) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL)")
            rows = await conn.fetch("SELECT version FROM schema_migrations")
            applied = {int(row["version"]) for row in rows}
            for version, sql in POSTGRES_MIGRATIONS:
                if version in applied:
                    continue
                async with conn.transaction():
                    await conn.execute(sql)
                    await conn.execute(
                        "INSERT INTO schema_migrations(version, applied_at) VALUES($1, NOW()) ON CONFLICT(version) DO NOTHING",
                        version,
                    )

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    async def healthcheck(self) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")

    async def get_key_value(self, key: str) -> Any | None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT value_json FROM key_value_store WHERE key = $1", key)
            return None if row is None else row["value_json"]

    async def set_key_value(self, key: str, value: Any) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO key_value_store(key, value_json, updated_at) VALUES($1, $2::jsonb, NOW()) "
                "ON CONFLICT(key) DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = EXCLUDED.updated_at",
                key,
                dumps(value),
            )

    async def relay_item_exists(self, kind: str, item_id: str) -> bool:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT 1 FROM relay_history WHERE kind = $1 AND item_id = $2", kind, item_id)
            return row is not None

    async def remember_relay_item(self, kind: str, item_id: str, payload: dict[str, Any]) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO relay_history(kind, item_id, payload_json, created_at) VALUES($1, $2, $3::jsonb, NOW()) ON CONFLICT DO NOTHING",
                kind,
                item_id,
                dumps(payload),
            )

    async def append_audit_log(self, *, action: str, actor_user_id: int | None, target_user_id: int | None, status: str, payload: dict[str, Any]) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO audit_log(created_at, action, actor_user_id, target_user_id, status, payload_json) VALUES(NOW(), $1, $2, $3, $4, $5::jsonb)",
                action,
                str(actor_user_id) if actor_user_id is not None else None,
                str(target_user_id) if target_user_id is not None else None,
                status,
                dumps(payload),
            )

    async def list_recent_audit_entries(self, *, limit: int = 10, action: str | None = None) -> list[dict[str, Any]]:
        return await self.search_audit_entries(action=action, limit=limit)

    async def search_audit_entries(
        self,
        *,
        actor_user_id: str | None = None,
        target_user_id: str | None = None,
        status: str | None = None,
        category: str | None = None,
        hours: int | None = None,
        action: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        assert self.pool is not None
        conditions: list[str] = []
        params: list[Any] = []
        if action:
            params.append(action)
            conditions.append(f"action = ${len(params)}")
        if actor_user_id:
            params.append(actor_user_id)
            conditions.append(f"actor_user_id = ${len(params)}")
        if target_user_id:
            params.append(target_user_id)
            conditions.append(f"target_user_id = ${len(params)}")
        if status:
            params.append(status)
            conditions.append(f"LOWER(status) = LOWER(${len(params)})")
        if category:
            patterns_map = {
                'security': ("verify%", "%security%", "sync_verified_role%"),
                'business': ("announce%", "relay_announcement%", "relay_event%"),
                'ops': tuple(),
            }
            patterns = patterns_map.get(category.lower(), tuple())
            if patterns:
                fragments = []
                for pattern in patterns:
                    params.append(pattern)
                    fragments.append(f"LOWER(action) LIKE LOWER(${len(params)})")
                conditions.append("(" + " OR ".join(fragments) + ")")
        if hours and hours > 0:
            params.append(datetime.now(timezone.utc) - timedelta(hours=hours))
            conditions.append(f"created_at >= ${len(params)}::timestamptz")
        sql = "SELECT created_at, action, actor_user_id, target_user_id, status, payload_json FROM audit_log"
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        params.append(limit)
        sql += f" ORDER BY id DESC LIMIT ${len(params)}"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        result = [
            {
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "action": row["action"],
                "actor_user_id": row["actor_user_id"],
                "target_user_id": row["target_user_id"],
                "status": row["status"],
                "payload": row["payload_json"],
            }
            for row in rows
        ]
        if category and category.lower() == 'ops':
            filtered = []
            for item in result:
                normalized = str(item.get('action') or '').lower()
                if normalized.startswith('verify') or 'security' in normalized or normalized.startswith('announce') or normalized.startswith('relay_announcement') or normalized.startswith('relay_event') or normalized.startswith('sync_verified_role'):
                    continue
                filtered.append(item)
            return filtered
        return result

    async def create_verification_session(self, *, discord_user_id: int, discord_username: str, code: str, expires_at: str, metadata: dict[str, Any]) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO verification_sessions(discord_user_id, code, discord_username, status, metadata_json, created_at, updated_at, expires_at) "
                "VALUES($1, $2, $3, 'started', $4::jsonb, NOW(), NOW(), NULLIF($5, '')::timestamptz) "
                "ON CONFLICT(discord_user_id, code) DO UPDATE SET discord_username = EXCLUDED.discord_username, status = EXCLUDED.status, metadata_json = EXCLUDED.metadata_json, updated_at = NOW(), expires_at = EXCLUDED.expires_at",
                str(discord_user_id),
                code,
                discord_username,
                dumps(metadata),
                expires_at,
            )

    async def complete_verification_session(self, *, discord_user_id: int, code: str, status: str, metadata: dict[str, Any]) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE verification_sessions SET status = $1, metadata_json = $2::jsonb, updated_at = NOW() WHERE discord_user_id = $3 AND code = $4",
                status,
                dumps(metadata),
                str(discord_user_id),
                code,
            )

    async def upsert_link(self, *, discord_user_id: int, minecraft_username: str, minecraft_uuid: str, metadata: dict[str, Any]) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO discord_links(discord_user_id, minecraft_username, minecraft_uuid, metadata_json, linked_at, updated_at) VALUES($1, $2, $3, $4::jsonb, NOW(), NOW()) "
                "ON CONFLICT(discord_user_id) DO UPDATE SET minecraft_username = EXCLUDED.minecraft_username, minecraft_uuid = EXCLUDED.minecraft_uuid, metadata_json = EXCLUDED.metadata_json, updated_at = NOW()",
                str(discord_user_id),
                minecraft_username,
                minecraft_uuid,
                dumps(metadata),
            )

    async def get_link(self, discord_user_id: int) -> dict[str, Any] | None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT discord_user_id, minecraft_username, minecraft_uuid, metadata_json, linked_at, updated_at FROM discord_links WHERE discord_user_id = $1",
                str(discord_user_id),
            )
        if row is None:
            return None
        return {
            "discord_user_id": row["discord_user_id"],
            "minecraft_username": row["minecraft_username"],
            "minecraft_uuid": row["minecraft_uuid"],
            "metadata": row["metadata_json"],
            "linked_at": row["linked_at"].isoformat() if row["linked_at"] else None,
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        }

    async def get_link_by_minecraft_uuid(self, minecraft_uuid: str) -> dict[str, Any] | None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT discord_user_id, minecraft_username, minecraft_uuid, metadata_json, linked_at, updated_at FROM discord_links WHERE minecraft_uuid = $1 LIMIT 1",
                minecraft_uuid,
            )
        if row is None:
            return None
        return {
            "discord_user_id": row["discord_user_id"],
            "minecraft_username": row["minecraft_username"],
            "minecraft_uuid": row["minecraft_uuid"],
            "metadata": row["metadata_json"],
            "linked_at": row["linked_at"].isoformat() if row["linked_at"] else None,
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        }

    async def unlink(self, discord_user_id: int) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM discord_links WHERE discord_user_id = $1", str(discord_user_id))


@dataclass(slots=True)
class RedisCache:
    url: str
    namespace: str
    relay_dedupe_ttl_seconds: int
    lock_ttl_seconds: int
    command_cooldown_seconds: int
    client: Redis | None = None

    async def connect(self) -> None:
        if not self.url:
            return
        self.client = Redis.from_url(self.url, decode_responses=True)
        await self.client.ping()

    async def close(self) -> None:
        if self.client is not None:
            await self.client.aclose()
            self.client = None

    async def healthcheck(self) -> None:
        if self.client is None:
            return
        await self.client.ping()

    def _key(self, suffix: str) -> str:
        return f"{self.namespace}:{suffix}"

    async def get_json(self, key: str) -> Any | None:
        if self.client is None:
            return None
        raw = await self.client.get(self._key(key))
        return loads(raw, None)

    async def set_json(self, key: str, value: Any, *, ex: int | None = None) -> None:
        if self.client is None:
            return
        await self.client.set(self._key(key), dumps(value), ex=ex)

    async def mark_seen(self, key: str, *, ex: int | None = None) -> bool:
        if self.client is None:
            return False
        return bool(await self.client.set(self._key(key), "1", ex=ex, nx=True))

    async def acquire_lock(self, key: str, *, ex: int | None = None) -> str | None:
        if self.client is None:
            return None
        token = str(uuid.uuid4())
        locked = await self.client.set(self._key(f"lock:{key}"), token, ex=ex or self.lock_ttl_seconds, nx=True)
        return token if locked else None

    async def release_lock(self, key: str, token: str | None) -> None:
        if self.client is None or not token:
            return
        redis_key = self._key(f"lock:{key}")
        current = await self.client.get(redis_key)
        if current == token:
            await self.client.delete(redis_key)

    async def check_and_set_cooldown(self, key: str, *, ex: int | None = None) -> int:
        if self.client is None:
            return 0
        redis_key = self._key(f"cooldown:{key}")
        created = await self.client.set(redis_key, "1", ex=ex or self.command_cooldown_seconds, nx=True)
        if created:
            return 0
        ttl = await self.client.ttl(redis_key)
        return max(int(ttl), 1)


def _utc_cutoff_iso(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


class StorageManager:
    def __init__(
        self,
        *,
        backend: str,
        database_url: str,
        sqlite_path: Path,
        postgres_pool_min_size: int,
        postgres_pool_max_size: int,
        redis_url: str,
        redis_namespace: str,
        redis_relay_dedupe_ttl_seconds: int,
        redis_lock_ttl_seconds: int,
        redis_command_cooldown_seconds: int,
        allow_degraded_without_redis: bool,
        sqlite_optimize_on_cleanup: bool,
        sqlite_analyze_on_cleanup: bool,
        sqlite_vacuum_min_interval_seconds: int,
    ) -> None:
        self.database: DatabaseBackend = (
            PostgresBackend(database_url, min_size=postgres_pool_min_size, max_size=postgres_pool_max_size)
            if backend == "postgresql"
            else SQLiteBackend(sqlite_path)
        )
        self.cache = RedisCache(
            redis_url,
            redis_namespace,
            redis_relay_dedupe_ttl_seconds,
            redis_lock_ttl_seconds,
            redis_command_cooldown_seconds,
        )
        self.allow_degraded_without_redis = allow_degraded_without_redis
        self.sqlite_optimize_on_cleanup = sqlite_optimize_on_cleanup
        self.sqlite_analyze_on_cleanup = sqlite_analyze_on_cleanup
        self.sqlite_vacuum_min_interval_seconds = sqlite_vacuum_min_interval_seconds
        self.redis_degraded = False
        self.redis_last_error: str | None = None

    @property
    def is_connected(self) -> bool:
        if isinstance(self.database, PostgresBackend):
            return self.database.pool is not None
        if isinstance(self.database, SQLiteBackend):
            return self.database.conn is not None
        return False

    async def connect(self) -> None:
        if not self.is_connected:
            await self.database.connect()
        try:
            await self.cache.connect()
            self.redis_degraded = False
            self.redis_last_error = None
        except Exception as exc:
            self.cache.client = None
            self.redis_degraded = bool(self.cache.url)
            self.redis_last_error = str(exc)
            if self.cache.url and not self.allow_degraded_without_redis:
                raise

    async def close(self) -> None:
        await self.cache.close()
        await self.database.close()

    async def healthcheck(self, *, strict_redis: bool = False) -> None:
        await self.database.healthcheck()
        if strict_redis:
            await self.cache.healthcheck()

    async def get_status_online(self) -> bool | None:
        cached = await self.cache.get_json("status_online")
        if isinstance(cached, bool):
            return cached
        value = await self.database.get_key_value("status_online")
        return value if isinstance(value, bool) else None

    async def set_status_online(self, online: bool) -> None:
        await self.database.set_key_value("status_online", online)
        await self.cache.set_json("status_online", online)

    async def is_known_relay_item(self, kind: str, item_id: str) -> bool:
        cache_key = f"relay_seen:{kind}:{item_id}"
        cache_first = await self.cache.mark_seen(cache_key, ex=self.cache.relay_dedupe_ttl_seconds)
        if self.cache.client is not None:
            return not cache_first
        return await self.database.relay_item_exists(kind, item_id)

    async def remember_relay_item(self, kind: str, item_id: str, payload: dict[str, Any]) -> None:
        await self.database.remember_relay_item(kind, item_id, payload)
        await self.cache.set_json(f"relay_payload:{kind}:{item_id}", payload, ex=self.cache.relay_dedupe_ttl_seconds)

    async def append_audit_log(self, *, action: str, actor_user_id: int | None, target_user_id: int | None, status: str, payload: dict[str, Any]) -> None:
        await self.database.append_audit_log(
            action=action,
            actor_user_id=actor_user_id,
            target_user_id=target_user_id,
            status=status,
            payload=payload,
        )

    async def list_recent_audit_entries(self, *, limit: int = 10, action: str | None = None) -> list[dict[str, Any]]:
        return await self.database.list_recent_audit_entries(limit=limit, action=action)

    async def search_audit_entries(
        self,
        *,
        actor_user_id: str | None = None,
        target_user_id: str | None = None,
        status: str | None = None,
        category: str | None = None,
        hours: int | None = None,
        action: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return await self.database.search_audit_entries(
            actor_user_id=actor_user_id,
            target_user_id=target_user_id,
            status=status,
            category=category,
            hours=hours,
            action=action,
            limit=limit,
        )

    async def create_verification_session(self, *, discord_user_id: int, discord_username: str, code: str, expires_at: str, metadata: dict[str, Any]) -> None:
        await self.database.create_verification_session(
            discord_user_id=discord_user_id,
            discord_username=discord_username,
            code=code,
            expires_at=expires_at,
            metadata=metadata,
        )
        await self.cache.set_json(
            f"verify_session:{discord_user_id}:{code}",
            {"expires_at": expires_at, "metadata": metadata},
            ex=self.cache.relay_dedupe_ttl_seconds,
        )

    async def complete_verification_session(self, *, discord_user_id: int, code: str, status: str, metadata: dict[str, Any]) -> None:
        await self.database.complete_verification_session(
            discord_user_id=discord_user_id,
            code=code,
            status=status,
            metadata=metadata,
        )

    async def upsert_link(self, *, discord_user_id: int, minecraft_username: str, minecraft_uuid: str, metadata: dict[str, Any]) -> None:
        await self.database.upsert_link(
            discord_user_id=discord_user_id,
            minecraft_username=minecraft_username,
            minecraft_uuid=minecraft_uuid,
            metadata=metadata,
        )
        await self.cache.set_json(
            f"link:{discord_user_id}",
            {"discord_user_id": str(discord_user_id), "minecraft_username": minecraft_username, "minecraft_uuid": minecraft_uuid, "metadata": metadata},
        )
        await self.cache.set_json(f"link_uuid:{minecraft_uuid}", {"discord_user_id": str(discord_user_id), "minecraft_username": minecraft_username}, ex=self.cache.relay_dedupe_ttl_seconds)

    async def get_link(self, discord_user_id: int) -> dict[str, Any] | None:
        cached = await self.cache.get_json(f"link:{discord_user_id}")
        if isinstance(cached, dict):
            return cached
        link = await self.database.get_link(discord_user_id)
        if link is not None:
            await self.cache.set_json(f"link:{discord_user_id}", link)
        return link

    async def get_link_by_minecraft_uuid(self, minecraft_uuid: str) -> dict[str, Any] | None:
        cached = await self.cache.get_json(f"link_uuid:{minecraft_uuid}")
        if isinstance(cached, dict) and cached.get("discord_user_id"):
            link = await self.database.get_link(int(cached["discord_user_id"]))
            if link is not None:
                return link
        link = await self.database.get_link_by_minecraft_uuid(minecraft_uuid)
        if link is not None:
            await self.cache.set_json(f"link_uuid:{minecraft_uuid}", {"discord_user_id": link["discord_user_id"], "minecraft_username": link["minecraft_username"]}, ex=self.cache.relay_dedupe_ttl_seconds)
        return link

    async def unlink(self, discord_user_id: int) -> None:
        link = await self.database.get_link(discord_user_id)
        await self.database.unlink(discord_user_id)
        if self.cache.client is not None:
            await self.cache.client.delete(self.cache._key(f"link:{discord_user_id}"))
            if link is not None and link.get("minecraft_uuid"):
                await self.cache.client.delete(self.cache._key(f"link_uuid:{link['minecraft_uuid']}"))

    async def purge_old_records(
        self,
        *,
        audit_log_retention_days: int,
        verification_session_retention_days: int,
        relay_history_retention_days: int,
    ) -> dict[str, int]:
        audit_cutoff = _utc_cutoff_iso(audit_log_retention_days)
        verification_cutoff = _utc_cutoff_iso(verification_session_retention_days)
        relay_cutoff = _utc_cutoff_iso(relay_history_retention_days)
        deleted = {"audit_log": 0, "verification_sessions": 0, "relay_history": 0}

        if isinstance(self.database, SQLiteBackend):
            conn = self.database.conn
            assert conn is not None
            cursor = await conn.execute("DELETE FROM audit_log WHERE created_at < ?", (audit_cutoff,))
            deleted["audit_log"] = cursor.rowcount if cursor.rowcount != -1 else 0
            cursor = await conn.execute(
                "DELETE FROM verification_sessions WHERE (expires_at IS NOT NULL AND expires_at < ?) OR updated_at < ?",
                (_utc_cutoff_iso(0), verification_cutoff),
            )
            deleted["verification_sessions"] = cursor.rowcount if cursor.rowcount != -1 else 0
            cursor = await conn.execute("DELETE FROM relay_history WHERE created_at < ?", (relay_cutoff,))
            deleted["relay_history"] = cursor.rowcount if cursor.rowcount != -1 else 0
            await conn.commit()
            return deleted

        if isinstance(self.database, PostgresBackend):
            pool = self.database.pool
            assert pool is not None
            async with pool.acquire() as conn:
                result = await conn.execute("DELETE FROM audit_log WHERE created_at < $1::timestamptz", audit_cutoff)
                deleted["audit_log"] = int(result.split()[-1])
                result = await conn.execute(
                    "DELETE FROM verification_sessions WHERE (expires_at IS NOT NULL AND expires_at < NOW()) OR updated_at < $1::timestamptz",
                    verification_cutoff,
                )
                deleted["verification_sessions"] = int(result.split()[-1])
                result = await conn.execute("DELETE FROM relay_history WHERE created_at < $1::timestamptz", relay_cutoff)
                deleted["relay_history"] = int(result.split()[-1])
            return deleted

        return deleted

    async def optimize_sqlite(self, *, deleted_rows: dict[str, int]) -> list[str]:
        if not self.sqlite_optimize_on_cleanup or not isinstance(self.database, SQLiteBackend):
            return []
        if sum(int(v) for v in deleted_rows.values()) <= 0:
            return []

        vacuum = False
        if self.sqlite_vacuum_min_interval_seconds >= 0:
            last_vacuum = await self.database.get_key_value("__sqlite_last_vacuum_ts__")
            try:
                last_vacuum_ts = float(last_vacuum) if last_vacuum is not None else 0.0
            except (TypeError, ValueError):
                last_vacuum_ts = 0.0
            vacuum = (time.time() - last_vacuum_ts) >= self.sqlite_vacuum_min_interval_seconds

        actions = await self.database.optimize(vacuum=vacuum, analyze=self.sqlite_analyze_on_cleanup)
        if vacuum:
            await self.database.set_key_value("__sqlite_last_vacuum_ts__", time.time())
        return actions

    async def acquire_lock(self, key: str) -> str | None:
        return await self.cache.acquire_lock(key)

    async def release_lock(self, key: str, token: str | None) -> None:
        await self.cache.release_lock(key, token)

    async def command_cooldown(self, *, discord_user_id: int, command_name: str) -> int:
        return await self.cache.check_and_set_cooldown(f"cmd:{command_name}:{discord_user_id}")
