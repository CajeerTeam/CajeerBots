from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator


SCHEMA_VERSION = 9


def _utc_now() -> str:
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')


def _as_utc_text(value: Any) -> str:
    if value is None:
        return ''
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.strftime('%Y-%m-%d %H:%M:%S')
    raw = str(value).strip()
    if not raw:
        return ''
    normalized = raw.replace('T', ' ').replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return normalized[:19]
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.strftime('%Y-%m-%d %H:%M:%S')


def _clean_tags(tags: list[str] | str | None) -> list[str]:
    if tags is None:
        return []
    if isinstance(tags, str):
        values = tags.split(',')
    else:
        values = tags
    return sorted({item.strip().lower() for item in values if str(item).strip()})


def _bool_to_int(value: bool) -> int:
    return 1 if value else 0


SCHEMA = '''
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS interactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    chat_id INTEGER,
    user_id INTEGER,
    username TEXT,
    command TEXT NOT NULL,
    ok INTEGER NOT NULL,
    details TEXT
);
CREATE INDEX IF NOT EXISTS idx_interactions_created_at ON interactions(created_at);
CREATE INDEX IF NOT EXISTS idx_interactions_user_id ON interactions(user_id);
CREATE INDEX IF NOT EXISTS idx_interactions_command ON interactions(command);

CREATE TABLE IF NOT EXISTS admin_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    chat_id INTEGER,
    user_id INTEGER,
    action TEXT NOT NULL,
    payload TEXT
);
CREATE INDEX IF NOT EXISTS idx_admin_actions_created_at ON admin_actions(created_at);

CREATE TABLE IF NOT EXISTS pending_broadcasts (
    user_id INTEGER PRIMARY KEY,
    chat_id INTEGER,
    message TEXT NOT NULL,
    target_scope TEXT NOT NULL DEFAULT 'all',
    target_tags TEXT NOT NULL DEFAULT '',
    media_kind TEXT NOT NULL DEFAULT '',
    media_ref TEXT NOT NULL DEFAULT '',
    message_thread_id INTEGER,
    disable_notification INTEGER NOT NULL DEFAULT 0,
    delivery_key TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    created_by_user_id INTEGER,
    message TEXT NOT NULL,
    target_scope TEXT NOT NULL DEFAULT 'all',
    target_tags TEXT NOT NULL DEFAULT '',
    media_kind TEXT NOT NULL DEFAULT '',
    media_ref TEXT NOT NULL DEFAULT '',
    message_thread_id INTEGER,
    disable_notification INTEGER NOT NULL DEFAULT 0,
    scheduled_for TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    sent_at TEXT,
    last_attempt_at TEXT,
    last_error TEXT,
    delivery_key TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_scheduled_broadcasts_status_time ON scheduled_broadcasts(status, scheduled_for);
CREATE INDEX IF NOT EXISTS idx_scheduled_broadcasts_retry ON scheduled_broadcasts(status, next_retry_at);

CREATE TABLE IF NOT EXISTS pending_links (
    code TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    chat_id INTEGER,
    username TEXT,
    player_name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT NOT NULL,
    consumed_at TEXT,
    verified_at TEXT,
    verified_payload TEXT
);
CREATE INDEX IF NOT EXISTS idx_pending_links_user_id ON pending_links(user_id);

CREATE TABLE IF NOT EXISTS linked_accounts (
    user_id INTEGER PRIMARY KEY,
    chat_id INTEGER,
    username TEXT,
    player_name TEXT NOT NULL,
    player_uuid TEXT,
    linked_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS link_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    event TEXT NOT NULL,
    code TEXT,
    user_id INTEGER,
    admin_user_id INTEGER,
    player_name TEXT,
    player_uuid TEXT,
    details TEXT
);
CREATE INDEX IF NOT EXISTS idx_link_events_created_at ON link_events(created_at);

CREATE TABLE IF NOT EXISTS chat_settings (
    chat_id INTEGER PRIMARY KEY,
    title TEXT,
    chat_type TEXT,
    allow_status INTEGER NOT NULL DEFAULT 1,
    allow_announcements INTEGER NOT NULL DEFAULT 1,
    allow_broadcasts INTEGER NOT NULL DEFAULT 1,
    tags TEXT NOT NULL DEFAULT '',
    default_thread_id INTEGER,
    disable_notifications INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS runtime_state (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS runtime_locks (
    name TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_idempotency_expires_at ON idempotency_keys(expires_at);

CREATE TABLE IF NOT EXISTS external_announcements (
    event_id TEXT PRIMARY KEY,
    tag TEXT,
    text TEXT NOT NULL,
    source_created_at TEXT,
    delivered_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_external_announcements_delivered_at ON external_announcements(delivered_at);

CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    source_type TEXT NOT NULL,
    source_id TEXT,
    chat_id INTEGER,
    payload_json TEXT NOT NULL,
    error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    last_retry_at TEXT,
    resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_dead_letter_status_created ON dead_letter_jobs(status, created_at);

CREATE TABLE IF NOT EXISTS external_announcement_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    tag TEXT,
    text TEXT NOT NULL,
    source_created_at TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    delivered_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(event_id, chat_id)
);
CREATE INDEX IF NOT EXISTS idx_ext_ann_delivery_status_time ON external_announcement_deliveries(status, next_retry_at, created_at);

CREATE TABLE IF NOT EXISTS rate_limit_hits (
    key TEXT PRIMARY KEY,
    last_seen_at REAL NOT NULL,
    hits INTEGER NOT NULL DEFAULT 1,
    expires_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rate_limit_hits_expires_at ON rate_limit_hits(expires_at);

CREATE TABLE IF NOT EXISTS broadcast_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    delivery_key TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_attempt_at TEXT,
    delivered_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(source_type, source_id, chat_id)
);
CREATE INDEX IF NOT EXISTS idx_broadcast_deliveries_status_time ON broadcast_deliveries(status, updated_at);
'''


@dataclass(slots=True)
class InteractionRecord:
    chat_id: int | None
    user_id: int | None
    username: str | None
    command: str
    ok: bool
    details: str = ''


@dataclass(slots=True)
class LinkedAccount:
    user_id: int
    chat_id: int | None
    username: str | None
    player_name: str
    player_uuid: str | None
    linked_at: str


@dataclass(slots=True)
class PendingLink:
    code: str
    user_id: int
    chat_id: int | None
    username: str | None
    player_name: str
    created_at: str
    expires_at: str
    verified_at: str | None = None
    verified_payload: str | None = None


@dataclass(slots=True)
class ChatSettings:
    chat_id: int
    title: str | None
    chat_type: str | None
    allow_status: bool
    allow_announcements: bool
    allow_broadcasts: bool
    tags: list[str]
    default_thread_id: int | None
    disable_notifications: bool
    created_at: str
    updated_at: str




def inspect_sqlite(path: Path | str) -> dict[str, Any]:
    sqlite_path = Path(path)
    if not sqlite_path.exists():
        raise FileNotFoundError(f"SQLite file not found: {sqlite_path}")
    connection = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        def _scalar(query: str, params: tuple[object, ...] = ()) -> Any:
            row = connection.execute(query, params).fetchone()
            if row is None:
                return None
            if isinstance(row, sqlite3.Row):
                return row[0]
            return row[0]

        pragma = connection.execute('PRAGMA journal_mode').fetchone()
        tables = {str(row['name']) for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

        schema_version = 0
        if 'schema_meta' in tables:
            value = _scalar("SELECT value FROM schema_meta WHERE key='schema_version'")
            if value is not None and str(value).isdigit():
                schema_version = int(str(value))

        dead_letters = int(_scalar("SELECT COUNT(*) FROM dead_letter_jobs WHERE status='pending'") or 0) if 'dead_letter_jobs' in tables else 0
        scheduled_backlog = int(_scalar("SELECT COUNT(*) FROM scheduled_broadcasts WHERE status='pending' AND scheduled_for <= datetime('now')") or 0) if 'scheduled_broadcasts' in tables else 0
        feed_backlog = int(_scalar("SELECT COUNT(*) FROM external_announcement_deliveries WHERE status='pending' AND (next_retry_at IS NULL OR next_retry_at <= datetime('now'))") or 0) if 'external_announcement_deliveries' in tables else 0
        active_locks = int(_scalar("SELECT COUNT(*) FROM runtime_locks WHERE expires_at > datetime('now')") or 0) if 'runtime_locks' in tables else 0
        broadcast_backlog = int(_scalar("SELECT COUNT(*) FROM broadcast_deliveries WHERE status IN ('pending','retry','failed')") or 0) if 'broadcast_deliveries' in tables else 0

        return {
            'journal_mode': str(pragma[0]) if pragma else 'unknown',
            'schema_version': schema_version,
            'dead_letters': dead_letters,
            'scheduled_backlog': scheduled_backlog,
            'feed_backlog': feed_backlog,
            'broadcast_backlog': broadcast_backlog,
            'active_locks': active_locks,
        }
    finally:
        connection.close()


class Database:
    backend_name = 'sqlite'

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA)
            self._migrate(connection)
            connection.commit()

    def _ensure_column(self, connection: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
        cols = {row['name'] for row in connection.execute(f'PRAGMA table_info({table})').fetchall()}
        if column not in cols:
            connection.execute(f'ALTER TABLE {table} ADD COLUMN {ddl}')

    def _schema_version(self, connection: sqlite3.Connection) -> int:
        row = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
        return int(row['value']) if row and str(row['value']).isdigit() else 0

    def _set_schema_version(self, connection: sqlite3.Connection, version: int) -> None:
        connection.execute(
            "INSERT INTO schema_meta (key, value, updated_at) VALUES ('schema_version', ?, datetime('now')) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')",
            (str(version),),
        )

    def _migrate(self, connection: sqlite3.Connection) -> None:
        version = self._schema_version(connection)
        if version < 1:
            self._set_schema_version(connection, 1)
            version = 1
        if version < 2:
            self._ensure_column(connection, 'pending_broadcasts', 'target_scope', "target_scope TEXT NOT NULL DEFAULT 'all'")
            self._ensure_column(connection, 'pending_broadcasts', 'target_tags', "target_tags TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, 'pending_links', 'verified_at', 'verified_at TEXT')
            self._ensure_column(connection, 'pending_links', 'verified_payload', 'verified_payload TEXT')
            self._set_schema_version(connection, 2)
            version = 2
        if version < 3:
            self._ensure_column(connection, 'chat_settings', 'default_thread_id', 'default_thread_id INTEGER')
            self._ensure_column(connection, 'chat_settings', 'disable_notifications', 'disable_notifications INTEGER NOT NULL DEFAULT 0')
            self._ensure_column(connection, 'scheduled_broadcasts', 'retry_count', 'retry_count INTEGER NOT NULL DEFAULT 0')
            self._ensure_column(connection, 'scheduled_broadcasts', 'next_retry_at', 'next_retry_at TEXT')
            self._ensure_column(connection, 'scheduled_broadcasts', 'last_attempt_at', 'last_attempt_at TEXT')
            self._set_schema_version(connection, 3)
            version = 3
        if version < 4:
            for table in ('pending_broadcasts', 'scheduled_broadcasts'):
                self._ensure_column(connection, table, 'media_kind', "media_kind TEXT NOT NULL DEFAULT ''")
                self._ensure_column(connection, table, 'media_ref', "media_ref TEXT NOT NULL DEFAULT ''")
                self._ensure_column(connection, table, 'message_thread_id', 'message_thread_id INTEGER')
                self._ensure_column(connection, table, 'disable_notification', 'disable_notification INTEGER NOT NULL DEFAULT 0')
                self._ensure_column(connection, table, 'delivery_key', "delivery_key TEXT NOT NULL DEFAULT ''")
            self._ensure_column(connection, 'scheduled_broadcasts', 'sent_at', 'sent_at TEXT')
            self._set_schema_version(connection, 4)
            version = 4
        if version < 5:
            connection.executescript('''
            CREATE TABLE IF NOT EXISTS external_announcement_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                tag TEXT,
                text TEXT NOT NULL,
                source_created_at TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                retry_count INTEGER NOT NULL DEFAULT 0,
                next_retry_at TEXT,
                delivered_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(event_id, chat_id)
            );
            CREATE INDEX IF NOT EXISTS idx_ext_ann_delivery_status_time ON external_announcement_deliveries(status, next_retry_at, created_at);
            CREATE TABLE IF NOT EXISTS rate_limit_hits (
                key TEXT PRIMARY KEY,
                last_seen_at REAL NOT NULL,
                hits INTEGER NOT NULL DEFAULT 1,
                expires_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_rate_limit_hits_expires_at ON rate_limit_hits(expires_at);
            ''')
            self._set_schema_version(connection, 5)
            version = 5
        if version < 6:
            connection.executescript('''
            CREATE TABLE IF NOT EXISTS broadcast_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                delivery_key TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                last_attempt_at TEXT,
                delivered_at TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(source_type, source_id, chat_id)
            );
            CREATE INDEX IF NOT EXISTS idx_broadcast_deliveries_status_time ON broadcast_deliveries(status, updated_at);
            ''')
            self._set_schema_version(connection, 6)
            version = 6
        self._set_schema_version(connection, SCHEMA_VERSION)

    def record_interaction(self, record: InteractionRecord) -> None:
        with self.connect() as connection:
            connection.execute('INSERT INTO interactions (chat_id, user_id, username, command, ok, details) VALUES (?, ?, ?, ?, ?, ?)', (record.chat_id, record.user_id, record.username, record.command, _bool_to_int(record.ok), record.details))
            connection.commit()

    def record_admin_action(self, *, chat_id: int | None, user_id: int | None, action: str, payload: str) -> None:
        with self.connect() as connection:
            connection.execute('INSERT INTO admin_actions (chat_id, user_id, action, payload) VALUES (?, ?, ?, ?)', (chat_id, user_id, action, payload))
            connection.commit()

    def count_interactions(self) -> int:
        with self.connect() as connection:
            row = connection.execute('SELECT COUNT(*) AS c FROM interactions').fetchone()
            return int(row['c']) if row else 0

    def basic_stats(self) -> dict[str, int]:
        with self.connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS total, COUNT(DISTINCT user_id) AS unique_users, COUNT(DISTINCT chat_id) AS unique_chats, SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS errors, SUM(CASE WHEN created_at >= datetime('now', '-1 day') THEN 1 ELSE 0 END) AS last_24h FROM interactions").fetchone()
            return {k: int(row[k] or 0) for k in row.keys()} if row else {'total': 0, 'unique_users': 0, 'unique_chats': 0, 'errors': 0, 'last_24h': 0}

    def top_commands(self, limit: int = 5) -> list[tuple[str, int]]:
        with self.connect() as connection:
            rows = connection.execute('SELECT command, COUNT(*) AS c FROM interactions GROUP BY command ORDER BY c DESC, command ASC LIMIT ?', (limit,)).fetchall()
            return [(str(row['command']), int(row['c'])) for row in rows]

    def runtime_value(self, key: str, default: str = '') -> str:
        with self.connect() as connection:
            row = connection.execute('SELECT value FROM runtime_state WHERE key = ?', (key,)).fetchone()
            return str(row['value']) if row and row['value'] is not None else default

    def set_runtime_value(self, key: str, value: str) -> None:
        with self.connect() as connection:
            connection.execute("INSERT INTO runtime_state (key, value, updated_at) VALUES (?, ?, datetime('now')) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')", (key, value))
            connection.commit()

    def increment_runtime_counter(self, key: str, delta: int = 1) -> int:
        current = int(self.runtime_value(key, '0') or '0') + delta
        self.set_runtime_value(key, str(current))
        return current

    def acquire_leader_lock(self, *, name: str, owner: str, ttl_seconds: int) -> bool:
        now = _utc_now()
        expires = (datetime.utcnow() + timedelta(seconds=ttl_seconds)).strftime('%Y-%m-%d %H:%M:%S')
        with self.connect() as connection:
            row = connection.execute('SELECT owner, expires_at FROM runtime_locks WHERE name = ?', (name,)).fetchone()
            if row and _as_utc_text(row['expires_at']) > now and str(row['owner']) != owner:
                return False
            connection.execute("INSERT INTO runtime_locks (name, owner, acquired_at, expires_at) VALUES (?, ?, ?, ?) ON CONFLICT(name) DO UPDATE SET owner=excluded.owner, acquired_at=excluded.acquired_at, expires_at=excluded.expires_at", (name, owner, now, expires))
            connection.commit()
            return True

    def claim_idempotency_key(self, key: str, *, ttl_seconds: int = 300) -> bool:
        expires = (datetime.utcnow() + timedelta(seconds=ttl_seconds)).strftime('%Y-%m-%d %H:%M:%S')
        now = _utc_now()
        with self.connect() as connection:
            connection.execute('DELETE FROM idempotency_keys WHERE expires_at <= ?', (now,))
            try:
                connection.execute('INSERT INTO idempotency_keys (key, expires_at) VALUES (?, ?)', (key, expires))
                connection.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def check_and_hit_rate_limit(self, key: str, *, cooldown_seconds: float) -> float:
        now_ts = datetime.utcnow().timestamp()
        expires_at = (datetime.utcnow() + timedelta(seconds=max(int(cooldown_seconds * 3), 60))).strftime('%Y-%m-%d %H:%M:%S')
        with self.connect() as connection:
            connection.execute('BEGIN IMMEDIATE')
            connection.execute("DELETE FROM rate_limit_hits WHERE expires_at <= datetime('now')")
            row = connection.execute('SELECT last_seen_at FROM rate_limit_hits WHERE key = ? LIMIT 1', (key,)).fetchone()
            if row is not None:
                elapsed = now_ts - float(row['last_seen_at'])
                if elapsed < cooldown_seconds:
                    remaining = max(cooldown_seconds - elapsed, 0.0)
                    connection.execute('UPDATE rate_limit_hits SET hits = hits + 1, expires_at = ? WHERE key = ?', (expires_at, key))
                    connection.commit()
                    return remaining
                connection.execute('UPDATE rate_limit_hits SET last_seen_at = ?, hits = hits + 1, expires_at = ? WHERE key = ?', (now_ts, expires_at, key))
            else:
                connection.execute('INSERT INTO rate_limit_hits (key, last_seen_at, hits, expires_at) VALUES (?, ?, 1, ?)', (key, now_ts, expires_at))
            connection.commit()
        return 0.0

    def save_pending_broadcast(self, *, user_id: int, chat_id: int | None, message: str, expires_at: str, target_scope: str, target_tags: list[str], media_kind: str = '', media_ref: str = '', message_thread_id: int | None = None, disable_notification: bool = False, delivery_key: str = '') -> None:
        with self.connect() as connection:
            connection.execute("INSERT INTO pending_broadcasts (user_id, chat_id, message, target_scope, target_tags, media_kind, media_ref, message_thread_id, disable_notification, delivery_key, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET chat_id=excluded.chat_id, message=excluded.message, target_scope=excluded.target_scope, target_tags=excluded.target_tags, media_kind=excluded.media_kind, media_ref=excluded.media_ref, message_thread_id=excluded.message_thread_id, disable_notification=excluded.disable_notification, delivery_key=excluded.delivery_key, created_at=datetime('now'), expires_at=excluded.expires_at", (user_id, chat_id, message, target_scope, ','.join(_clean_tags(target_tags)), media_kind, media_ref, message_thread_id, _bool_to_int(disable_notification), delivery_key, expires_at))
            connection.commit()

    def get_pending_broadcast(self, *, user_id: int) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute('SELECT * FROM pending_broadcasts WHERE user_id = ?', (user_id,)).fetchone()
            return dict(row) if row else None

    def clear_pending_broadcast(self, *, user_id: int) -> None:
        with self.connect() as connection:
            connection.execute('DELETE FROM pending_broadcasts WHERE user_id = ?', (user_id,))
            connection.commit()

    def schedule_broadcast(self, *, created_by_user_id: int, message: str, target_scope: str, target_tags: list[str], scheduled_for: str, media_kind: str = '', media_ref: str = '', message_thread_id: int | None = None, disable_notification: bool = False, delivery_key: str = '') -> int:
        with self.connect() as connection:
            cursor = connection.execute('INSERT INTO scheduled_broadcasts (created_by_user_id, message, target_scope, target_tags, media_kind, media_ref, message_thread_id, disable_notification, scheduled_for, delivery_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', (created_by_user_id, message, target_scope, ','.join(_clean_tags(target_tags)), media_kind, media_ref, message_thread_id, _bool_to_int(disable_notification), scheduled_for, delivery_key))
            connection.commit()
            return int(cursor.lastrowid)

    def list_scheduled_broadcasts(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute('SELECT * FROM scheduled_broadcasts ORDER BY scheduled_for ASC, id ASC LIMIT ?', (limit,)).fetchall()
            return [dict(row) for row in rows]

    def cancel_scheduled_broadcast(self, job_id: int) -> bool:
        with self.connect() as connection:
            cursor = connection.execute("UPDATE scheduled_broadcasts SET status='cancelled', last_attempt_at=datetime('now') WHERE id = ? AND status IN ('pending','retry')", (job_id,))
            connection.commit()
            return cursor.rowcount > 0

    def requeue_scheduled_broadcast(self, job_id: int) -> bool:
        with self.connect() as connection:
            cursor = connection.execute("UPDATE scheduled_broadcasts SET status='retry', next_retry_at=datetime('now'), last_error=NULL WHERE id = ? AND status IN ('failed','dead')", (job_id,))
            connection.commit()
            return cursor.rowcount > 0

    def due_scheduled_broadcasts(self, now_utc: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute("SELECT * FROM scheduled_broadcasts WHERE (status = 'pending' AND scheduled_for <= ?) OR (status = 'retry' AND COALESCE(next_retry_at, scheduled_for) <= ?) ORDER BY scheduled_for ASC, id ASC", (now_utc, now_utc)).fetchall()
            return [dict(row) for row in rows]

    def mark_scheduled_broadcast_sent(self, job_id: int) -> None:
        with self.connect() as connection:
            connection.execute("UPDATE scheduled_broadcasts SET status='sent', sent_at=datetime('now'), last_attempt_at=datetime('now'), last_error=NULL WHERE id = ?", (job_id,))
            connection.commit()

    def mark_scheduled_broadcast_retry(self, job_id: int, *, error: str, retry_count: int, next_retry_at: str) -> None:
        with self.connect() as connection:
            connection.execute("UPDATE scheduled_broadcasts SET status='retry', retry_count=?, next_retry_at=?, last_attempt_at=datetime('now'), last_error=? WHERE id = ?", (retry_count, next_retry_at, error[:2000], job_id))
            connection.commit()

    def mark_scheduled_broadcast_dead(self, job_id: int, *, error: str, retry_count: int) -> None:
        with self.connect() as connection:
            connection.execute("UPDATE scheduled_broadcasts SET status='dead', retry_count=?, last_attempt_at=datetime('now'), last_error=? WHERE id = ?", (retry_count, error[:2000], job_id))
            connection.commit()

    def enqueue_dead_letter(self, *, source_type: str, source_id: str | None, chat_id: int | None, payload: dict[str, Any], error: str, retry_count: int = 0) -> None:
        with self.connect() as connection:
            connection.execute('INSERT INTO dead_letter_jobs (source_type, source_id, chat_id, payload_json, error, retry_count) VALUES (?, ?, ?, ?, ?, ?)', (source_type, source_id, chat_id, json.dumps(payload, ensure_ascii=False), error[:4000], retry_count))
            connection.commit()

    def list_dead_letters(self, limit: int = 20, *, status: str | None = None) -> list[dict[str, Any]]:
        query = 'SELECT * FROM dead_letter_jobs'
        params: list[Any] = []
        if status:
            query += ' WHERE status = ?'
            params.append(status)
        query += ' ORDER BY created_at DESC, id DESC LIMIT ?'
        params.append(limit)
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def resolve_dead_letter(self, job_id: int) -> bool:
        with self.connect() as connection:
            cursor = connection.execute("UPDATE dead_letter_jobs SET status='resolved', resolved_at=datetime('now') WHERE id = ? AND status != 'resolved'", (job_id,))
            connection.commit()
            return cursor.rowcount > 0

    def create_pending_link(self, *, code: str, user_id: int, chat_id: int | None, username: str | None, player_name: str, expires_at: str) -> None:
        with self.connect() as connection:
            connection.execute('DELETE FROM pending_links WHERE user_id = ? AND consumed_at IS NULL', (user_id,))
            connection.execute('INSERT INTO pending_links (code, user_id, chat_id, username, player_name, expires_at) VALUES (?, ?, ?, ?, ?, ?)', (code, user_id, chat_id, username, player_name, expires_at))
            connection.commit()
        self.record_link_event(event='request', code=code, user_id=user_id, admin_user_id=None, player_name=player_name, player_uuid=None, details='created')

    def get_pending_link_by_user(self, *, user_id: int) -> PendingLink | None:
        with self.connect() as connection:
            row = connection.execute('SELECT * FROM pending_links WHERE user_id = ? AND consumed_at IS NULL ORDER BY created_at DESC LIMIT 1', (user_id,)).fetchone()
            return PendingLink(**dict(row)) if row else None

    def get_pending_link_by_code(self, *, code: str) -> PendingLink | None:
        with self.connect() as connection:
            row = connection.execute('SELECT * FROM pending_links WHERE code = ? AND consumed_at IS NULL LIMIT 1', (code,)).fetchone()
            return PendingLink(**dict(row)) if row else None

    def list_pending_links(self, limit: int = 20) -> list[PendingLink]:
        with self.connect() as connection:
            rows = connection.execute('SELECT * FROM pending_links WHERE consumed_at IS NULL ORDER BY created_at DESC LIMIT ?', (limit,)).fetchall()
            return [PendingLink(**dict(row)) for row in rows]

    def reject_pending_link(self, *, code: str, admin_user_id: int | None) -> bool:
        link = self.get_pending_link_by_code(code=code)
        if link is None:
            return False
        with self.connect() as connection:
            connection.execute("UPDATE pending_links SET consumed_at=datetime('now') WHERE code = ? AND consumed_at IS NULL", (code,))
            connection.commit()
        self.record_link_event(event='reject', code=code, user_id=link.user_id, admin_user_id=admin_user_id, player_name=link.player_name, player_uuid=None, details='rejected')
        return True

    def mark_pending_link_verified(self, *, code: str, payload: str) -> None:
        with self.connect() as connection:
            connection.execute("UPDATE pending_links SET verified_at=datetime('now'), verified_payload=? WHERE code = ?", (payload, code))
            connection.commit()

    def link_account(self, *, code: str, player_uuid: str | None) -> LinkedAccount | None:
        pending = self.get_pending_link_by_code(code=code)
        if pending is None:
            return None
        with self.connect() as connection:
            connection.execute("INSERT INTO linked_accounts (user_id, chat_id, username, player_name, player_uuid, linked_at) VALUES (?, ?, ?, ?, ?, datetime('now')) ON CONFLICT(user_id) DO UPDATE SET chat_id=excluded.chat_id, username=excluded.username, player_name=excluded.player_name, player_uuid=excluded.player_uuid, linked_at=datetime('now')", (pending.user_id, pending.chat_id, pending.username, pending.player_name, player_uuid))
            connection.execute("UPDATE pending_links SET consumed_at=datetime('now') WHERE code = ?", (code,))
            connection.commit()
        self.record_link_event(event='approve', code=code, user_id=pending.user_id, admin_user_id=None, player_name=pending.player_name, player_uuid=player_uuid, details='approved')
        return self.get_linked_account(user_id=pending.user_id)

    def get_linked_account(self, *, user_id: int) -> LinkedAccount | None:
        with self.connect() as connection:
            row = connection.execute('SELECT * FROM linked_accounts WHERE user_id = ? LIMIT 1', (user_id,)).fetchone()
            return LinkedAccount(**dict(row)) if row else None

    def unlink_account(self, *, user_id: int, admin_user_id: int | None = None) -> bool:
        account = self.get_linked_account(user_id=user_id)
        if account is None:
            return False
        with self.connect() as connection:
            connection.execute('DELETE FROM linked_accounts WHERE user_id = ?', (user_id,))
            connection.commit()
        self.record_link_event(event='unlink', code=None, user_id=user_id, admin_user_id=admin_user_id, player_name=account.player_name, player_uuid=account.player_uuid, details='unlinked')
        return True

    def count_linked_accounts(self) -> int:
        with self.connect() as connection:
            row = connection.execute('SELECT COUNT(*) AS c FROM linked_accounts').fetchone()
            return int(row['c']) if row else 0

    def record_link_event(self, *, event: str, code: str | None, user_id: int | None, admin_user_id: int | None, player_name: str | None, player_uuid: str | None, details: str | None) -> None:
        with self.connect() as connection:
            connection.execute('INSERT INTO link_events (event, code, user_id, admin_user_id, player_name, player_uuid, details) VALUES (?, ?, ?, ?, ?, ?, ?)', (event, code, user_id, admin_user_id, player_name, player_uuid, details))
            connection.commit()

    def list_link_events(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute('SELECT * FROM link_events ORDER BY created_at DESC, id DESC LIMIT ?', (limit,)).fetchall()
            return [dict(row) for row in rows]

    def touch_chat(self, *, chat_id: int, title: str | None, chat_type: str | None) -> ChatSettings:
        with self.connect() as connection:
            connection.execute("INSERT INTO chat_settings (chat_id, title, chat_type, updated_at) VALUES (?, ?, ?, datetime('now')) ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title, chat_type=excluded.chat_type, updated_at=datetime('now')", (chat_id, title, chat_type))
            connection.commit()
        return self.get_chat_settings(chat_id)

    def get_chat_settings(self, chat_id: int) -> ChatSettings | None:
        with self.connect() as connection:
            row = connection.execute('SELECT * FROM chat_settings WHERE chat_id = ? LIMIT 1', (chat_id,)).fetchone()
            return self._row_to_chat_settings(row) if row else None

    def update_chat_settings(self, chat_id: int, **updates: object) -> ChatSettings:
        allowed = {'allow_status', 'allow_announcements', 'allow_broadcasts', 'tags', 'default_thread_id', 'disable_notifications'}
        fields, values = [], []
        for key, value in updates.items():
            if key not in allowed:
                continue
            if key == 'tags':
                value = ','.join(_clean_tags(value if isinstance(value, list) else str(value)))
            elif key in {'allow_status', 'allow_announcements', 'allow_broadcasts', 'disable_notifications'}:
                value = _bool_to_int(bool(value))
            fields.append(f'{key} = ?')
            values.append(value)
        if not fields:
            return self.get_chat_settings(chat_id) or self.touch_chat(chat_id=chat_id, title=None, chat_type=None)
        with self.connect() as connection:
            connection.execute(f"UPDATE chat_settings SET {', '.join(fields)}, updated_at=datetime('now') WHERE chat_id = ?", (*values, chat_id))
            connection.commit()
        return self.get_chat_settings(chat_id)

    def list_chat_settings(self, *, chat_type: str | None = None, tag: str | None = None) -> list[ChatSettings]:
        query = 'SELECT * FROM chat_settings'
        params, conditions = [], []
        if chat_type:
            conditions.append('chat_type = ?')
            params.append(chat_type)
        if tag:
            conditions.append("(',' || tags || ',') LIKE ?")
            params.append(f'%,{tag.lower()},%')
        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)
        query += ' ORDER BY updated_at DESC, chat_id ASC'
        with self.connect() as connection:
            rows = connection.execute(query, params).fetchall()
            return [self._row_to_chat_settings(row) for row in rows]

    def bulk_update_chat_settings(self, *, chat_type: str | None, tag: str | None, updates: dict[str, Any]) -> int:
        targets = self.list_chat_settings(chat_type=chat_type, tag=tag)
        count = 0
        for item in targets:
            self.update_chat_settings(item.chat_id, **updates)
            count += 1
        return count

    def resolve_target_chats(self, *, allowed_chat_ids: set[int], fallback_chat_id: int | None, target_scope: str, target_tags: list[str], feature: str) -> list[int]:
        tag_set = set(_clean_tags(target_tags))
        known = {item.chat_id: item for item in self.list_chat_settings()}
        candidates = sorted(allowed_chat_ids) if allowed_chat_ids else ([fallback_chat_id] if fallback_chat_id is not None else [])
        resolved = []
        for chat_id in candidates:
            settings = known.get(chat_id)
            if settings is None:
                if target_scope in {'all', 'current'}:
                    resolved.append(chat_id)
                continue
            if target_scope == 'private' and settings.chat_type != 'private':
                continue
            if target_scope == 'groups' and settings.chat_type == 'private':
                continue
            if feature == 'announcements' and not settings.allow_announcements:
                continue
            if feature == 'broadcasts' and not settings.allow_broadcasts:
                continue
            if tag_set and not tag_set.intersection(settings.tags):
                continue
            resolved.append(chat_id)
        if target_scope == 'current' and fallback_chat_id is not None:
            return [fallback_chat_id]
        return sorted(set(resolved))

    def mark_external_announcement_delivered(self, *, event_id: str, tag: str | None, text: str, source_created_at: str | None) -> None:
        with self.connect() as connection:
            connection.execute('INSERT OR IGNORE INTO external_announcements (event_id, tag, text, source_created_at) VALUES (?, ?, ?, ?)', (event_id, tag, text, source_created_at))
            connection.commit()

    def has_external_announcement(self, event_id: str) -> bool:
        with self.connect() as connection:
            row = connection.execute('SELECT 1 FROM external_announcements WHERE event_id = ? LIMIT 1', (event_id,)).fetchone()
            return row is not None

    def enqueue_feed_deliveries(self, *, event_id: str, tag: str | None, text: str, source_created_at: str | None, chat_ids: list[int]) -> int:
        created = 0
        with self.connect() as connection:
            for chat_id in sorted(set(chat_ids)):
                cursor = connection.execute(
                    'INSERT OR IGNORE INTO external_announcement_deliveries (event_id, chat_id, tag, text, source_created_at) VALUES (?, ?, ?, ?, ?)',
                    (event_id, chat_id, tag, text, source_created_at),
                )
                created += cursor.rowcount
            connection.commit()
        return created

    def due_feed_deliveries(self, now: str, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT * FROM external_announcement_deliveries WHERE status IN ('pending', 'retry') AND (next_retry_at IS NULL OR next_retry_at <= ?) ORDER BY created_at ASC, id ASC LIMIT ?",
                (now, limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def mark_feed_delivery_sent(self, delivery_id: int) -> None:
        with self.connect() as connection:
            connection.execute("UPDATE external_announcement_deliveries SET status='sent', delivered_at=datetime('now'), updated_at=datetime('now'), last_error=NULL WHERE id = ?", (delivery_id,))
            connection.commit()

    def mark_feed_delivery_retry(self, delivery_id: int, *, error: str, retry_count: int, next_retry_at: str) -> None:
        with self.connect() as connection:
            connection.execute(
                "UPDATE external_announcement_deliveries SET status='retry', retry_count=?, next_retry_at=?, last_error=?, updated_at=datetime('now') WHERE id = ?",
                (retry_count, next_retry_at, error[:2000], delivery_id),
            )
            connection.commit()

    def mark_feed_delivery_dead(self, delivery_id: int, *, error: str, retry_count: int) -> None:
        with self.connect() as connection:
            connection.execute(
                "UPDATE external_announcement_deliveries SET status='dead', retry_count=?, last_error=?, updated_at=datetime('now') WHERE id = ?",
                (retry_count, error[:2000], delivery_id),
            )
            connection.commit()


    def ensure_broadcast_delivery(self, *, source_type: str, source_id: str, chat_id: int, delivery_key: str, payload: dict[str, Any]) -> None:
        with self.connect() as connection:
            connection.execute(
                "INSERT INTO broadcast_deliveries (source_type, source_id, chat_id, delivery_key, payload_json) VALUES (?, ?, ?, ?, ?) ON CONFLICT(source_type, source_id, chat_id) DO NOTHING",
                (source_type, source_id, chat_id, delivery_key, json.dumps(payload, ensure_ascii=False)),
            )
            connection.commit()

    def mark_broadcast_delivery_attempt(self, *, source_type: str, source_id: str, chat_id: int, error: str | None = None) -> None:
        with self.connect() as connection:
            connection.execute(
                "UPDATE broadcast_deliveries SET status='retry', attempt_count=attempt_count+1, last_attempt_at=datetime('now'), last_error=?, updated_at=datetime('now') WHERE source_type=? AND source_id=? AND chat_id=?",
                ((error or '')[:2000], source_type, source_id, chat_id),
            )
            connection.commit()

    def mark_broadcast_delivery_sent(self, *, source_type: str, source_id: str, chat_id: int) -> None:
        with self.connect() as connection:
            connection.execute(
                "UPDATE broadcast_deliveries SET status='sent', delivered_at=datetime('now'), last_error=NULL, updated_at=datetime('now') WHERE source_type=? AND source_id=? AND chat_id=?",
                (source_type, source_id, chat_id),
            )
            connection.commit()

    def mark_broadcast_delivery_failed(self, *, source_type: str, source_id: str, chat_id: int, error: str) -> None:
        with self.connect() as connection:
            connection.execute(
                "UPDATE broadcast_deliveries SET status='failed', attempt_count=attempt_count+1, last_attempt_at=datetime('now'), last_error=?, updated_at=datetime('now') WHERE source_type=? AND source_id=? AND chat_id=?",
                (error[:2000], source_type, source_id, chat_id),
            )
            connection.commit()

    def broadcast_delivery_is_sent(self, *, source_type: str, source_id: str, chat_id: int) -> bool:
        with self.connect() as connection:
            row = connection.execute('SELECT status FROM broadcast_deliveries WHERE source_type=? AND source_id=? AND chat_id=? LIMIT 1', (source_type, source_id, chat_id)).fetchone()
            return bool(row and str(row['status']) == 'sent')

    def list_broadcast_deliveries(self, *, source_type: str | None = None, source_id: str | None = None, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        query = 'SELECT * FROM broadcast_deliveries WHERE 1=1'
        params: list[Any] = []
        if source_type is not None:
            query += ' AND source_type = ?'
            params.append(source_type)
        if source_id is not None:
            query += ' AND source_id = ?'
            params.append(source_id)
        if status is not None:
            query += ' AND status = ?'
            params.append(status)
        query += ' ORDER BY updated_at DESC, id DESC LIMIT ?'
        params.append(limit)
        with self.connect() as connection:
            rows = connection.execute(query, tuple(params)).fetchall()
            return [dict(row) for row in rows]

    def get_dead_letter(self, job_id: int) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute('SELECT * FROM dead_letter_jobs WHERE id = ? LIMIT 1', (job_id,)).fetchone()
            return dict(row) if row else None

    def touch_dead_letter_retry(self, job_id: int, *, error: str | None = None) -> bool:
        with self.connect() as connection:
            cursor = connection.execute(
                "UPDATE dead_letter_jobs SET retry_count = retry_count + 1, last_retry_at=datetime('now'), error=COALESCE(?, error), status='pending' WHERE id = ?",
                (error[:4000] if error else None, job_id),
            )
            connection.commit()
            return cursor.rowcount > 0

    def list_active_locks(self, now: str | None = None) -> list[dict[str, Any]]:
        current = now or _utc_now()
        with self.connect() as connection:
            rows = connection.execute('SELECT name, owner, acquired_at, expires_at FROM runtime_locks WHERE expires_at > ? ORDER BY name ASC', (current,)).fetchall()
            return [dict(row) for row in rows]

    def db_health(self) -> dict[str, Any]:
        with self.connect() as connection:
            pragma = connection.execute('PRAGMA journal_mode').fetchone()
            row = connection.execute("SELECT COUNT(*) AS c FROM broadcast_deliveries WHERE status IN ('pending','retry','failed')").fetchone()
            broadcast_backlog = int(row['c']) if row else 0
            return {'journal_mode': str(pragma[0]) if pragma else 'unknown', 'schema_version': self._schema_version(connection), 'dead_letters': len(self.list_dead_letters(limit=1000, status='pending')), 'scheduled_backlog': len(self.due_scheduled_broadcasts(_utc_now())), 'feed_backlog': len(self.due_feed_deliveries(_utc_now(), limit=1000)), 'broadcast_backlog': broadcast_backlog, 'active_locks': len(self.list_active_locks())}

    def cleanup(self, *, interaction_retention_days: int, admin_action_retention_days: int, runtime_state_retention_days: int, dead_letter_retention_days: int = 30) -> dict[str, int]:
        with self.connect() as connection:
            counters = {'interactions': 0, 'admin_actions': 0, 'runtime_state': 0, 'idempotency_keys': 0, 'pending_links': 0, 'dead_letters': 0, 'link_events': 0, 'rate_limit_hits': 0}
            counters['interactions'] = connection.execute("DELETE FROM interactions WHERE created_at < datetime('now', ?)", (f'-{interaction_retention_days} day',)).rowcount
            counters['admin_actions'] = connection.execute("DELETE FROM admin_actions WHERE created_at < datetime('now', ?)", (f'-{admin_action_retention_days} day',)).rowcount
            counters['runtime_state'] = connection.execute("DELETE FROM runtime_state WHERE updated_at < datetime('now', ?)", (f'-{runtime_state_retention_days} day',)).rowcount
            counters['idempotency_keys'] = connection.execute("DELETE FROM idempotency_keys WHERE expires_at <= datetime('now')").rowcount
            counters['pending_links'] = connection.execute("DELETE FROM pending_links WHERE consumed_at IS NULL AND expires_at <= datetime('now')").rowcount
            counters['dead_letters'] = connection.execute("DELETE FROM dead_letter_jobs WHERE status = 'resolved' AND created_at < datetime('now', ?)", (f'-{dead_letter_retention_days} day',)).rowcount
            counters['rate_limit_hits'] = connection.execute("DELETE FROM rate_limit_hits WHERE expires_at <= datetime('now')").rowcount
            counters['link_events'] = connection.execute("DELETE FROM link_events WHERE created_at < datetime('now', '-180 day')").rowcount
            connection.commit()
            return counters


    def list_runtime_prefix(self, prefix: str, *, limit: int = 200) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT key, value, updated_at FROM runtime_state WHERE key LIKE ? ORDER BY key ASC LIMIT ?",
                (f"{prefix}%", limit),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_json_state(self, key: str, default: Any = None) -> Any:
        raw = self.runtime_value(key, '')
        if not raw:
            return default
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return default

    def set_json_state(self, key: str, value: Any) -> None:
        self.set_runtime_value(key, json.dumps(value, ensure_ascii=False, sort_keys=True))

    def get_maintenance_state(self) -> dict[str, Any]:
        payload = self.get_json_state('maintenance:state', default={}) or {}
        return {
            'active': bool(payload.get('active', False)),
            'message': str(payload.get('message', '') or ''),
            'updated_at': str(payload.get('updated_at', '') or ''),
            'updated_by': str(payload.get('updated_by', '') or ''),
        }

    def set_maintenance_state(self, *, active: bool, message: str = '', updated_by: str = '') -> None:
        self.set_json_state('maintenance:state', {
            'active': bool(active),
            'message': str(message or ''),
            'updated_at': _utc_now(),
            'updated_by': str(updated_by or ''),
        })

    def get_user_role_override(self, user_id: int) -> str | None:
        value = self.runtime_value(f'rbac:user:{user_id}', '')
        return value or None

    def set_user_role_override(self, user_id: int, role: str) -> None:
        self.set_runtime_value(f'rbac:user:{user_id}', role)

    def clear_user_role_override(self, user_id: int) -> None:
        with self.connect() as connection:
            connection.execute('DELETE FROM runtime_state WHERE key = ?', (f'rbac:user:{user_id}',))
            connection.commit()

    def get_required_role_override(self, command: str, *, chat_id: int | None = None) -> str | None:
        keys = []
        if chat_id is not None:
            keys.append(f'rbac:chat:{chat_id}:{command}')
        keys.append(f'rbac:global:{command}')
        with self.connect() as connection:
            for key in keys:
                row = connection.execute('SELECT value FROM runtime_state WHERE key = ? LIMIT 1', (key,)).fetchone()
                if row and row['value']:
                    return str(row['value'])
        return None

    def set_command_role_override(self, *, scope: str, command: str, role: str, chat_id: int | None = None) -> None:
        if scope == 'global':
            key = f'rbac:global:{command}'
        elif scope == 'chat' and chat_id is not None:
            key = f'rbac:chat:{chat_id}:{command}'
        else:
            raise ValueError('unsupported scope')
        self.set_runtime_value(key, role)

    def clear_command_role_override(self, *, scope: str, command: str, chat_id: int | None = None) -> None:
        if scope == 'global':
            key = f'rbac:global:{command}'
        elif scope == 'chat' and chat_id is not None:
            key = f'rbac:chat:{chat_id}:{command}'
        else:
            raise ValueError('unsupported scope')
        with self.connect() as connection:
            connection.execute('DELETE FROM runtime_state WHERE key = ?', (key,))
            connection.commit()

    def list_rbac_entries(self) -> list[dict[str, str]]:
        rows = self.list_runtime_prefix('rbac:', limit=500)
        entries: list[dict[str, str]] = []
        for row in rows:
            key = str(row['key'])
            parts = key.split(':')
            item = {'key': key, 'value': str(row['value']), 'updated_at': str(row['updated_at'])}
            if len(parts) >= 3 and parts[1] == 'user':
                item.update({'kind': 'user', 'target': parts[2], 'command': '-'})
            elif len(parts) >= 3 and parts[1] == 'global':
                item.update({'kind': 'global', 'target': '-', 'command': parts[2]})
            elif len(parts) >= 4 and parts[1] == 'chat':
                item.update({'kind': 'chat', 'target': parts[2], 'command': parts[3]})
            else:
                item.update({'kind': 'unknown', 'target': '-', 'command': '-'})
            entries.append(item)
        return entries

    def get_onboarding_status(self, chat_id: int) -> dict[str, Any] | None:
        return self.get_json_state(f'onboarding:chat:{chat_id}', default=None)

    def set_onboarding_status(self, *, chat_id: int, status: str, title: str = '', chat_type: str = '', updated_by: str = '') -> None:
        self.set_json_state(f'onboarding:chat:{chat_id}', {
            'chat_id': chat_id,
            'status': status,
            'title': title,
            'chat_type': chat_type,
            'updated_at': _utc_now(),
            'updated_by': updated_by,
        })

    def list_onboarding(self) -> list[dict[str, Any]]:
        rows = self.list_runtime_prefix('onboarding:chat:', limit=500)
        result: list[dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(str(row['value']))
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                result.append(payload)
        result.sort(key=lambda item: (str(item.get('status', '')), str(item.get('updated_at', ''))), reverse=True)
        return result

    def housekeeping(self) -> dict[str, Any]:
        with self.connect() as connection:
            checkpoint = connection.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
            connection.execute('ANALYZE')
            connection.commit()
            busy = int(checkpoint[0]) if checkpoint else 0
            log_frames = int(checkpoint[1]) if checkpoint and len(checkpoint) > 1 else 0
            ckpt_frames = int(checkpoint[2]) if checkpoint and len(checkpoint) > 2 else 0
        payload = {'busy': busy, 'log_frames': log_frames, 'checkpointed_frames': ckpt_frames, 'at': _utc_now()}
        self.set_json_state('housekeeping:last', payload)
        return payload

    def metrics_snapshot(self) -> dict[str, Any]:
        stats = self.basic_stats()
        health = self.db_health()
        return {
            'interactions_total': int(stats['total']),
            'interactions_24h': int(stats['last_24h']),
            'unique_users': int(stats['unique_users']),
            'unique_chats': int(stats['unique_chats']),
            'linked_accounts': int(self.count_linked_accounts()),
            'dead_letters': int(health['dead_letters']),
            'scheduled_backlog': int(health['scheduled_backlog']),
            'feed_backlog': int(health['feed_backlog']),
            'broadcast_backlog': int(health.get('broadcast_backlog', 0)),
            'active_locks': int(health['active_locks']),
            'rate_limit_rejections': int(self.runtime_value('rate_limit_rejections', '0') or '0'),
            'feed_sync_total': int(self.runtime_value('feed_sync_total', '0') or '0'),
            'scheduled_sent_total': int(self.runtime_value('scheduled_sent_total', '0') or '0'),
            'scheduled_failed_total': int(self.runtime_value('scheduled_failed_total', '0') or '0'),
        }

    def _row_to_chat_settings(self, row: sqlite3.Row) -> ChatSettings:
        return ChatSettings(chat_id=int(row['chat_id']), title=row['title'], chat_type=row['chat_type'], allow_status=bool(row['allow_status']), allow_announcements=bool(row['allow_announcements']), allow_broadcasts=bool(row['allow_broadcasts']), tags=_clean_tags(row['tags']), default_thread_id=int(row['default_thread_id']) if row['default_thread_id'] is not None else None, disable_notifications=bool(row['disable_notifications']), created_at=str(row['created_at']), updated_at=str(row['updated_at']))

# ---- Extended runtime patches (v1.5.0) ----
from dataclasses import dataclass as _dataclass


@_dataclass(slots=True)
class ChatRuntimeSettings:
    chat_id: int
    title: str | None
    chat_type: str | None
    allow_status: bool
    allow_announcements: bool
    allow_broadcasts: bool
    tags: list[str]
    default_thread_id: int | None
    disable_notifications: bool
    created_at: str
    updated_at: str
    chat_timezone: str = 'Europe/Berlin'
    quiet_hours_start: int = -1
    quiet_hours_end: int = -1
    feature_flags: dict[str, bool] | None = None
    shards: list[str] | None = None


def _normalize_feature_flags(value: Any) -> dict[str, bool]:
    if isinstance(value, dict):
        return {str(k): bool(v) for k, v in value.items()}
    if isinstance(value, str) and value.strip():
        try:
            payload = json.loads(value)
            if isinstance(payload, dict):
                return {str(k): bool(v) for k, v in payload.items()}
        except json.JSONDecodeError:
            result: dict[str, bool] = {}
            for item in value.split(','):
                part = item.strip()
                if not part:
                    continue
                if '=' in part:
                    k, v = part.split('=', 1)
                    result[k.strip()] = str(v).strip().lower() in {'1', 'true', 'yes', 'on'}
                else:
                    result[part] = True
            return result
    return {}


def _feature_enabled(settings: ChatRuntimeSettings | None, feature: str, default: bool = True) -> bool:
    if settings is None:
        return default
    flags = settings.feature_flags or {}
    return bool(flags.get(feature, default))


_OriginalDatabaseMigrate = Database._migrate


def _patched_migrate(self, connection: sqlite3.Connection) -> None:
    _OriginalDatabaseMigrate(self, connection)
    self._ensure_column(connection, 'chat_settings', 'chat_timezone', "chat_timezone TEXT NOT NULL DEFAULT 'Europe/Berlin'")
    self._ensure_column(connection, 'chat_settings', 'quiet_hours_start', 'quiet_hours_start INTEGER NOT NULL DEFAULT -1')
    self._ensure_column(connection, 'chat_settings', 'quiet_hours_end', 'quiet_hours_end INTEGER NOT NULL DEFAULT -1')
    self._ensure_column(connection, 'chat_settings', 'feature_flags', "feature_flags TEXT NOT NULL DEFAULT '{}' ")
    for column, ddl in {
        'media_kind': "media_kind TEXT NOT NULL DEFAULT ''",
        'media_ref': "media_ref TEXT NOT NULL DEFAULT ''",
        'buttons_json': "buttons_json TEXT NOT NULL DEFAULT '[]'",
        'priority': 'priority INTEGER NOT NULL DEFAULT 0',
        'silent': 'silent INTEGER NOT NULL DEFAULT 0',
        'parse_mode': "parse_mode TEXT NOT NULL DEFAULT ''",
    }.items():
        self._ensure_column(connection, 'external_announcement_deliveries', column, ddl)
    connection.executescript('''
    CREATE TABLE IF NOT EXISTS security_challenge_notices (
        challenge_id TEXT PRIMARY KEY,
        action TEXT NOT NULL DEFAULT '2fa',
        status TEXT NOT NULL DEFAULT 'pending',
        payload_json TEXT NOT NULL DEFAULT '{}',
        actor_user_id INTEGER,
        delivery_chat_id INTEGER,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_security_challenge_status ON security_challenge_notices(status, updated_at);
    ''')
    self._set_schema_version(connection, 7)


Database._migrate = _patched_migrate


def _patched_row_to_chat_settings(self, row: sqlite3.Row) -> ChatRuntimeSettings:
    return ChatRuntimeSettings(
        chat_id=int(row['chat_id']),
        title=row['title'],
        chat_type=row['chat_type'],
        allow_status=bool(row['allow_status']),
        allow_announcements=bool(row['allow_announcements']),
        allow_broadcasts=bool(row['allow_broadcasts']),
        tags=_clean_tags(row['tags']),
        default_thread_id=int(row['default_thread_id']) if row['default_thread_id'] is not None else None,
        disable_notifications=bool(row['disable_notifications']),
        created_at=str(row['created_at']),
        updated_at=str(row['updated_at']),
        chat_timezone=str(row['chat_timezone'] or 'Europe/Berlin') if 'chat_timezone' in row.keys() else 'Europe/Berlin',
        quiet_hours_start=int(row['quiet_hours_start']) if 'quiet_hours_start' in row.keys() and row['quiet_hours_start'] is not None else -1,
        quiet_hours_end=int(row['quiet_hours_end']) if 'quiet_hours_end' in row.keys() and row['quiet_hours_end'] is not None else -1,
        feature_flags=_normalize_feature_flags(row['feature_flags']) if 'feature_flags' in row.keys() else {},
    )


Database._row_to_chat_settings = _patched_row_to_chat_settings


_OriginalUpdateChatSettings = Database.update_chat_settings


def _patched_update_chat_settings(self, chat_id: int, **updates: object):
    allowed = {'allow_status', 'allow_announcements', 'allow_broadcasts', 'tags', 'shards', 'default_thread_id', 'disable_notifications', 'chat_timezone', 'quiet_hours_start', 'quiet_hours_end', 'feature_flags'}
    fields, values = [], []
    for key, value in updates.items():
        if key not in allowed:
            continue
        if key in {'tags', 'shards'}:
            value = ','.join(_clean_tags(value if isinstance(value, list) else str(value)))
        elif key in {'allow_status', 'allow_announcements', 'allow_broadcasts', 'disable_notifications'}:
            value = _bool_to_int(bool(value))
        elif key == 'feature_flags':
            value = json.dumps(_normalize_feature_flags(value), ensure_ascii=False, sort_keys=True)
        elif key in {'quiet_hours_start', 'quiet_hours_end'}:
            try:
                value = int(value) if value not in {None, ''} else -1
            except Exception:
                value = -1
        fields.append(f'{key} = ?')
        values.append(value)
    if not fields:
        return self.get_chat_settings(chat_id) or self.touch_chat(chat_id=chat_id, title=None, chat_type=None)
    with self.connect() as connection:
        connection.execute(f"UPDATE chat_settings SET {', '.join(fields)}, updated_at=datetime('now') WHERE chat_id = ?", (*values, chat_id))
        connection.commit()
    return self.get_chat_settings(chat_id)


Database.update_chat_settings = _patched_update_chat_settings


def get_chat_feature_flags(self, chat_id: int) -> dict[str, bool]:
    settings = self.get_chat_settings(chat_id)
    return dict(settings.feature_flags or {}) if settings else {}


def set_chat_feature_flag(self, chat_id: int, name: str, enabled: bool) -> None:
    flags = self.get_chat_feature_flags(chat_id)
    flags[str(name)] = bool(enabled)
    self.update_chat_settings(chat_id, feature_flags=flags)


Database.get_chat_feature_flags = get_chat_feature_flags
Database.set_chat_feature_flag = set_chat_feature_flag


def resolve_target_chats(self, *, allowed_chat_ids: set[int], fallback_chat_id: int | None, target_scope: str, target_tags: list[str], feature: str) -> list[int]:
    tag_set = set(_clean_tags(target_tags))
    known = {item.chat_id: item for item in self.list_chat_settings()}
    candidates = sorted(allowed_chat_ids) if allowed_chat_ids else ([fallback_chat_id] if fallback_chat_id is not None else [])
    resolved = []
    for chat_id in candidates:
        settings = known.get(chat_id)
        if settings is None:
            if target_scope in {'all', 'current'}:
                resolved.append(chat_id)
            continue
        if target_scope == 'private' and settings.chat_type != 'private':
            continue
        if target_scope == 'groups' and settings.chat_type == 'private':
            continue
        if feature == 'announcements' and (not settings.allow_announcements or not _feature_enabled(settings, 'announcements', True)):
            continue
        if feature == 'broadcasts' and (not settings.allow_broadcasts or not _feature_enabled(settings, 'broadcasts', True)):
            continue
        if feature == 'status' and (not settings.allow_status or not _feature_enabled(settings, 'status', True)):
            continue
        if tag_set and not tag_set.intersection(settings.tags):
            continue
        resolved.append(chat_id)
    if target_scope == 'current' and fallback_chat_id is not None:
        return [fallback_chat_id]
    return sorted(set(resolved))


Database.resolve_target_chats = resolve_target_chats


def should_deliver_now(self, *, chat_id: int, tag: str = '', force: bool = False, now_utc: datetime | None = None) -> tuple[bool, str]:
    if force:
        return True, ''
    settings = self.get_chat_settings(chat_id)
    if settings is None:
        return True, ''
    if tag and not _feature_enabled(settings, f'tag:{tag}', True) and settings.tags and tag not in settings.tags:
        return False, 'tag_unsubscribed'
    if not _feature_enabled(settings, 'delivery', True):
        return False, 'delivery_disabled'
    start = int(settings.quiet_hours_start)
    end = int(settings.quiet_hours_end)
    if start < 0 or end < 0 or start == end:
        return True, ''
    try:
        from zoneinfo import ZoneInfo
        now = now_utc or datetime.utcnow()
        local_hour = now.replace(tzinfo=ZoneInfo('UTC')).astimezone(ZoneInfo(settings.chat_timezone or 'Europe/Berlin')).hour
    except Exception:
        local_hour = (now_utc or datetime.utcnow()).hour
    in_quiet = start < end and start <= local_hour < end or start > end and (local_hour >= start or local_hour < end)
    if in_quiet and tag != 'maintenance':
        return False, f'quiet_hours:{start}-{end}'
    return True, ''


def next_delivery_not_before(self, *, chat_id: int, now_utc: datetime | None = None) -> str:
    settings = self.get_chat_settings(chat_id)
    if settings is None:
        return _utc_now()
    start = int(settings.quiet_hours_start)
    end = int(settings.quiet_hours_end)
    now = now_utc or datetime.utcnow()
    if start < 0 or end < 0 or start == end:
        return now.strftime('%Y-%m-%d %H:%M:%S')
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(settings.chat_timezone or 'Europe/Berlin')
        local = now.replace(tzinfo=ZoneInfo('UTC')).astimezone(tz)
        if start < end:
            target = local.replace(hour=end, minute=0, second=0, microsecond=0)
            if local.hour >= end:
                target = target + timedelta(days=1)
        else:
            if local.hour < end:
                target = local.replace(hour=end, minute=0, second=0, microsecond=0)
            else:
                target = (local + timedelta(days=1)).replace(hour=end, minute=0, second=0, microsecond=0)
        return target.astimezone(ZoneInfo('UTC')).strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return (now + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')


Database.should_deliver_now = should_deliver_now
Database.next_delivery_not_before = next_delivery_not_before


def upsert_security_challenge_notice(self, challenge_id: str, *, action: str, payload: dict[str, Any], delivery_chat_id: int | None = None) -> bool:
    with self.connect() as connection:
        cursor = connection.execute(
            "INSERT INTO security_challenge_notices (challenge_id, action, payload_json, delivery_chat_id) VALUES (?, ?, ?, ?) ON CONFLICT(challenge_id) DO NOTHING",
            (challenge_id, action, json.dumps(payload, ensure_ascii=False, sort_keys=True), delivery_chat_id),
        )
        connection.commit()
        return bool(cursor.rowcount)


def mark_security_challenge_notice(self, challenge_id: str, *, status: str, actor_user_id: int | None = None) -> None:
    with self.connect() as connection:
        connection.execute(
            "UPDATE security_challenge_notices SET status=?, actor_user_id=?, updated_at=datetime('now') WHERE challenge_id=?",
            (status, actor_user_id, challenge_id),
        )
        connection.commit()


def list_security_challenge_notices(self, *, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    query = 'SELECT * FROM security_challenge_notices'
    params: list[Any] = []
    if status:
        query += ' WHERE status = ?'
        params.append(status)
    query += ' ORDER BY created_at DESC LIMIT ?'
    params.append(limit)
    with self.connect() as connection:
        rows = connection.execute(query, params).fetchall()
        return [dict(row) for row in rows]


Database.upsert_security_challenge_notice = upsert_security_challenge_notice
Database.mark_security_challenge_notice = mark_security_challenge_notice
Database.list_security_challenge_notices = list_security_challenge_notices


_OriginalCleanup = Database.cleanup


def _patched_cleanup(self, *, interaction_retention_days: int = 30, admin_action_retention_days: int = 90, runtime_state_retention_days: int = 90, dead_letter_retention_days: int = 30) -> dict[str, int]:
    counters = _OriginalCleanup(self, interaction_retention_days=interaction_retention_days, admin_action_retention_days=admin_action_retention_days, runtime_state_retention_days=runtime_state_retention_days, dead_letter_retention_days=dead_letter_retention_days)
    link_days = int(self.runtime_value('config:link_history_retention_days', '90') or '90')
    security_days = int(self.runtime_value('config:security_history_retention_days', '30') or '30')
    with self.connect() as connection:
        counters['link_events'] = connection.execute("DELETE FROM link_events WHERE created_at < datetime('now', ?)", (f'-{link_days} day',)).rowcount
        counters['pending_links'] = connection.execute("DELETE FROM pending_links WHERE expires_at < datetime('now', '-7 day')").rowcount
        counters['security_challenge_notices'] = connection.execute("DELETE FROM security_challenge_notices WHERE updated_at < datetime('now', ?)", (f'-{security_days} day',)).rowcount
        connection.commit()
    return counters


Database.cleanup = _patched_cleanup


_OriginalDbHealth = Database.db_health


def _patched_db_health(self) -> dict[str, Any]:
    payload = _OriginalDbHealth(self)
    with self.connect() as connection:
        row = connection.execute("SELECT COUNT(*) AS c FROM security_challenge_notices WHERE status='pending'").fetchone()
        payload['security_pending'] = int(row['c']) if row else 0
    return payload


Database.db_health = _patched_db_health


def enqueue_feed_deliveries_rich(self, *, event_id: str, tag: str | None, text: str, source_created_at: str | None, chat_ids: list[int], media_kind: str = '', media_ref: str = '', buttons: list[dict[str, str]] | None = None, priority: int = 0, silent: bool = False, parse_mode: str = '') -> int:
    created = 0
    with self.connect() as connection:
        for chat_id in sorted(set(chat_ids)):
            cursor = connection.execute(
                'INSERT OR IGNORE INTO external_announcement_deliveries (event_id, chat_id, tag, text, source_created_at, status, media_kind, media_ref, buttons_json, priority, silent, parse_mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (event_id, chat_id, tag, text, source_created_at, 'pending', media_kind, media_ref, json.dumps(buttons or [], ensure_ascii=False), priority, _bool_to_int(silent), parse_mode),
            )
            created += cursor.rowcount
            connection.execute(
                "UPDATE external_announcement_deliveries SET text=?, tag=?, source_created_at=?, media_kind=?, media_ref=?, buttons_json=?, priority=?, silent=?, parse_mode=?, updated_at=datetime('now') WHERE event_id=? AND chat_id=?",
                (text, tag, source_created_at, media_kind, media_ref, json.dumps(buttons or [], ensure_ascii=False), priority, _bool_to_int(silent), parse_mode, event_id, chat_id),
            )
        connection.commit()
    return created


Database.enqueue_feed_deliveries_rich = enqueue_feed_deliveries_rich


_OriginalDueFeedDeliveries = Database.due_feed_deliveries


def _patched_due_feed_deliveries(self, now: str, limit: int = 100) -> list[dict[str, Any]]:
    rows = _OriginalDueFeedDeliveries(self, now, limit)
    for row in rows:
        row['media_kind'] = row.get('media_kind', '')
        row['media_ref'] = row.get('media_ref', '')
        try:
            row['buttons'] = json.loads(row.get('buttons_json') or '[]')
        except Exception:
            row['buttons'] = []
        row['priority'] = int(row.get('priority') or 0)
        row['silent'] = bool(row.get('silent') or 0)
        row['parse_mode'] = row.get('parse_mode', '')
    return rows


Database.due_feed_deliveries = _patched_due_feed_deliveries


# --- v1.6.0 extensions ---
SCHEMA_VERSION = 8

_OriginalMigrate_v160 = Database._migrate

def _migrate_v160(self, connection: sqlite3.Connection) -> None:
    _OriginalMigrate_v160(self, connection)
    version = self._schema_version(connection)
    if version < 8:
        connection.executescript("""
        CREATE TABLE IF NOT EXISTS external_replay_guards (
            source TEXT NOT NULL,
            replay_key TEXT NOT NULL,
            seen_at TEXT NOT NULL DEFAULT (datetime('now')),
            expires_at TEXT NOT NULL,
            PRIMARY KEY(source, replay_key)
        );
        CREATE INDEX IF NOT EXISTS idx_ext_replay_guards_exp ON external_replay_guards(expires_at);
        CREATE TABLE IF NOT EXISTS user_notification_prefs (
            user_id INTEGER PRIMARY KEY,
                    tags TEXT NOT NULL DEFAULT '',
            timezone TEXT NOT NULL DEFAULT 'Europe/Berlin',
            quiet_hours_start INTEGER NOT NULL DEFAULT -1,
            quiet_hours_end INTEGER NOT NULL DEFAULT -1,
            security_enabled INTEGER NOT NULL DEFAULT 1,
            status_enabled INTEGER NOT NULL DEFAULT 1,
            events_enabled INTEGER NOT NULL DEFAULT 1,
            maintenance_enabled INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS operator_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            incident_key TEXT NOT NULL UNIQUE,
            kind TEXT NOT NULL,
            severity TEXT NOT NULL DEFAULT 'warning',
            status TEXT NOT NULL DEFAULT 'open',
            summary TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '{}',
            count INTEGER NOT NULL DEFAULT 1,
            mute_until TEXT,
            acked_by INTEGER,
            resolved_by INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_sent_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_operator_alerts_status ON operator_alerts(status, updated_at);
        CREATE TABLE IF NOT EXISTS media_cache (
            media_key TEXT PRIMARY KEY,
            media_kind TEXT NOT NULL,
            media_ref TEXT NOT NULL,
            telegram_file_id TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """)
        self._set_schema_version(connection, 8)

Database._migrate = _migrate_v160

def claim_replay_guard(self, source: str, replay_key: str, ttl_seconds: int = 600) -> bool:
    replay_key = str(replay_key or '').strip()
    if not replay_key:
        return True
    expires_at = (datetime.utcnow() + timedelta(seconds=max(60, int(ttl_seconds)))).strftime('%Y-%m-%d %H:%M:%S')
    with self.connect() as connection:
        connection.execute("DELETE FROM external_replay_guards WHERE expires_at <= datetime('now')")
        cursor = connection.execute("INSERT OR IGNORE INTO external_replay_guards (source, replay_key, expires_at) VALUES (?, ?, ?)", (source, replay_key, expires_at))
        connection.commit()
        return bool(cursor.rowcount)

def get_user_notification_prefs(self, user_id: int) -> dict[str, Any]:
    with self.connect() as connection:
        row = connection.execute('SELECT * FROM user_notification_prefs WHERE user_id=? LIMIT 1', (user_id,)).fetchone()
        if not row:
            return {}
        data = dict(row)
        data['tags'] = _clean_tags(data.get('tags') or '')
        for key in ('security_enabled','status_enabled','events_enabled','maintenance_enabled'):
            data[key] = bool(data.get(key))
        return data

def update_user_notification_prefs(self, user_id: int, **updates: Any) -> dict[str, Any]:
    allowed = {'tags','timezone','quiet_hours_start','quiet_hours_end','security_enabled','status_enabled','events_enabled','maintenance_enabled'}
    normalized: dict[str, Any] = {}
    for key, value in updates.items():
        if key not in allowed:
            continue
        if key == 'tags':
            normalized[key] = ','.join(_clean_tags(value))
        elif key in {'quiet_hours_start','quiet_hours_end'}:
            try:
                normalized[key] = int(value) if value not in (None, '') else -1
            except Exception:
                normalized[key] = -1
        elif key in {'security_enabled','status_enabled','events_enabled','maintenance_enabled'}:
            normalized[key] = _bool_to_int(bool(value))
        else:
            normalized[key] = value
    if not normalized:
        return self.get_user_notification_prefs(user_id)
    cols = ', '.join(normalized.keys())
    placeholders = ', '.join('?' for _ in normalized)
    updates_sql = ', '.join(f"{k}=excluded.{k}" for k in normalized)
    with self.connect() as connection:
        connection.execute(f"INSERT INTO user_notification_prefs (user_id, {cols}) VALUES (?, {placeholders}) ON CONFLICT(user_id) DO UPDATE SET {updates_sql}, updated_at=datetime('now')", (user_id, *normalized.values()))
        connection.commit()
    return self.get_user_notification_prefs(user_id)

def find_linked_account_by_player_name(self, player_name: str) -> LinkedAccount | None:
    with self.connect() as connection:
        row = connection.execute('SELECT * FROM linked_accounts WHERE lower(player_name)=lower(?) ORDER BY linked_at DESC LIMIT 1', (player_name,)).fetchone()
        return LinkedAccount(**dict(row)) if row else None

def get_media_file_id(self, media_kind: str, media_ref: str) -> str:
    key = f"{media_kind}:{media_ref}"
    with self.connect() as connection:
        row = connection.execute('SELECT telegram_file_id FROM media_cache WHERE media_key=? LIMIT 1', (key,)).fetchone()
        return str(row['telegram_file_id']) if row else ''

def set_media_file_id(self, media_kind: str, media_ref: str, telegram_file_id: str) -> None:
    if not telegram_file_id:
        return
    key = f"{media_kind}:{media_ref}"
    with self.connect() as connection:
        connection.execute("INSERT INTO media_cache (media_key, media_kind, media_ref, telegram_file_id) VALUES (?, ?, ?, ?) ON CONFLICT(media_key) DO UPDATE SET telegram_file_id=excluded.telegram_file_id, updated_at=datetime('now')", (key, media_kind, media_ref, telegram_file_id))
        connection.commit()

def upsert_operator_alert(self, *, incident_key: str, kind: str, severity: str, summary: str, payload: dict[str, Any] | None = None) -> int:
    payload_json = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)
    with self.connect() as connection:
        row = connection.execute('SELECT id, count, status, mute_until FROM operator_alerts WHERE incident_key=? LIMIT 1', (incident_key,)).fetchone()
        if row:
            connection.execute("UPDATE operator_alerts SET count=count+1, severity=?, summary=?, payload_json=?, updated_at=datetime('now') WHERE incident_key=?", (severity, summary[:4000], payload_json, incident_key))
            alert_id = int(row['id'])
        else:
            cur = connection.execute("INSERT INTO operator_alerts (incident_key, kind, severity, summary, payload_json) VALUES (?, ?, ?, ?, ?)", (incident_key, kind, severity, summary[:4000], payload_json))
            alert_id = int(cur.lastrowid)
        connection.commit()
        return alert_id

def list_operator_alerts(self, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    query = 'SELECT * FROM operator_alerts'
    params: list[Any] = []
    if status:
        query += ' WHERE status=?'
        params.append(status)
    query += ' ORDER BY updated_at DESC, id DESC LIMIT ?'
    params.append(limit)
    with self.connect() as connection:
        return [dict(row) for row in connection.execute(query, params).fetchall()]

def ack_operator_alert(self, alert_id: int, user_id: int) -> bool:
    with self.connect() as connection:
        cur = connection.execute("UPDATE operator_alerts SET status='acked', acked_by=?, updated_at=datetime('now') WHERE id=? AND status!='resolved'", (user_id, alert_id))
        connection.commit()
        return bool(cur.rowcount)

def mute_operator_alert(self, alert_id: int, *, minutes: int, user_id: int) -> bool:
    until = (datetime.utcnow() + timedelta(minutes=max(1, minutes))).strftime('%Y-%m-%d %H:%M:%S')
    with self.connect() as connection:
        cur = connection.execute("UPDATE operator_alerts SET mute_until=?, acked_by=?, updated_at=datetime('now') WHERE id=? AND status!='resolved'", (until, user_id, alert_id))
        connection.commit()
        return bool(cur.rowcount)

def resolve_operator_alert(self, alert_id: int, user_id: int) -> bool:
    with self.connect() as connection:
        cur = connection.execute("UPDATE operator_alerts SET status='resolved', resolved_by=?, updated_at=datetime('now') WHERE id=? AND status!='resolved'", (user_id, alert_id))
        connection.commit()
        return bool(cur.rowcount)

def operator_alert_muted(self, incident_key: str) -> bool:
    with self.connect() as connection:
        row = connection.execute("SELECT mute_until, status FROM operator_alerts WHERE incident_key=? LIMIT 1", (incident_key,)).fetchone()
        if not row:
            return False
        if str(row['status']) == 'resolved':
            return True
        mute_until = row['mute_until']
        if mute_until:
            chk = connection.execute("SELECT datetime('now') < ? AS active", (mute_until,)).fetchone()
            return bool(chk['active']) if chk else False
        return False

def collect_incident_snapshot(self) -> dict[str, Any]:
    return {
        'generated_at': _utc_now(),
        'db_health': self.db_health(),
        'alerts': self.list_operator_alerts(limit=20),
        'active_locks': self.list_active_locks(),
        'top_commands': self.top_commands(10),
        'runtime_state': {k: v for k, v in self.list_runtime_state(limit=200).items()} if hasattr(self, 'list_runtime_state') else {},
    }

Database.claim_replay_guard = claim_replay_guard
Database.get_user_notification_prefs = get_user_notification_prefs
Database.update_user_notification_prefs = update_user_notification_prefs
Database.find_linked_account_by_player_name = find_linked_account_by_player_name
Database.get_media_file_id = get_media_file_id
Database.set_media_file_id = set_media_file_id
Database.upsert_operator_alert = upsert_operator_alert
Database.list_operator_alerts = list_operator_alerts
Database.ack_operator_alert = ack_operator_alert
Database.mute_operator_alert = mute_operator_alert
Database.resolve_operator_alert = resolve_operator_alert
Database.operator_alert_muted = operator_alert_muted
Database.collect_incident_snapshot = collect_incident_snapshot

_OriginalCleanup_v160 = Database.cleanup
def _cleanup_v160(self, **kwargs):
    counters = _OriginalCleanup_v160(self, **kwargs)
    with self.connect() as connection:
        counters['external_replay_guards'] = connection.execute("DELETE FROM external_replay_guards WHERE expires_at < datetime('now')").rowcount
        counters['operator_alerts'] = connection.execute("DELETE FROM operator_alerts WHERE status='resolved' AND updated_at < datetime('now', '-30 day')").rowcount
        connection.commit()
    return counters
Database.cleanup = _cleanup_v160

_OriginalDbHealth_v160 = Database.db_health
def _db_health_v160(self):
    payload = _OriginalDbHealth_v160(self)
    with self.connect() as connection:
        row = connection.execute("SELECT COUNT(*) AS c FROM operator_alerts WHERE status != 'resolved'").fetchone()
        payload['open_operator_alerts'] = int(row['c']) if row else 0
    return payload
Database.db_health = _db_health_v160




def _artifact_root_for_db(path: Path) -> Path:
    candidates = []
    try:
        candidates.append(path.parent.parent)
    except Exception:
        pass
    try:
        candidates.append(path.parent)
    except Exception:
        pass
    candidates.append(Path(tempfile.gettempdir()) / 'nmtelegrambot-runtime')
    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            if os.access(candidate, os.W_OK | os.X_OK):
                return candidate
        except Exception:
            continue
    fallback = Path(tempfile.gettempdir()) / 'nmtelegrambot-runtime'
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback

# --- v1.7.0 extensions ---
_OriginalMigrate_v170 = Database._migrate

def _migrate_v170(self, connection: sqlite3.Connection) -> None:
    version = self._schema_version(connection)
    if version < 9 and self.path.exists():
        artifact_root = _artifact_root_for_db(self.path)
        backup_dir = artifact_root / 'backups'
        manifest_dir = artifact_root / 'migrations'
        backup_dir.mkdir(parents=True, exist_ok=True)
        manifest_dir.mkdir(parents=True, exist_ok=True)
        backup_path = backup_dir / f"pre-migration-v{version}-to-v9-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.db"
        with sqlite3.connect(self.path) as src, sqlite3.connect(backup_path) as dst:
            src.backup(dst)
        manifest = {'from_version': version, 'to_version': 9, 'created_at': _utc_now(), 'backup_path': str(backup_path)}
        (manifest_dir / f"migration-v{version}-to-v9.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    _OriginalMigrate_v170(self, connection)
    version = self._schema_version(connection)
    if version < 9:
        connection.executescript("""
        CREATE TABLE IF NOT EXISTS approval_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}',
            requested_by INTEGER NOT NULL,
            requested_by_name TEXT NOT NULL DEFAULT '',
            required_role TEXT NOT NULL DEFAULT 'owner',
            status TEXT NOT NULL DEFAULT 'pending',
            acted_by INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            acted_at TEXT,
            result_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_approval_requests_status ON approval_requests(status, updated_at);
        CREATE TABLE IF NOT EXISTS external_sync_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_kind TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            payload_json TEXT NOT NULL DEFAULT '{}',
            destination TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            delivered_at TEXT,
            last_error TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_external_sync_events_status ON external_sync_events(status, updated_at);
        """)
        self._set_schema_version(connection, 9)

Database._migrate = _migrate_v170

def create_approval_request(self, *, kind: str, payload: dict[str, Any], requested_by: int, requested_by_name: str = '', required_role: str = 'owner') -> int:
    with self.connect() as connection:
        cur = connection.execute("INSERT INTO approval_requests (kind, payload_json, requested_by, requested_by_name, required_role) VALUES (?, ?, ?, ?, ?)", (kind, json.dumps(payload, ensure_ascii=False, sort_keys=True), requested_by, requested_by_name, required_role))
        connection.commit()
        return int(cur.lastrowid)

def list_approval_requests(self, *, status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    query = 'SELECT * FROM approval_requests'
    params: list[Any] = []
    if status:
        query += ' WHERE status=?'
        params.append(status)
    query += ' ORDER BY updated_at DESC, id DESC LIMIT ?'
    params.append(limit)
    with self.connect() as connection:
        return [dict(row) for row in connection.execute(query, params).fetchall()]

def get_approval_request(self, request_id: int) -> dict[str, Any] | None:
    with self.connect() as connection:
        row = connection.execute('SELECT * FROM approval_requests WHERE id=? LIMIT 1', (request_id,)).fetchone()
        return dict(row) if row else None

def resolve_approval_request(self, request_id: int, *, status: str, acted_by: int, result_json: dict[str, Any] | None = None) -> bool:
    with self.connect() as connection:
        cur = connection.execute("UPDATE approval_requests SET status=?, acted_by=?, acted_at=datetime('now'), updated_at=datetime('now'), result_json=? WHERE id=? AND status='pending'", (status, acted_by, json.dumps(result_json or {}, ensure_ascii=False, sort_keys=True), request_id))
        connection.commit()
        return bool(cur.rowcount)

def queue_external_sync_event(self, *, event_kind: str, destination: str, payload: dict[str, Any]) -> int:
    with self.connect() as connection:
        cur = connection.execute("INSERT INTO external_sync_events (event_kind, destination, payload_json) VALUES (?, ?, ?)", (event_kind, destination, json.dumps(payload, ensure_ascii=False, sort_keys=True)))
        connection.commit()
        return int(cur.lastrowid)

def mark_external_sync_event(self, event_id: int, *, status: str, error: str = '') -> None:
    with self.connect() as connection:
        if status == 'sent':
            connection.execute("UPDATE external_sync_events SET status=?, delivered_at=datetime('now'), updated_at=datetime('now'), last_error=? WHERE id=?", (status, error[:2000], event_id))
        else:
            connection.execute("UPDATE external_sync_events SET status=?, updated_at=datetime('now'), last_error=? WHERE id=?", (status, error[:2000], event_id))
        connection.commit()

def list_external_sync_events(self, *, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    q='SELECT * FROM external_sync_events'
    params=[]
    if status:
        q+=' WHERE status=?'
        params.append(status)
    q+=' ORDER BY updated_at DESC, id DESC LIMIT ?'
    params.append(limit)
    with self.connect() as connection:
        return [dict(r) for r in connection.execute(q, params).fetchall()]

def repair_runtime_state(self) -> dict[str, int]:
    counters={'stale_locks':0,'stuck_broadcast_deliveries':0,'stuck_feed_deliveries':0,'stuck_external_sync_events':0}
    with self.connect() as connection:
        counters['stale_locks'] = connection.execute("DELETE FROM runtime_locks WHERE expires_at <= datetime('now')").rowcount
        counters['stuck_broadcast_deliveries'] = connection.execute("UPDATE broadcast_deliveries SET status='retry', updated_at=datetime('now') WHERE status='retry' AND updated_at < datetime('now', '-30 minute')").rowcount
        counters['stuck_feed_deliveries'] = connection.execute("UPDATE external_announcement_deliveries SET status='retry', updated_at=datetime('now') WHERE status='retry' AND updated_at < datetime('now', '-30 minute')").rowcount
        counters['stuck_external_sync_events'] = connection.execute("UPDATE external_sync_events SET status='pending', updated_at=datetime('now') WHERE status='retry' AND updated_at < datetime('now', '-30 minute')").rowcount
        connection.commit()
    return counters

def enqueue_feed_from_push(self, payload: dict[str, Any], *, chat_ids: list[int]) -> int:
    return self.enqueue_feed_deliveries_rich(event_id=str(payload.get('event_id') or payload.get('id') or ''), tag=str(payload.get('tag') or ''), text=str(payload.get('text') or payload.get('message') or ''), source_created_at=str(payload.get('created_at') or ''), chat_ids=chat_ids, media_kind=str(payload.get('media_kind') or ''), media_ref=str(payload.get('media_ref') or payload.get('media_url') or ''), buttons=payload.get('buttons') if isinstance(payload.get('buttons'), list) else None, priority=int(payload.get('priority') or 0), silent=bool(payload.get('silent') or False), parse_mode=str(payload.get('parse_mode') or ''))

Database.create_approval_request = create_approval_request
Database.list_approval_requests = list_approval_requests
Database.get_approval_request = get_approval_request
Database.resolve_approval_request = resolve_approval_request
Database.queue_external_sync_event = queue_external_sync_event
Database.mark_external_sync_event = mark_external_sync_event
Database.list_external_sync_events = list_external_sync_events
Database.repair_runtime_state = repair_runtime_state
Database.enqueue_feed_from_push = enqueue_feed_from_push

_OriginalCollectIncident_v170 = Database.collect_incident_snapshot
def _collect_incident_snapshot_v170(self) -> dict[str, Any]:
    payload = _OriginalCollectIncident_v170(self)
    payload['approval_requests'] = self.list_approval_requests(limit=20)
    payload['external_sync_events'] = self.list_external_sync_events(limit=20)
    return payload
Database.collect_incident_snapshot = _collect_incident_snapshot_v170

_OriginalCleanup_v170 = Database.cleanup
def _cleanup_v170(self, **kwargs):
    counters = _OriginalCleanup_v170(self, **kwargs)
    with self.connect() as connection:
        counters['approval_requests'] = connection.execute("DELETE FROM approval_requests WHERE status!='pending' AND updated_at < datetime('now', '-30 day')").rowcount
        counters['external_sync_events'] = connection.execute("DELETE FROM external_sync_events WHERE status='sent' AND updated_at < datetime('now', '-30 day')").rowcount
        connection.commit()
    return counters
Database.cleanup = _cleanup_v170

_OriginalDbHealth_v170 = Database.db_health
def _db_health_v170(self):
    payload = _OriginalDbHealth_v170(self)
    with self.connect() as connection:
        row = connection.execute("SELECT COUNT(*) AS c FROM approval_requests WHERE status='pending'").fetchone()
        payload['pending_approvals'] = int(row['c']) if row else 0
        row = connection.execute("SELECT COUNT(*) AS c FROM external_sync_events WHERE status!='sent'").fetchone()
        payload['external_sync_backlog'] = int(row['c']) if row else 0
    return payload
Database.db_health = _db_health_v170


# --- v1.8.0 extensions ---
_OriginalMigrate_v180 = Database._migrate

def _migrate_v180(self, connection: sqlite3.Connection) -> None:
    _OriginalMigrate_v180(self, connection)
    self._ensure_column(connection, 'chat_settings', 'shards', "shards TEXT NOT NULL DEFAULT ''")
    self._set_schema_version(connection, 10)
Database._migrate = _migrate_v180

_OriginalPatchedRow_v180 = Database._row_to_chat_settings

def _row_to_chat_settings_v180(self, row: sqlite3.Row) -> ChatRuntimeSettings:
    item = _OriginalPatchedRow_v180(self, row)
    return ChatRuntimeSettings(
        chat_id=item.chat_id,
        title=item.title,
        chat_type=item.chat_type,
        allow_status=item.allow_status,
        allow_announcements=item.allow_announcements,
        allow_broadcasts=item.allow_broadcasts,
        tags=item.tags,
        default_thread_id=item.default_thread_id,
        disable_notifications=item.disable_notifications,
        created_at=item.created_at,
        updated_at=item.updated_at,
        chat_timezone=item.chat_timezone,
        quiet_hours_start=item.quiet_hours_start,
        quiet_hours_end=item.quiet_hours_end,
        feature_flags=item.feature_flags,
        shards=_clean_tags(row['shards']) if 'shards' in row.keys() else [],
    )
Database._row_to_chat_settings = _row_to_chat_settings_v180

_OriginalUpdate_v180 = Database.update_chat_settings

def _update_chat_settings_v180(self, chat_id: int, **updates: object):
    if 'shards' in updates:
        updates['shards'] = ','.join(_clean_tags(updates.get('shards')))
    return _OriginalUpdate_v180(self, chat_id, **updates)
Database.update_chat_settings = _update_chat_settings_v180

_OriginalResolveTargetChats_v180 = Database.resolve_target_chats

def resolve_target_chats_v180(self, *, allowed_chat_ids: set[int], fallback_chat_id: int | None, target_scope: str, target_tags: list[str], feature: str, target_shards: list[str] | None = None) -> list[int]:
    shard_set = set(_clean_tags(target_shards or []))
    resolved = _OriginalResolveTargetChats_v180(self, allowed_chat_ids=allowed_chat_ids, fallback_chat_id=fallback_chat_id, target_scope=target_scope, target_tags=target_tags, feature=feature)
    if not shard_set:
        return resolved
    out = []
    for chat_id in resolved:
        settings = self.get_chat_settings(chat_id)
        configured = set(getattr(settings, 'shards', []) or []) if settings else set()
        if configured and not configured.intersection(shard_set):
            continue
        out.append(chat_id)
    return out
Database.resolve_target_chats = resolve_target_chats_v180

_OriginalMetrics_v180 = Database.metrics_snapshot

def metrics_snapshot_v180(self) -> dict[str, Any]:
    payload = _OriginalMetrics_v180(self)
    payload.update({
        'delivery_sent_total': int(self.runtime_value('delivery_sent_total', '0') or '0'),
        'delivery_failed_total': int(self.runtime_value('delivery_failed_total', '0') or '0'),
        'external_sync_backlog': int(self.db_health().get('external_sync_backlog', 0)),
        'open_operator_alerts': int(self.db_health().get('open_operator_alerts', 0)),
        'pending_approvals': int(self.db_health().get('pending_approvals', 0)),
    })
    return payload
Database.metrics_snapshot = metrics_snapshot_v180


# --- v1.9 runtime state housekeeping ---
def cleanup_runtime_state(self, *, older_than_days: int = 30) -> dict[str, int]:
    prefixes = ('operator_alert_state:', 'operator_alert:', 'onboarding:wizard:', 'delivery:mode_switch:', 'compat:cache:')
    counters = {'runtime_values': 0}
    with self.connect() as connection:
        for prefix in prefixes:
            cur = connection.execute("DELETE FROM runtime_state WHERE key LIKE ? AND updated_at < datetime('now', ?)", (f'{prefix}%', f'-{max(1, older_than_days)} day'))
            counters['runtime_values'] += cur.rowcount
        connection.commit()
    return counters

Database.cleanup_runtime_state = cleanup_runtime_state


LATEST_SCHEMA_VERSION = 10
