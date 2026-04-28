from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


@dataclass(slots=True)
class OutboundRecord:
    row_id: int
    event_id: str
    target_url: str
    body_json: str
    headers_json: str
    status: str
    attempts: int
    next_attempt_at: int
    last_error: str | None = None
    last_http_status: int | None = None


class Storage:
    def __init__(self, *, database_url: str, sqlite_path: str, schema_prefix: str = 'nmvkbot') -> None:
        self.database_url = database_url.strip()
        self.schema_prefix = schema_prefix.strip() or 'nmvkbot'
        self.backend = 'postgres' if self.database_url.startswith(('postgres://', 'postgresql://')) else 'sqlite'
        self.sqlite_path = sqlite_path
        self._psycopg = None
        self._lock = threading.RLock()
        self._connect()

    def _connect(self) -> None:
        if self.backend == 'sqlite':
            Path(self.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(self.sqlite_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute('PRAGMA journal_mode=WAL')
            self._conn.execute('PRAGMA foreign_keys=ON')
            return
        try:
            import psycopg  # type: ignore
            from psycopg.rows import dict_row  # type: ignore
        except Exception as exc:  # pragma: no cover
            raise RuntimeError('psycopg is required for PostgreSQL mode') from exc
        self._psycopg = psycopg
        self._conn = psycopg.connect(self.database_url, row_factory=dict_row, connect_timeout=5)

    def _reconnect_postgres(self) -> None:
        if self.backend != 'postgres':
            return
        try:
            self._conn.close()
        except Exception:
            pass
        self._connect()

    def _with_retry(self, fn):
        if self.backend != 'postgres':
            return fn()
        for attempt in range(2):
            try:
                return fn()
            except Exception:
                if attempt == 0:
                    self._reconnect_postgres()
                    continue
                raise

    def _adapt_sql(self, sql: str) -> str:
        return sql if self.backend == 'sqlite' else sql.replace('?', '%s')

    def _execute(self, sql: str, params: Iterable[Any] = ()):  # noqa: ANN001
        def op():
            cur = self._conn.cursor()
            cur.execute(self._adapt_sql(sql), tuple(params))
            return cur
        return self._with_retry(op)

    def _fetchone_dict(self, sql: str, params: Iterable[Any] = ()) -> dict[str, Any] | None:
        cur = self._execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row is not None else None

    def _fetchall_dicts(self, sql: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
        cur = self._execute(sql, params)
        return [dict(row) for row in cur.fetchall()]

    def _commit(self) -> None:
        self._with_retry(lambda: self._conn.commit())

    def ping(self) -> bool:
        try:
            self._fetchone_dict('SELECT 1 AS ok')
            return True
        except Exception:
            return False

    def _table_name(self, base: str) -> str:
        return f'{self.schema_prefix}_{base}' if self.backend == 'postgres' else base

    def _sqlite_has_column(self, table: str, column: str) -> bool:
        rows = self._fetchall_dicts(f'PRAGMA table_info({table})')
        return any(str(row.get('name', '')) == column for row in rows)

    def _pg_has_column(self, table: str, column: str) -> bool:
        row = self._fetchone_dict(
            '''
            SELECT 1 AS ok
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?
            ''',
            (table, column),
        )
        return bool(row)

    def has_column(self, base_table: str, column: str) -> bool:
        table = self._table_name(base_table)
        if self.backend == 'sqlite':
            return self._sqlite_has_column(table, column)
        return self._pg_has_column(table, column)

    def _ensure_column(self, base_table: str, column: str, sql_type: str) -> None:
        if self.has_column(base_table, column):
            return
        table = self._table_name(base_table)
        self._execute(f'ALTER TABLE {table} ADD COLUMN {column} {sql_type}')
        self._commit()

    def _ensure_migrations_table(self) -> None:
        table = self._table_name('schema_migrations')
        self._execute(
            f'''
            CREATE TABLE IF NOT EXISTS {table} (
                version INTEGER PRIMARY KEY,
                applied_at INTEGER NOT NULL
            )
            '''
        )
        self._commit()

    def _applied_migrations(self) -> set[int]:
        rows = self._fetchall_dicts(f'SELECT version FROM {self._table_name("schema_migrations")}')
        return {int(row['version']) for row in rows}

    def _mark_migration(self, version: int) -> None:
        self._execute(
            f'INSERT INTO {self._table_name("schema_migrations")}(version, applied_at) VALUES (?, ?)',
            (version, int(time.time())),
        )
        self._commit()

    def initialize(self) -> None:
        with self._lock:
            self._ensure_migrations_table()
            applied = self._applied_migrations()
            for version, migration in (
                (1, self._migration_v1),
                (2, self._migration_v2),
                (3, self._migration_v3),
                (4, self._migration_v4),
                (5, self._migration_v5),
            ):
                if version not in applied:
                    migration()
                    self._mark_migration(version)
            self.cleanup_processed_events()

    def _migration_v1(self) -> None:
        support = self._table_name('support_tickets')
        outbound = self._table_name('outbound_events')
        support_pk = 'INTEGER PRIMARY KEY AUTOINCREMENT' if self.backend == 'sqlite' else 'BIGSERIAL PRIMARY KEY'
        outbound_pk = 'INTEGER PRIMARY KEY AUTOINCREMENT' if self.backend == 'sqlite' else 'BIGSERIAL PRIMARY KEY'
        self._execute(
            f'''
            CREATE TABLE IF NOT EXISTS {support} (
                ticket_row_id {support_pk},
                ticket_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                priority TEXT NOT NULL DEFAULT 'normal',
                user_id BIGINT NOT NULL,
                peer_id BIGINT NOT NULL,
                text TEXT NOT NULL,
                attachments_json TEXT NOT NULL DEFAULT '[]',
                correlation_id TEXT NOT NULL,
                bridge_event_id TEXT,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                resolved_by_user_id BIGINT,
                resolved_at BIGINT,
                assigned_to_user_id BIGINT,
                last_activity_at BIGINT,
                source_message_id BIGINT,
                last_reply_direction TEXT,
                last_actor_user_id BIGINT
            )
            '''
        )
        self._execute(
            f'''
            CREATE TABLE IF NOT EXISTS {outbound} (
                id {outbound_pk},
                event_id TEXT NOT NULL UNIQUE,
                target_url TEXT NOT NULL,
                body_json TEXT NOT NULL,
                headers_json TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at BIGINT NOT NULL,
                created_at BIGINT NOT NULL,
                updated_at BIGINT NOT NULL,
                last_error TEXT,
                last_http_status INTEGER,
                dead_letter_path TEXT,
                dead_reason TEXT,
                delivered_at BIGINT
            )
            '''
        )
        self._execute(f'CREATE INDEX IF NOT EXISTS idx_{self.schema_prefix}_outbound_due ON {outbound}(status, next_attempt_at)')
        self._commit()

    def _migration_v2(self) -> None:
        comments = self._table_name('ticket_comments')
        comment_pk = 'INTEGER PRIMARY KEY AUTOINCREMENT' if self.backend == 'sqlite' else 'BIGSERIAL PRIMARY KEY'
        self._execute(
            f'''
            CREATE TABLE IF NOT EXISTS {comments} (
                id {comment_pk},
                ticket_id TEXT NOT NULL,
                author_user_id BIGINT,
                author_role TEXT NOT NULL,
                body TEXT NOT NULL,
                direction TEXT NOT NULL,
                attachments_json TEXT NOT NULL DEFAULT '[]',
                created_at BIGINT NOT NULL
            )
            '''
        )
        self._execute(f'CREATE INDEX IF NOT EXISTS idx_{self.schema_prefix}_ticket_comments_ticket ON {comments}(ticket_id, created_at)')
        self._commit()

    def _migration_v3(self) -> None:
        processed = self._table_name('processed_events')
        self._execute(
            f'''
            CREATE TABLE IF NOT EXISTS {processed} (
                event_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                event_type TEXT NOT NULL,
                processed_at BIGINT NOT NULL,
                expires_at BIGINT
            )
            '''
        )
        self._execute(f'CREATE INDEX IF NOT EXISTS idx_{self.schema_prefix}_processed_expires ON {processed}(expires_at)')
        self._commit()

    def _migration_v4(self) -> None:
        self._ensure_column('outbound_events', 'dead_letter_path', 'TEXT')
        self._ensure_column('outbound_events', 'dead_reason', 'TEXT')
        self._ensure_column('outbound_events', 'delivered_at', 'BIGINT')
        self._commit()

    def _migration_v5(self) -> None:
        self._ensure_column('support_tickets', 'priority', "TEXT DEFAULT 'normal'")
        self._ensure_column('support_tickets', 'attachments_json', "TEXT DEFAULT '[]'")
        self._ensure_column('support_tickets', 'last_actor_user_id', 'BIGINT')
        self._ensure_column('ticket_comments', 'attachments_json', "TEXT DEFAULT '[]'")
        self._commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def database_diagnostics(self) -> dict[str, Any]:
        return {
            'backend': self.backend,
            'schema_prefix': self.schema_prefix,
            'sqlite_path': self.sqlite_path if self.backend == 'sqlite' else None,
            'connection_ok': self.ping(),
            'processed_events': self.processed_events_count(),
            'dead_letters': self.dead_outbound_count(),
        }

    def create_support_ticket(self, *, user_id: int, peer_id: int, text: str, source_message_id: int | None = None, attachments: list[str] | None = None) -> dict[str, Any]:
        now = int(time.time())
        ticket_id = f'VK-{time.strftime("%Y%m%d", time.gmtime(now))}-{uuid.uuid4().hex[:8].upper()}'
        correlation_id = uuid.uuid4().hex
        attachments_json = json.dumps(list(attachments or []), ensure_ascii=False)
        with self._lock:
            self._execute(
                f'''
                INSERT INTO {self._table_name('support_tickets')}(
                    ticket_id, status, priority, user_id, peer_id, text, attachments_json, correlation_id,
                    bridge_event_id, created_at, updated_at, resolved_by_user_id, resolved_at, assigned_to_user_id,
                    last_activity_at, source_message_id, last_reply_direction, last_actor_user_id
                ) VALUES (?, 'new', 'normal', ?, ?, ?, ?, ?, NULL, ?, ?, NULL, NULL, NULL, ?, ?, 'inbound', ?)
                ''',
                (ticket_id, user_id, peer_id, text, attachments_json, correlation_id, now, now, now, source_message_id, user_id),
            )
            self._execute(
                f'''
                INSERT INTO {self._table_name('ticket_comments')}(ticket_id, author_user_id, author_role, body, direction, attachments_json, created_at)
                VALUES (?, ?, 'user', ?, 'inbound', ?, ?)
                ''',
                (ticket_id, user_id, text, attachments_json, now),
            )
            self._commit()
        return self.get_ticket(ticket_id) or {'ticket_id': ticket_id, 'status': 'new', 'user_id': user_id, 'peer_id': peer_id, 'text': text, 'correlation_id': correlation_id}

    def get_ticket(self, ticket_id: str) -> dict[str, Any] | None:
        return self._fetchone_dict(f'SELECT * FROM {self._table_name("support_tickets")} WHERE ticket_id = ?', (ticket_id,))

    def bind_ticket_event(self, ticket_id: str, bridge_event_id: str) -> None:
        now = int(time.time())
        with self._lock:
            self._execute(
                f'UPDATE {self._table_name("support_tickets")} SET bridge_event_id = ?, updated_at = ?, last_activity_at = ? WHERE ticket_id = ?',
                (bridge_event_id, now, now, ticket_id),
            )
            self._commit()

    def list_tickets(self, *, status: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
        if status and status.lower() != 'all':
            return self._fetchall_dicts(
                f'SELECT * FROM {self._table_name("support_tickets")} WHERE status = ? ORDER BY last_activity_at DESC, created_at DESC LIMIT ?',
                (status.lower(), limit),
            )
        return self._fetchall_dicts(
            f'SELECT * FROM {self._table_name("support_tickets")} ORDER BY last_activity_at DESC, created_at DESC LIMIT ?',
            (limit,),
        )

    def list_open_tickets(self, limit: int = 10) -> list[dict[str, Any]]:
        return self._fetchall_dicts(
            f"SELECT * FROM {self._table_name('support_tickets')} WHERE status NOT IN ('resolved', 'closed') ORDER BY last_activity_at DESC, created_at DESC LIMIT ?",
            (limit,),
        )

    def open_tickets_count(self) -> int:
        row = self._fetchone_dict(f"SELECT COUNT(1) AS count FROM {self._table_name('support_tickets')} WHERE status NOT IN ('resolved', 'closed')")
        return int(row['count']) if row else 0

    def add_ticket_comment(self, *, ticket_id: str, body: str, author_user_id: int | None, author_role: str, direction: str, attachments: list[str] | None = None) -> bool:
        now = int(time.time())
        attachments_json = json.dumps(list(attachments or []), ensure_ascii=False)
        with self._lock:
            if not self.get_ticket(ticket_id):
                return False
            self._execute(
                f'INSERT INTO {self._table_name("ticket_comments")}(ticket_id, author_user_id, author_role, body, direction, attachments_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
                (ticket_id, author_user_id, author_role, body, direction, attachments_json, now),
            )
            self._execute(
                f'UPDATE {self._table_name("support_tickets")} SET updated_at = ?, last_activity_at = ?, last_reply_direction = ?, last_actor_user_id = ? WHERE ticket_id = ?',
                (now, now, direction, author_user_id, ticket_id),
            )
            self._commit()
            return True

    def list_ticket_comments(self, ticket_id: str, limit: int = 50) -> list[dict[str, Any]]:
        return self._fetchall_dicts(
            f'SELECT * FROM {self._table_name("ticket_comments")} WHERE ticket_id = ? ORDER BY created_at ASC LIMIT ?',
            (ticket_id, limit),
        )

    def update_ticket_status(self, ticket_id: str, status: str, *, actor_user_id: int) -> bool:
        now = int(time.time())
        resolved_at = now if status in {'resolved', 'closed'} else None
        with self._lock:
            cur = self._execute(
                f'''
                UPDATE {self._table_name('support_tickets')}
                SET status = ?, updated_at = ?, last_activity_at = ?, last_actor_user_id = ?,
                    resolved_by_user_id = CASE WHEN ? IS NULL THEN resolved_by_user_id ELSE ? END,
                    resolved_at = CASE WHEN ? IS NULL THEN resolved_at ELSE ? END
                WHERE ticket_id = ?
                ''',
                (status, now, now, actor_user_id, resolved_at, actor_user_id if resolved_at else None, resolved_at, resolved_at, ticket_id),
            )
            ok = (cur.rowcount or 0) > 0
            if ok:
                self._execute(
                    f'INSERT INTO {self._table_name("ticket_comments")}(ticket_id, author_user_id, author_role, body, direction, attachments_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (ticket_id, actor_user_id, 'staff', f'status -> {status}', 'internal', '[]', now),
                )
            self._commit()
            return ok

    def assign_ticket(self, ticket_id: str, *, assigned_to_user_id: int, actor_user_id: int) -> bool:
        now = int(time.time())
        with self._lock:
            cur = self._execute(
                f'UPDATE {self._table_name("support_tickets")} SET assigned_to_user_id = ?, updated_at = ?, last_activity_at = ?, last_actor_user_id = ? WHERE ticket_id = ?',
                (assigned_to_user_id, now, now, actor_user_id, ticket_id),
            )
            ok = (cur.rowcount or 0) > 0
            if ok:
                self._execute(
                    f'INSERT INTO {self._table_name("ticket_comments")}(ticket_id, author_user_id, author_role, body, direction, attachments_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (ticket_id, actor_user_id, 'staff', f'assigned -> {assigned_to_user_id}', 'internal', '[]', now),
                )
            self._commit()
            return ok

    def set_ticket_priority(self, ticket_id: str, priority: str, *, actor_user_id: int) -> bool:
        now = int(time.time())
        with self._lock:
            cur = self._execute(
                f'UPDATE {self._table_name("support_tickets")} SET priority = ?, updated_at = ?, last_activity_at = ?, last_actor_user_id = ? WHERE ticket_id = ?',
                (priority, now, now, actor_user_id, ticket_id),
            )
            ok = (cur.rowcount or 0) > 0
            if ok:
                self._execute(
                    f'INSERT INTO {self._table_name("ticket_comments")}(ticket_id, author_user_id, author_role, body, direction, attachments_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
                    (ticket_id, actor_user_id, 'staff', f'priority -> {priority}', 'internal', '[]', now),
                )
            self._commit()
            return ok

    def reopen_ticket(self, ticket_id: str, *, actor_user_id: int) -> bool:
        return self.update_ticket_status(ticket_id, 'triaged', actor_user_id=actor_user_id)

    def resolve_ticket(self, ticket_id: str, *, actor_user_id: int) -> bool:
        return self.update_ticket_status(ticket_id, 'resolved', actor_user_id=actor_user_id)

    def enqueue_outbound(self, *, event_id: str, target_url: str, body_json: str, headers_json: str) -> None:
        now = int(time.time())
        with self._lock:
            self._execute(
                f'''
                INSERT INTO {self._table_name('outbound_events')}(event_id, target_url, body_json, headers_json, status, attempts, next_attempt_at, created_at, updated_at, delivered_at)
                VALUES (?, ?, ?, ?, 'queued', 0, ?, ?, ?, NULL)
                ON CONFLICT(event_id) DO NOTHING
                ''',
                (event_id, target_url, body_json, headers_json, now, now, now),
            )
            self._commit()

    def fetch_due_outbound(self, limit: int = 25) -> list[OutboundRecord]:
        now = int(time.time())
        rows = self._fetchall_dicts(
            f'''
            SELECT id AS row_id, event_id, target_url, body_json, headers_json, status, attempts, next_attempt_at, last_error, last_http_status
            FROM {self._table_name('outbound_events')}
            WHERE status IN ('queued', 'retry') AND next_attempt_at <= ?
            ORDER BY next_attempt_at ASC, id ASC LIMIT ?
            ''',
            (now, limit),
        )
        return [OutboundRecord(**row) for row in rows]

    def list_outbound(self, limit: int = 10, *, include_dead: bool = False) -> list[dict[str, Any]]:
        where = '' if include_dead else "WHERE status != 'dead'"
        return self._fetchall_dicts(f'SELECT * FROM {self._table_name("outbound_events")} {where} ORDER BY updated_at DESC, id DESC LIMIT ?', (limit,))

    def get_outbound(self, reference: str) -> dict[str, Any] | None:
        ref = reference.strip()
        if not ref:
            return None
        if ref.isdigit():
            row = self._fetchone_dict(f'SELECT * FROM {self._table_name("outbound_events")} WHERE id = ?', (int(ref),))
            if row:
                return row
        return self._fetchone_dict(f'SELECT * FROM {self._table_name("outbound_events")} WHERE event_id = ?', (ref,))

    def requeue_outbound(self, reference: str) -> bool:
        now = int(time.time())
        with self._lock:
            numeric = int(reference) if reference.isdigit() else -1
            cur = self._execute(
                f'''
                UPDATE {self._table_name('outbound_events')}
                SET status = 'queued', next_attempt_at = ?, updated_at = ?, dead_reason = NULL, dead_letter_path = NULL
                WHERE id = ? OR event_id = ?
                ''',
                (now, now, numeric, reference),
            )
            self._commit()
            return (cur.rowcount or 0) > 0

    def purge_outbound(self, mode: str = 'sent') -> int:
        normalized = (mode or 'sent').strip().lower()
        if normalized == 'all':
            statuses = ('sent', 'dead')
        elif normalized == 'dead':
            statuses = ('dead',)
        else:
            statuses = ('sent',)
        placeholders = ', '.join('?' for _ in statuses)
        with self._lock:
            cur = self._execute(f'DELETE FROM {self._table_name("outbound_events")} WHERE status IN ({placeholders})', statuses)
            self._commit()
            return int(cur.rowcount or 0)

    def mark_outbound_success(self, row_id: int) -> None:
        now = int(time.time())
        with self._lock:
            self._execute(
                f"UPDATE {self._table_name('outbound_events')} SET status = 'sent', updated_at = ?, delivered_at = ?, last_error = NULL WHERE id = ?",
                (now, now, row_id),
            )
            self._commit()

    def mark_outbound_failure(self, row_id: int, *, attempts: int, delay_seconds: int, error: str, http_status: int | None) -> None:
        now = int(time.time())
        with self._lock:
            self._execute(
                f"UPDATE {self._table_name('outbound_events')} SET status = 'retry', attempts = ?, next_attempt_at = ?, updated_at = ?, last_error = ?, last_http_status = ? WHERE id = ?",
                (attempts, now + max(1, delay_seconds), now, error[:1000], http_status, row_id),
            )
            self._commit()

    def mark_outbound_dead(self, row_id: int, *, attempts: int, reason: str, http_status: int | None, dead_letter_path: str = '') -> None:
        now = int(time.time())
        with self._lock:
            self._execute(
                f"UPDATE {self._table_name('outbound_events')} SET status = 'dead', attempts = ?, updated_at = ?, last_error = ?, last_http_status = ?, dead_reason = ?, dead_letter_path = ? WHERE id = ?",
                (attempts, now, reason[:1000], http_status, reason[:1000], dead_letter_path, row_id),
            )
            self._commit()

    def pending_outbound_count(self) -> int:
        row = self._fetchone_dict(f"SELECT COUNT(1) AS count FROM {self._table_name('outbound_events')} WHERE status IN ('queued', 'retry')")
        return int(row['count']) if row else 0

    def dead_outbound_count(self) -> int:
        row = self._fetchone_dict(f"SELECT COUNT(1) AS count FROM {self._table_name('outbound_events')} WHERE status = 'dead'")
        return int(row['count']) if row else 0

    def processed_events_count(self) -> int:
        row = self._fetchone_dict(f'SELECT COUNT(1) AS count FROM {self._table_name("processed_events")}')
        return int(row['count']) if row else 0

    def has_processed_event(self, event_id: str) -> bool:
        row = self._fetchone_dict(f'SELECT 1 AS ok FROM {self._table_name("processed_events")} WHERE event_id = ?', (event_id,))
        return bool(row)

    def register_processed_event(self, *, event_id: str, source: str, event_type: str, expires_at: int | None) -> None:
        with self._lock:
            self._execute(
                f'INSERT INTO {self._table_name("processed_events")}(event_id, source, event_type, processed_at, expires_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(event_id) DO NOTHING',
                (event_id, source, event_type, int(time.time()), expires_at),
            )
            self._commit()

    def cleanup_processed_events(self) -> int:
        now = int(time.time())
        with self._lock:
            cur = self._execute(f'DELETE FROM {self._table_name("processed_events")} WHERE expires_at IS NOT NULL AND expires_at <= ?', (now,))
            self._commit()
            return int(cur.rowcount or 0)

    def cleanup_old_records(self, *, processed_events_retention_days: int, outbound_sent_retention_days: int, outbound_dead_retention_days: int, closed_ticket_retention_days: int) -> dict[str, int]:
        now = int(time.time())
        result = {'processed_events': 0, 'sent_outbound': 0, 'dead_outbound': 0, 'closed_tickets': 0, 'old_comments': 0}
        with self._lock:
            pe_cutoff = now - processed_events_retention_days * 86400
            cur = self._execute(
                f'DELETE FROM {self._table_name("processed_events")} WHERE (expires_at IS NOT NULL AND expires_at <= ?) OR processed_at <= ?',
                (now, pe_cutoff),
            )
            result['processed_events'] = int(cur.rowcount or 0)

            sent_cutoff = now - outbound_sent_retention_days * 86400
            cur = self._execute(f"DELETE FROM {self._table_name('outbound_events')} WHERE status = 'sent' AND updated_at <= ?", (sent_cutoff,))
            result['sent_outbound'] = int(cur.rowcount or 0)

            dead_cutoff = now - outbound_dead_retention_days * 86400
            cur = self._execute(f"DELETE FROM {self._table_name('outbound_events')} WHERE status = 'dead' AND updated_at <= ?", (dead_cutoff,))
            result['dead_outbound'] = int(cur.rowcount or 0)

            closed_cutoff = now - closed_ticket_retention_days * 86400
            tickets = self._fetchall_dicts(
                f"SELECT ticket_id FROM {self._table_name('support_tickets')} WHERE status IN ('resolved', 'closed') AND last_activity_at <= ?",
                (closed_cutoff,),
            )
            ticket_ids = [row['ticket_id'] for row in tickets]
            if ticket_ids:
                placeholders = ', '.join('?' for _ in ticket_ids)
                cur = self._execute(f'DELETE FROM {self._table_name("ticket_comments")} WHERE ticket_id IN ({placeholders})', ticket_ids)
                result['old_comments'] = int(cur.rowcount or 0)
                cur = self._execute(f'DELETE FROM {self._table_name("support_tickets")} WHERE ticket_id IN ({placeholders})', ticket_ids)
                result['closed_tickets'] = int(cur.rowcount or 0)
            self._commit()
        return result
