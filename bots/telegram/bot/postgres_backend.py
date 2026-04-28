from __future__ import annotations

import re
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

try:
    from psycopg import connect
    from psycopg.rows import dict_row
    from psycopg.errors import UniqueViolation
except Exception:  # pragma: no cover
    connect = None
    dict_row = None
    class UniqueViolation(Exception):
        pass

from nmbot.database import Database, LATEST_SCHEMA_VERSION, SCHEMA

_NOW_EXPR = "to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')"


def _translate_sql(sql: str) -> str:
    sql = sql.strip()
    if not sql:
        return ''
    if sql.upper().startswith('PRAGMA'):
        return ''
    if sql.upper().startswith('BEGIN IMMEDIATE'):
        return 'BEGIN'
    sql = re.sub(r"datetime\('now'\)", _NOW_EXPR, sql)
    sql = re.sub(r"datetime\('now',\s*'([^']+)'\)", lambda m: f"to_char((CURRENT_TIMESTAMP AT TIME ZONE 'UTC' + INTERVAL '{m.group(1)}'), 'YYYY-MM-DD HH24:MI:SS')", sql)
    sql = re.sub(r"datetime\('now',\s*\?\)", f"to_char((CURRENT_TIMESTAMP AT TIME ZONE 'UTC' + %s::interval), 'YYYY-MM-DD HH24:MI:SS')", sql)
    sql = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO\s+(\w+)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)", r"INSERT INTO \1 (\2) VALUES (\3) ON CONFLICT DO NOTHING", sql, flags=re.IGNORECASE|re.DOTALL)
    if re.match(r'^\s*(CREATE|ALTER)\s+', sql, flags=re.IGNORECASE):
        sql = re.sub(r'INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT', 'BIGSERIAL PRIMARY KEY', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bAUTOINCREMENT\b', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bINTEGER\b', 'BIGINT', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bREAL\b', 'DOUBLE PRECISION', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\s+', ' ', sql).strip()
    sql = sql.replace('?', '%s')
    return sql


def _sqlite_type_to_pg(name: str, decl: str, default: str | None, pk_inline: bool, notnull: bool) -> str:
    decl_u = (decl or 'TEXT').upper()
    if pk_inline and name == 'id' and 'INT' in decl_u:
        return 'BIGSERIAL PRIMARY KEY'
    if 'INT' in decl_u:
        base = 'BIGINT'
    elif 'REAL' in decl_u or 'FLOA' in decl_u or 'DOUB' in decl_u:
        base = 'DOUBLE PRECISION'
    else:
        base = 'TEXT'
    pieces = [base]
    if pk_inline:
        pieces.append('PRIMARY KEY')
    elif notnull:
        pieces.append('NOT NULL')
    if default is not None:
        d = str(default).strip()
        if 'datetime(' in d.lower():
            pieces.append(f'DEFAULT {_NOW_EXPR}')
        elif d.upper() != 'NULL':
            pieces.append(f'DEFAULT {d}')
    return ' '.join(pieces)


class _NullCursor:
    rowcount = 0
    lastrowid = None
    def fetchone(self): return None
    def fetchall(self): return []


class _PgCursor:
    def __init__(self, cur, first_row=None):
        self._cur = cur
        self._first_row = first_row
        self.lastrowid = int(first_row['id']) if isinstance(first_row, dict) and 'id' in first_row and first_row['id'] is not None else None
    @property
    def rowcount(self):
        return self._cur.rowcount
    def fetchone(self):
        if self._first_row is not None:
            row = self._first_row
            self._first_row = None
            return row
        return self._cur.fetchone()
    def fetchall(self):
        rows=[]
        if self._first_row is not None:
            rows.append(self._first_row)
            self._first_row=None
        more=self._cur.fetchall()
        if more: rows.extend(more)
        return rows


class _PgConn:
    RETURNING_TABLES = {'scheduled_broadcasts','operator_alerts','approval_requests','external_sync_events'}
    def __init__(self, conn):
        self._conn=conn
    def commit(self): self._conn.commit()
    def rollback(self): self._conn.rollback()
    def close(self): self._conn.close()
    def executescript(self, script: str):
        for part in script.split(';'):
            stmt=part.strip()
            if stmt:
                self.execute(stmt)
    def execute(self, sql: str, params: tuple|list=()):
        translated=_translate_sql(sql)
        if not translated:
            return _NullCursor()
        wants_returning=False
        m=re.match(r"INSERT\s+INTO\s+(\w+)", translated, flags=re.IGNORECASE)
        if m and 'RETURNING' not in translated.upper() and m.group(1).lower() in self.RETURNING_TABLES:
            translated += ' RETURNING id'
            wants_returning=True
        try:
            cur=self._conn.cursor()
            cur.execute(translated, params)
            first=cur.fetchone() if wants_returning else None
            return _PgCursor(cur, first)
        except UniqueViolation as exc:
            self._conn.rollback()
            raise sqlite3.IntegrityError(str(exc)) from exc


class PostgresDatabase(Database):
    backend_name = 'postgresql'
    def __init__(self, database_url: str) -> None:
        if connect is None:
            raise RuntimeError('psycopg не установлен; PostgreSQL backend недоступен')
        self.database_url = database_url
        self.path = Path(tempfile.gettempdir()) / 'nmtelegrambot-postgresql'
        self.path.mkdir(parents=True, exist_ok=True)
        self._initialize()
    @contextmanager
    def connect(self) -> Iterator[_PgConn]:
        conn = connect(self.database_url, row_factory=dict_row)
        try:
            yield _PgConn(conn)
        finally:
            conn.close()
    def _initialize(self) -> None:
        with self.connect() as connection:
            self._create_schema(connection)
            self._set_schema_version(connection, LATEST_SCHEMA_VERSION)
            connection.commit()

    def _create_schema(self, connection: _PgConn) -> None:
        for part in SCHEMA.split(';'):
            stmt = part.strip()
            if not stmt:
                continue
            translated = _translate_sql(stmt)
            if not translated:
                continue
            connection.execute(translated)
    def _ensure_column(self, connection, table: str, column: str, ddl: str) -> None:
        row = connection.execute("SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name=%s AND column_name=%s", (table, column)).fetchone()
        if row:
            return
        translated = _translate_sql(ddl).replace('INTEGER','BIGINT').replace('REAL','DOUBLE PRECISION')
        connection.execute(f'ALTER TABLE {table} ADD COLUMN {translated}')
    def _schema_version(self, connection) -> int:
        row = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
        return int(row['value']) if row and str(row['value']).isdigit() else 0
    def _set_schema_version(self, connection, version: int) -> None:
        connection.execute(f"INSERT INTO schema_meta (key, value, updated_at) VALUES ('schema_version', %s, {_NOW_EXPR}) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value, updated_at={_NOW_EXPR}", (str(version),))
    def db_health(self) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS c FROM broadcast_deliveries WHERE status IN ('pending','retry','failed')").fetchone()
            broadcast_backlog = int(row['c']) if row else 0
            now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            return {'journal_mode':'postgresql','schema_version':self._schema_version(connection),'dead_letters':len(self.list_dead_letters(limit=1000, status='pending')),'scheduled_backlog':len(self.due_scheduled_broadcasts(now)),'feed_backlog':len(self.due_feed_deliveries(now, limit=1000)),'broadcast_backlog':broadcast_backlog,'active_locks':len(self.list_active_locks())}
    def housekeeping(self) -> dict[str, Any]:
        with self.connect() as connection:
            connection.execute('ANALYZE')
            connection.commit()
        payload={'backend':'postgresql','at':datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}
        self.set_json_state('housekeeping:last', payload)
        return payload
