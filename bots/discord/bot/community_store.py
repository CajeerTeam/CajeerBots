from __future__ import annotations

import json
import re
import contextlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .handlers.storage import PostgresBackend, SQLiteBackend, StorageManager
from .event_contracts import EVENT_CONTRACT_VERSION

SCHEMA_VERSION = 14
COMMUNITY_SCHEMA_MIGRATIONS: list[tuple[int, str]] = [(5, "baseline"), (6, "scheduled_jobs_retry_lifecycle"), (7, "bridge_destination_state"), (8, "due_queue_indexes_observability"), (9, "approval_quorum_rules_reacceptance"), (10, "state_restore_symmetry_comment_lifecycle"), (11, "comment_mirror_rules_enforcement_restore_replay"), (12, "sqlite_schema_parity_and_recurring_digest_support"), (13, "transport_mirror_registry_calendar_and_legacy_lifecycle"), (14, "snapshot_mirror_restore_and_content_lifecycle")]


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _expiry(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(1, seconds))).strftime("%Y-%m-%d %H:%M:%S")


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _delivery_backoff(attempt: int, *, base_seconds: int = 15, max_seconds: int = 900) -> int:
    attempt = max(1, int(attempt or 1))
    return min(max_seconds, max(base_seconds, base_seconds * (2 ** (attempt - 1))))


def _fingerprint_json(value: Any) -> str:
    import hashlib
    return hashlib.sha256(_json(value).encode('utf-8')).hexdigest()


def _parse_json_value(value: Any, default: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    if value is None:
        return default
    return value


def _approval_expiry(expires_in_seconds: int | None) -> str | None:
    if expires_in_seconds is None:
        return None
    seconds = max(60, int(expires_in_seconds))
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime("%Y-%m-%d %H:%M:%S")


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    for candidate in (raw, raw.replace('Z', '+00:00')):
        with contextlib.suppress(ValueError):
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    with contextlib.suppress(ValueError):
        dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    return None


def _schema_statements(schema: str) -> list[str]:
    return [statement.strip() for statement in schema.split(';') if statement.strip()]


def _is_index_statement(statement: str) -> bool:
    return bool(re.match(r"^CREATE\s+(?:UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\b", statement, re.I))


def _schema_without_indexes(schema: str) -> str:
    statements = [statement for statement in _schema_statements(schema) if not _is_index_statement(statement)]
    return ';\n'.join(statements) + (';' if statements else '')


def _schema_indexes_only(schema: str) -> str:
    statements = [statement for statement in _schema_statements(schema) if _is_index_statement(statement)]
    return ';\n'.join(statements) + (';' if statements else '')


SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_external_sync_events_due_retry ON external_sync_events(status, next_retry_at, updated_at);
CREATE TABLE IF NOT EXISTS approval_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    requested_by TEXT NOT NULL,
    requested_by_name TEXT NOT NULL DEFAULT '',
    required_role TEXT NOT NULL DEFAULT 'owner',
    status TEXT NOT NULL DEFAULT 'pending',
    acted_by TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    acted_at TEXT,
    result_json TEXT NOT NULL DEFAULT '{}',
    expires_at TEXT,
    required_approvals INTEGER NOT NULL DEFAULT 1,
    approval_policy TEXT NOT NULL DEFAULT 'single_admin',
    approvals_json TEXT NOT NULL DEFAULT '[]',
    rejection_reason_code TEXT NOT NULL DEFAULT '',
    finalized_by_rule TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_approval_requests_status ON approval_requests(status, updated_at);
CREATE TABLE IF NOT EXISTS platform_account_links (
    platform TEXT NOT NULL,
    platform_user_id TEXT NOT NULL,
    platform_username TEXT NOT NULL DEFAULT '',
    guild_or_chat_id TEXT,
    minecraft_username TEXT NOT NULL,
    minecraft_uuid TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    linked_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (platform, platform_user_id)
);
CREATE INDEX IF NOT EXISTS idx_platform_account_links_uuid ON platform_account_links(minecraft_uuid);
CREATE TABLE IF NOT EXISTS community_identities (
    minecraft_uuid TEXT PRIMARY KEY,
    minecraft_username TEXT NOT NULL DEFAULT '',
    discord_user_id TEXT,
    discord_username TEXT,
    telegram_user_id TEXT,
    telegram_username TEXT,
    vk_user_id TEXT,
    workspace_actor_id TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_community_identities_discord_user_id ON community_identities(discord_user_id);
CREATE INDEX IF NOT EXISTS idx_community_identities_telegram_user_id ON community_identities(telegram_user_id);
CREATE TABLE IF NOT EXISTS platform_link_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    platform TEXT NOT NULL,
    event TEXT NOT NULL,
    platform_user_id TEXT,
    admin_user_id TEXT,
    player_name TEXT,
    player_uuid TEXT,
    details_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_platform_link_events_created_at ON platform_link_events(created_at);
CREATE TABLE IF NOT EXISTS panel_registry (
    guild_id TEXT NOT NULL,
    panel_type TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    version TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (guild_id, panel_type)
);
CREATE INDEX IF NOT EXISTS idx_panel_registry_channel_id ON panel_registry(channel_id);

CREATE TABLE IF NOT EXISTS forum_topic_registry (
    thread_id TEXT PRIMARY KEY,
    guild_id TEXT NOT NULL,
    forum_channel_id TEXT NOT NULL,
    topic_kind TEXT NOT NULL,
    owner_user_id TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    title TEXT NOT NULL DEFAULT '',
    tags_json TEXT NOT NULL DEFAULT '[]',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    auto_close_after_seconds INTEGER,
    closed_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_forum_topic_registry_status_updated_at ON forum_topic_registry(status, updated_at);
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    guild_id TEXT,
    channel_id TEXT,
    payload_json TEXT NOT NULL DEFAULT '{}',
    run_at TEXT NOT NULL,
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    sent_at TEXT,
    last_error TEXT NOT NULL DEFAULT '',
    dedupe_key TEXT NOT NULL DEFAULT '',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    first_attempt_at TEXT,
    last_attempt_at TEXT,
    dead_letter_reason_code TEXT NOT NULL DEFAULT '',
    backoff_seconds INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_status_run_at ON scheduled_jobs(status, run_at);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_due_retry ON scheduled_jobs(status, next_retry_at, run_at);
CREATE TABLE IF NOT EXISTS subscription_preferences (
    platform TEXT NOT NULL,
    platform_user_id TEXT NOT NULL,
    minecraft_uuid TEXT,
    preferences_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (platform, platform_user_id)
);
CREATE INDEX IF NOT EXISTS idx_subscription_preferences_uuid ON subscription_preferences(minecraft_uuid);
CREATE TABLE IF NOT EXISTS rules_acceptance (
    guild_id TEXT NOT NULL,
    discord_user_id TEXT NOT NULL,
    accepted_rules_version TEXT NOT NULL,
    panel_version TEXT NOT NULL DEFAULT '',
    accepted_at TEXT NOT NULL DEFAULT (datetime('now')),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (guild_id, discord_user_id)
);
CREATE TABLE IF NOT EXISTS panel_drift_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_id TEXT NOT NULL,
    panel_type TEXT NOT NULL,
    old_version TEXT NOT NULL DEFAULT '',
    new_version TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    details_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS layout_alias_bindings (
    guild_id TEXT NOT NULL,
    alias TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    discord_id TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (guild_id, alias, resource_type)
);
CREATE INDEX IF NOT EXISTS idx_layout_alias_bindings_discord_id ON layout_alias_bindings(discord_id);
CREATE TABLE IF NOT EXISTS schema_meta_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    changed_at TEXT NOT NULL DEFAULT (datetime('now')),
    source TEXT NOT NULL DEFAULT 'ensure_schema'
);
CREATE TABLE IF NOT EXISTS community_schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (datetime('now')),
    source TEXT NOT NULL DEFAULT 'ensure_schema'
);
CREATE TABLE IF NOT EXISTS bridge_destination_state (
    destination TEXT PRIMARY KEY,
    circuit_state TEXT NOT NULL DEFAULT 'closed',
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    circuit_open_until TEXT,
    last_success_at TEXT,
    last_failure_at TEXT,
    last_error TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS bridge_comment_mirror (
    thread_id TEXT NOT NULL,
    source_platform TEXT NOT NULL DEFAULT 'external',
    external_comment_id TEXT NOT NULL,
    discord_message_id TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (thread_id, source_platform, external_comment_id)
);
CREATE INDEX IF NOT EXISTS idx_bridge_comment_mirror_message_id ON bridge_comment_mirror(discord_message_id);
CREATE TABLE IF NOT EXISTS external_discussion_mirror (
    source_platform TEXT NOT NULL DEFAULT 'external',
    external_topic_id TEXT NOT NULL,
    topic_kind TEXT NOT NULL DEFAULT '',
    discord_object_id TEXT NOT NULL,
    channel_id TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source_platform, external_topic_id)
);
CREATE INDEX IF NOT EXISTS idx_external_discussion_mirror_discord_object_id ON external_discussion_mirror(discord_object_id);
CREATE TABLE IF NOT EXISTS external_content_mirror (
    source_platform TEXT NOT NULL DEFAULT 'external',
    content_kind TEXT NOT NULL,
    external_content_id TEXT NOT NULL,
    discord_channel_id TEXT NOT NULL DEFAULT '',
    discord_message_id TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source_platform, content_kind, external_content_id)
);
CREATE INDEX IF NOT EXISTS idx_external_content_mirror_message_id ON external_content_mirror(discord_message_id);
CREATE TABLE IF NOT EXISTS legacy_layout_resources (
    guild_id TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    discord_id TEXT NOT NULL,
    resource_name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'legacy',
    marked_at TEXT NOT NULL DEFAULT (datetime('now')),
    review_after TEXT,
    delete_after TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (guild_id, resource_type, discord_id)
);
CREATE INDEX IF NOT EXISTS idx_legacy_layout_resources_status ON legacy_layout_resources(status, delete_after, review_after);

"""

POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS runtime_locks (
    name TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_idempotency_expires_at ON idempotency_keys(expires_at);
CREATE TABLE IF NOT EXISTS external_sync_events (
    id BIGSERIAL PRIMARY KEY,
    event_kind TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    destination TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_external_sync_events_status ON external_sync_events(status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_external_sync_events_due_retry ON external_sync_events(status, next_retry_at, updated_at);
CREATE TABLE IF NOT EXISTS approval_requests (
    id BIGSERIAL PRIMARY KEY,
    kind TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    requested_by TEXT NOT NULL,
    requested_by_name TEXT NOT NULL DEFAULT '',
    required_role TEXT NOT NULL DEFAULT 'owner',
    status TEXT NOT NULL DEFAULT 'pending',
    acted_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acted_at TIMESTAMPTZ,
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    expires_at TIMESTAMPTZ,
    required_approvals INTEGER NOT NULL DEFAULT 1,
    approval_policy TEXT NOT NULL DEFAULT 'single_admin',
    approvals_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    rejection_reason_code TEXT NOT NULL DEFAULT '',
    finalized_by_rule TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_approval_requests_status ON approval_requests(status, updated_at DESC);
CREATE TABLE IF NOT EXISTS platform_account_links (
    platform TEXT NOT NULL,
    platform_user_id TEXT NOT NULL,
    platform_username TEXT NOT NULL DEFAULT '',
    guild_or_chat_id TEXT,
    minecraft_username TEXT NOT NULL,
    minecraft_uuid TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (platform, platform_user_id)
);
CREATE INDEX IF NOT EXISTS idx_platform_account_links_uuid ON platform_account_links(minecraft_uuid);
CREATE TABLE IF NOT EXISTS community_identities (
    minecraft_uuid TEXT PRIMARY KEY,
    minecraft_username TEXT NOT NULL DEFAULT '',
    discord_user_id TEXT,
    discord_username TEXT,
    telegram_user_id TEXT,
    telegram_username TEXT,
    vk_user_id TEXT,
    workspace_actor_id TEXT,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_community_identities_discord_user_id ON community_identities(discord_user_id);
CREATE INDEX IF NOT EXISTS idx_community_identities_telegram_user_id ON community_identities(telegram_user_id);
CREATE TABLE IF NOT EXISTS platform_link_events (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    platform TEXT NOT NULL,
    event TEXT NOT NULL,
    platform_user_id TEXT,
    admin_user_id TEXT,
    player_name TEXT,
    player_uuid TEXT,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_platform_link_events_created_at ON platform_link_events(created_at DESC);
CREATE TABLE IF NOT EXISTS panel_registry (
    guild_id TEXT NOT NULL,
    panel_type TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    version TEXT NOT NULL DEFAULT '',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (guild_id, panel_type)
);
CREATE INDEX IF NOT EXISTS idx_panel_registry_channel_id ON panel_registry(channel_id);

CREATE TABLE IF NOT EXISTS forum_topic_registry (
    thread_id TEXT PRIMARY KEY,
    guild_id TEXT NOT NULL,
    forum_channel_id TEXT NOT NULL,
    topic_kind TEXT NOT NULL,
    owner_user_id TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    title TEXT NOT NULL DEFAULT '',
    tags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    auto_close_after_seconds INTEGER,
    closed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_forum_topic_registry_status_updated_at ON forum_topic_registry(status, updated_at DESC);
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    guild_id TEXT,
    channel_id TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    run_at TIMESTAMPTZ NOT NULL,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    dedupe_key TEXT NOT NULL DEFAULT '',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,
    first_attempt_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    dead_letter_reason_code TEXT NOT NULL DEFAULT '',
    backoff_seconds INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_status_run_at ON scheduled_jobs(status, run_at);
CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_due_retry ON scheduled_jobs(status, next_retry_at, run_at);
CREATE TABLE IF NOT EXISTS subscription_preferences (
    platform TEXT NOT NULL,
    platform_user_id TEXT NOT NULL,
    minecraft_uuid TEXT,
    preferences_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (platform, platform_user_id)
);
CREATE INDEX IF NOT EXISTS idx_subscription_preferences_uuid ON subscription_preferences(minecraft_uuid);
CREATE TABLE IF NOT EXISTS rules_acceptance (
    guild_id TEXT NOT NULL,
    discord_user_id TEXT NOT NULL,
    accepted_rules_version TEXT NOT NULL,
    panel_version TEXT NOT NULL DEFAULT '',
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (guild_id, discord_user_id)
);
CREATE TABLE IF NOT EXISTS panel_drift_log (
    id BIGSERIAL PRIMARY KEY,
    guild_id TEXT NOT NULL,
    panel_type TEXT NOT NULL,
    old_version TEXT NOT NULL DEFAULT '',
    new_version TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS layout_alias_bindings (
    guild_id TEXT NOT NULL,
    alias TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    discord_id TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (guild_id, alias, resource_type)
);
CREATE INDEX IF NOT EXISTS idx_layout_alias_bindings_discord_id ON layout_alias_bindings(discord_id);
CREATE TABLE IF NOT EXISTS schema_meta_ledger (
    id BIGSERIAL PRIMARY KEY,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL DEFAULT 'ensure_schema'
);
CREATE TABLE IF NOT EXISTS community_schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source TEXT NOT NULL DEFAULT 'ensure_schema'
);
CREATE TABLE IF NOT EXISTS bridge_destination_state (
    destination TEXT PRIMARY KEY,
    circuit_state TEXT NOT NULL DEFAULT 'closed',
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    circuit_open_until TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_failure_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS bridge_comment_mirror (
    thread_id TEXT NOT NULL,
    source_platform TEXT NOT NULL DEFAULT 'external',
    external_comment_id TEXT NOT NULL,
    discord_message_id TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (thread_id, source_platform, external_comment_id)
);
CREATE INDEX IF NOT EXISTS idx_bridge_comment_mirror_message_id ON bridge_comment_mirror(discord_message_id);
CREATE TABLE IF NOT EXISTS external_discussion_mirror (
    source_platform TEXT NOT NULL DEFAULT 'external',
    external_topic_id TEXT NOT NULL,
    topic_kind TEXT NOT NULL DEFAULT '',
    discord_object_id TEXT NOT NULL,
    channel_id TEXT NOT NULL DEFAULT '',
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_platform, external_topic_id)
);
CREATE INDEX IF NOT EXISTS idx_external_discussion_mirror_discord_object_id ON external_discussion_mirror(discord_object_id);
CREATE TABLE IF NOT EXISTS external_content_mirror (
    source_platform TEXT NOT NULL DEFAULT 'external',
    content_kind TEXT NOT NULL,
    external_content_id TEXT NOT NULL,
    discord_channel_id TEXT NOT NULL DEFAULT '',
    discord_message_id TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_platform, content_kind, external_content_id)
);
CREATE INDEX IF NOT EXISTS idx_external_content_mirror_message_id ON external_content_mirror(discord_message_id);
CREATE TABLE IF NOT EXISTS legacy_layout_resources (
    guild_id TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    discord_id TEXT NOT NULL,
    resource_name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'legacy',
    marked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    review_after TIMESTAMPTZ,
    delete_after TIMESTAMPTZ,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (guild_id, resource_type, discord_id)
);
CREATE INDEX IF NOT EXISTS idx_legacy_layout_resources_status ON legacy_layout_resources(status, delete_after, review_after);

"""


@dataclass(slots=True)
class CommunityStore:
    storage: StorageManager
    code_version: str

    async def _sqlite_add_column(self, table: str, column: str, definition: str) -> None:
        conn = self.storage.database.conn
        assert conn is not None
        row = await (await conn.execute(f"PRAGMA table_info({table})")).fetchall()
        existing = {str(item['name']) for item in row}
        if column not in existing:
            await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    async def _postgres_add_column(self, table: str, column: str, definition: str) -> None:
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {definition}")

    async def _postgres_column_type(self, table: str, column: str) -> str | None:
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            value = await conn.fetchval(
                """
                SELECT data_type
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = $1
                  AND column_name = $2
                LIMIT 1
                """,
                table,
                column,
            )
        return str(value).lower() if value is not None else None

    async def _postgres_coerce_timestamptz_column(self, table: str, column: str, *, fallback_sql: str = 'NOW()') -> None:
        column_type = await self._postgres_column_type(table, column)
        if column_type in (None, 'timestamp with time zone'):
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                f"""
                ALTER TABLE {table}
                ALTER COLUMN {column} TYPE TIMESTAMPTZ
                USING COALESCE(
                    CASE
                        WHEN {column} IS NULL THEN NULL
                        WHEN btrim({column}::text) = '' THEN NULL
                        ELSE {column}::timestamptz
                    END,
                    {fallback_sql}
                )
                """
            )

    async def _postgres_normalize_timestamp_columns(self) -> None:
        for table, column, fallback_sql in [
            ('runtime_locks', 'acquired_at', 'NOW()'),
            ('runtime_locks', 'expires_at', "NOW() - INTERVAL '1 second'"),
            ('idempotency_keys', 'created_at', 'NOW()'),
            ('idempotency_keys', 'expires_at', "NOW() - INTERVAL '1 second'"),
            ('external_sync_events', 'created_at', 'NOW()'),
            ('external_sync_events', 'updated_at', 'NOW()'),
            ('external_sync_events', 'delivered_at', 'NULL'),
            ('external_sync_events', 'next_retry_at', 'NULL'),
            ('external_sync_events', 'first_attempt_at', 'NULL'),
            ('external_sync_events', 'last_attempt_at', 'NULL'),
            ('approval_requests', 'created_at', 'NOW()'),
            ('approval_requests', 'updated_at', 'NOW()'),
            ('approval_requests', 'acted_at', 'NULL'),
            ('approval_requests', 'expires_at', 'NULL'),
            ('platform_account_links', 'linked_at', 'NOW()'),
            ('platform_account_links', 'updated_at', 'NOW()'),
            ('community_identities', 'updated_at', 'NOW()'),
            ('platform_link_events', 'created_at', 'NOW()'),
            ('panel_registry', 'updated_at', 'NOW()'),
            ('forum_topic_registry', 'created_at', 'NOW()'),
            ('forum_topic_registry', 'updated_at', 'NOW()'),
            ('forum_topic_registry', 'closed_at', 'NULL'),
            ('scheduled_jobs', 'run_at', 'NOW()'),
            ('scheduled_jobs', 'created_at', 'NOW()'),
            ('scheduled_jobs', 'updated_at', 'NOW()'),
            ('scheduled_jobs', 'sent_at', 'NULL'),
            ('scheduled_jobs', 'next_retry_at', 'NULL'),
            ('scheduled_jobs', 'first_attempt_at', 'NULL'),
            ('scheduled_jobs', 'last_attempt_at', 'NULL'),
            ('rules_acceptance', 'accepted_at', 'NOW()'),
            ('panel_drift_log', 'created_at', 'NOW()'),
            ('layout_alias_bindings', 'created_at', 'NOW()'),
            ('layout_alias_bindings', 'updated_at', 'NOW()'),
            ('bridge_destination_state', 'updated_at', 'NOW()'),
            ('bridge_destination_state', 'circuit_open_until', 'NULL'),
            ('bridge_comment_mirror', 'created_at', 'NOW()'),
            ('bridge_comment_mirror', 'updated_at', 'NOW()'),
            ('external_discussion_mirror', 'created_at', 'NOW()'),
            ('external_discussion_mirror', 'updated_at', 'NOW()'),
            ('external_content_mirror', 'created_at', 'NOW()'),
            ('external_content_mirror', 'updated_at', 'NOW()'),
            ('legacy_layout_resources', 'marked_at', 'NOW()'),
            ('legacy_layout_resources', 'review_after', 'NULL'),
            ('legacy_layout_resources', 'delete_after', 'NULL'),
            ('schema_meta_ledger', 'created_at', 'NOW()'),
            ('community_schema_migrations', 'applied_at', 'NOW()'),
        ]:
            await self._postgres_coerce_timestamptz_column(table, column, fallback_sql=fallback_sql)


    async def _get_recorded_schema_version(self) -> int:
        try:
            if isinstance(self.storage.database, SQLiteBackend):
                conn = self.storage.database.conn
                assert conn is not None
                row = await (await conn.execute("SELECT value FROM schema_meta WHERE key='nmdiscordbot_schema_version' LIMIT 1")).fetchone()
                return int(row[0]) if row and str(row[0]).strip() else 0
            pool = self.storage.database.pool
            assert pool is not None
            async with pool.acquire() as conn:
                value = await conn.fetchval("SELECT value FROM schema_meta WHERE key='nmdiscordbot_schema_version' LIMIT 1")
            return int(value) if value is not None and str(value).strip() else 0
        except Exception:
            return 0

    async def _record_schema_meta(self, *, key: str, value: str, source: str = 'ensure_schema') -> None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute("INSERT INTO schema_meta(key, value, updated_at) VALUES(?, ?, datetime('now')) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')", (key, value))
            await conn.execute("INSERT INTO schema_meta_ledger(key, value, source) VALUES(?, ?, ?)", (key, value, source))
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO schema_meta(key, value, updated_at) VALUES($1, $2, NOW()) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()", key, value)
            await conn.execute("INSERT INTO schema_meta_ledger(key, value, source) VALUES($1, $2, $3)", key, value, source)

    async def upsert_schema_meta(self, *, key: str, value: str, source: str = 'state_restore') -> None:
        await self._record_schema_meta(key=key, value=value, source=source)

    async def record_schema_meta_ledger_entry(self, *, key: str, value: str, source: str = 'state_restore') -> None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute("INSERT INTO schema_meta_ledger(key, value, source) VALUES(?, ?, ?)", (key, value, source))
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO schema_meta_ledger(key, value, source) VALUES($1, $2, $3)", key, value, source)

    async def _migration_versions_applied(self) -> set[int]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT version FROM community_schema_migrations ORDER BY version")).fetchall()
            return {int(row[0]) for row in rows if row and str(row[0]).isdigit()}
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT version FROM community_schema_migrations ORDER BY version")
        return {int(row['version']) for row in rows}

    async def _record_schema_migration(self, *, version: int, name: str, source: str = 'ensure_schema') -> None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO community_schema_migrations(version, name, applied_at, source) VALUES(?, ?, datetime('now'), ?) ON CONFLICT(version) DO UPDATE SET name=excluded.name, source=excluded.source",
                (int(version), name, source),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO community_schema_migrations(version, name, applied_at, source) VALUES($1, $2, NOW(), $3) ON CONFLICT(version) DO UPDATE SET name=EXCLUDED.name, source=EXCLUDED.source",
                int(version), name, source,
            )

    async def _apply_versioned_migrations(self) -> None:
        applied = await self._migration_versions_applied()
        for version, source in COMMUNITY_SCHEMA_MIGRATIONS:
            if version in applied:
                continue
            await self._record_schema_migration(version=version, name=source, source='ensure_schema')
            await self._record_schema_meta(key='nmdiscordbot_schema_version', value=str(version), source=f'migration:{source}')

    async def ensure_schema(self) -> None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.executescript(_schema_without_indexes(SQLITE_SCHEMA))
            for column, definition in [
                ('attempt_count', "INTEGER NOT NULL DEFAULT 0"),
                ('next_retry_at', "TEXT"),
                ('first_attempt_at', "TEXT"),
                ('last_attempt_at', "TEXT"),
                ('dead_letter_reason_code', "TEXT NOT NULL DEFAULT ''"),
                ('dedupe_key', "TEXT NOT NULL DEFAULT ''"),
                ('backoff_seconds', "INTEGER NOT NULL DEFAULT 0"),
            ]:
                await self._sqlite_add_column('external_sync_events', column, definition)
            for column, definition in [
                ('dedupe_key', "TEXT NOT NULL DEFAULT ''"),
                ('attempt_count', "INTEGER NOT NULL DEFAULT 0"),
                ('next_retry_at', "TEXT"),
                ('first_attempt_at', "TEXT"),
                ('last_attempt_at', "TEXT"),
                ('dead_letter_reason_code', "TEXT NOT NULL DEFAULT ''"),
                ('backoff_seconds', "INTEGER NOT NULL DEFAULT 0"),
            ]:
                await self._sqlite_add_column('scheduled_jobs', column, definition)
        if isinstance(self.storage.database, PostgresBackend):
            pool = self.storage.database.pool
            assert pool is not None
            async with pool.acquire() as conn:
                await conn.execute(_schema_without_indexes(POSTGRES_SCHEMA))
            for column, definition in [
                ('attempt_count', "INTEGER NOT NULL DEFAULT 0"),
                ('next_retry_at', "TIMESTAMPTZ"),
                ('first_attempt_at', "TIMESTAMPTZ"),
                ('last_attempt_at', "TIMESTAMPTZ"),
                ('dead_letter_reason_code', "TEXT NOT NULL DEFAULT ''"),
                ('dedupe_key', "TEXT NOT NULL DEFAULT ''"),
                ('backoff_seconds', "INTEGER NOT NULL DEFAULT 0"),
            ]:
                await self._postgres_add_column('external_sync_events', column, definition)
            for column, definition in [
                ('dedupe_key', "TEXT NOT NULL DEFAULT ''"),
                ('attempt_count', "INTEGER NOT NULL DEFAULT 0"),
                ('next_retry_at', "TIMESTAMPTZ"),
                ('first_attempt_at', "TIMESTAMPTZ"),
                ('last_attempt_at', "TIMESTAMPTZ"),
                ('dead_letter_reason_code', "TEXT NOT NULL DEFAULT ''"),
                ('backoff_seconds', "INTEGER NOT NULL DEFAULT 0"),
            ]:
                await self._postgres_add_column('scheduled_jobs', column, definition)
            for column, definition in [
                ('expires_at', "TIMESTAMPTZ"),
                ('required_approvals', "INTEGER NOT NULL DEFAULT 1"),
                ('approval_policy', "TEXT NOT NULL DEFAULT 'single_admin'"),
                ('approvals_json', "JSONB NOT NULL DEFAULT '[]'::jsonb"),
                ('rejection_reason_code', "TEXT NOT NULL DEFAULT ''"),
                ('finalized_by_rule', "TEXT NOT NULL DEFAULT ''"),
            ]:
                await self._postgres_add_column('approval_requests', column, definition)
            await self._postgres_normalize_timestamp_columns()
            async with pool.acquire() as conn:
                await conn.execute(_schema_indexes_only(POSTGRES_SCHEMA))
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.executescript(_schema_indexes_only(SQLITE_SCHEMA))
        await self._apply_versioned_migrations()
        await self._record_schema_meta(key='nmdiscordbot_code_version', value=self.code_version, source='ensure_schema')
        await self._record_schema_meta(key='nmdiscordbot_event_contract_version', value=str(EVENT_CONTRACT_VERSION), source='ensure_schema')
        await self._record_schema_meta(key='nmdiscordbot_schema_current', value=str(SCHEMA_VERSION), source='ensure_schema')

    async def health(self) -> dict[str, Any]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT value FROM schema_meta WHERE key='nmdiscordbot_schema_version'" )).fetchone()
            pending = await (await conn.execute("SELECT COUNT(*) FROM external_sync_events WHERE status!='sent'" )).fetchone()
            approvals = await (await conn.execute("SELECT COUNT(*) FROM approval_requests WHERE status='pending'" )).fetchone()
            return {"backend": "sqlite", "schema_version": row[0] if row else None, "pending_external_sync_events": int(pending[0]) if pending else 0, "pending_approval_requests": int(approvals[0]) if approvals else 0}
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT value FROM schema_meta WHERE key='nmdiscordbot_schema_version'")
            pending = await conn.fetchval("SELECT COUNT(*) FROM external_sync_events WHERE status!='sent'")
            approvals = await conn.fetchval("SELECT COUNT(*) FROM approval_requests WHERE status='pending'")
        return {"backend": "postgresql", "schema_version": row['value'] if row else None, "pending_external_sync_events": int(pending or 0), "pending_approval_requests": int(approvals or 0)}

    async def claim_idempotency_key(self, key: str, *, ttl_seconds: int = 300) -> bool:
        expires_at = _expiry(ttl_seconds)
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute("DELETE FROM idempotency_keys WHERE expires_at <= datetime('now')")
            try:
                await conn.execute("INSERT INTO idempotency_keys(key, expires_at) VALUES(?, ?)", (key, expires_at))
                await conn.commit()
                return True
            except Exception:
                return False
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM idempotency_keys WHERE expires_at <= NOW()")
            try:
                await conn.execute("INSERT INTO idempotency_keys(key, expires_at) VALUES($1, $2::timestamptz)", key, expires_at)
                return True
            except Exception:
                return False

    async def acquire_runtime_lock(self, name: str, owner: str, *, ttl_seconds: int = 30) -> bool:
        expires_at = _expiry(ttl_seconds)
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute("DELETE FROM runtime_locks WHERE expires_at <= datetime('now')")
            row = await (await conn.execute("SELECT owner FROM runtime_locks WHERE name=?", (name,))).fetchone()
            if row is not None:
                return False
            await conn.execute("INSERT INTO runtime_locks(name, owner, acquired_at, expires_at) VALUES(?, ?, ?, ?)", (name, owner, _utc_now(), expires_at))
            await conn.commit()
            return True
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM runtime_locks WHERE expires_at <= NOW()")
            try:
                await conn.execute("INSERT INTO runtime_locks(name, owner, acquired_at, expires_at) VALUES($1, $2, NOW(), $3::timestamptz)", name, owner, expires_at)
                return True
            except Exception:
                return False

    async def release_runtime_lock(self, name: str, owner: str) -> None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute("DELETE FROM runtime_locks WHERE name=? AND owner=?", (name, owner))
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM runtime_locks WHERE name=$1 AND owner=$2", name, owner)

    async def queue_external_sync_event(self, *, event_kind: str, destination: str, payload: dict[str, Any], dedupe_key: str | None = None) -> int:
        dedupe_key = str(dedupe_key or payload.get('event_id') or _fingerprint_json({'event_kind': event_kind, 'destination': destination, 'payload': payload}))[:190]
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            cur = await conn.execute(
                "INSERT INTO external_sync_events(event_kind, destination, payload_json, status, updated_at, dedupe_key, attempt_count, backoff_seconds) VALUES(?, ?, ?, 'pending', datetime('now'), ?, 0, 0)",
                (event_kind, destination, _json(payload), dedupe_key),
            )
            await conn.commit()
            return int(cur.lastrowid or 0)
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO external_sync_events(event_kind, destination, payload_json, status, updated_at, dedupe_key, attempt_count, backoff_seconds) VALUES($1, $2, $3::jsonb, 'pending', NOW(), $4, 0, 0) RETURNING id",
                event_kind,
                destination,
                _json(payload),
                dedupe_key,
            )
        return int(row['id']) if row else 0

    async def list_external_sync_events(self, *, status: str | None = 'pending', limit: int = 50) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            if status is None:
                rows = await (await conn.execute("SELECT * FROM external_sync_events ORDER BY id ASC LIMIT ?", (limit,))).fetchall()
            else:
                rows = await (await conn.execute("SELECT * FROM external_sync_events WHERE status=? ORDER BY id ASC LIMIT ?", (status, limit))).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                try:
                    data['payload_json']=json.loads(data.get('payload_json') or '{}')
                except Exception:
                    data['payload_json']={}
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            if status is None:
                rows = await conn.fetch("SELECT * FROM external_sync_events ORDER BY id ASC LIMIT $1", limit)
            else:
                rows = await conn.fetch("SELECT * FROM external_sync_events WHERE status=$1 ORDER BY id ASC LIMIT $2", status, limit)
        return [dict(row) for row in rows]

    async def list_deliverable_external_sync_events(self, *, limit: int = 50) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT * FROM external_sync_events WHERE status IN ('pending','retry') AND (next_retry_at IS NULL OR next_retry_at <= datetime('now')) ORDER BY id ASC LIMIT ?", (limit,))).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                try:
                    data['payload_json']=json.loads(data.get('payload_json') or '{}')
                except Exception:
                    data['payload_json']={}
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM external_sync_events WHERE status = ANY($1::text[]) AND (next_retry_at IS NULL OR next_retry_at <= NOW()) ORDER BY id ASC LIMIT $2", ['pending','retry'], limit)
        return [dict(row) for row in rows]

    async def mark_external_sync_event(self, event_id: int, *, status: str, error: str = '', backoff_seconds: int = 0, dead_letter_reason_code: str = '') -> None:
        error = error[:2000]
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            if status == 'sent':
                await conn.execute("UPDATE external_sync_events SET status=?, delivered_at=datetime('now'), updated_at=datetime('now'), last_error=?, attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, datetime('now')), last_attempt_at=datetime('now'), next_retry_at=NULL, backoff_seconds=0, dead_letter_reason_code='' WHERE id=?", (status, error, event_id))
            elif status == 'dead_letter':
                await conn.execute("UPDATE external_sync_events SET status=?, updated_at=datetime('now'), last_error=?, attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, datetime('now')), last_attempt_at=datetime('now'), next_retry_at=NULL, backoff_seconds=?, dead_letter_reason_code=? WHERE id=?", (status, error, int(backoff_seconds or 0), dead_letter_reason_code[:120], event_id))
            else:
                await conn.execute("UPDATE external_sync_events SET status=?, updated_at=datetime('now'), last_error=?, attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, datetime('now')), last_attempt_at=datetime('now'), next_retry_at=datetime('now', '+' || ? || ' seconds'), backoff_seconds=?, dead_letter_reason_code='' WHERE id=?", (status, error, int(backoff_seconds or 0), int(backoff_seconds or 0), event_id))
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            if status == 'sent':
                await conn.execute("UPDATE external_sync_events SET status=$1, delivered_at=NOW(), updated_at=NOW(), last_error=$2, attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, NOW()), last_attempt_at=NOW(), next_retry_at=NULL, backoff_seconds=0, dead_letter_reason_code='' WHERE id=$3", status, error, event_id)
            elif status == 'dead_letter':
                await conn.execute("UPDATE external_sync_events SET status=$1, updated_at=NOW(), last_error=$2, attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, NOW()), last_attempt_at=NOW(), next_retry_at=NULL, backoff_seconds=$3, dead_letter_reason_code=$4 WHERE id=$5", status, error, int(backoff_seconds or 0), dead_letter_reason_code[:120], event_id)
            else:
                await conn.execute("UPDATE external_sync_events SET status=$1, updated_at=NOW(), last_error=$2, attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, NOW()), last_attempt_at=NOW(), next_retry_at=NOW() + ($3::int * INTERVAL '1 second'), backoff_seconds=$3, dead_letter_reason_code='' WHERE id=$4", status, error, int(backoff_seconds or 0), event_id)

    async def create_approval_request(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        requested_by: str,
        requested_by_name: str,
        required_role: str = 'owner',
        expires_in_seconds: int | None = None,
        required_approvals: int = 1,
        approval_policy: str = 'single_admin',
    ) -> int:
        expires_at = _approval_expiry(expires_in_seconds)
        required_approvals = max(1, int(required_approvals or 1))
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            cur = await conn.execute(
                "INSERT INTO approval_requests(kind, payload_json, requested_by, requested_by_name, required_role, updated_at, expires_at, required_approvals, approval_policy, approvals_json) VALUES(?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, '[]')",
                (kind, _json(payload), requested_by, requested_by_name, required_role, expires_at, required_approvals, approval_policy),
            )
            await conn.commit()
            return int(cur.lastrowid or 0)
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO approval_requests(kind, payload_json, requested_by, requested_by_name, required_role, updated_at, expires_at, required_approvals, approval_policy, approvals_json) VALUES($1, $2::jsonb, $3, $4, $5, NOW(), $6::timestamptz, $7, $8, '[]'::jsonb) RETURNING id",
                kind,
                _json(payload),
                requested_by,
                requested_by_name,
                required_role,
                expires_at,
                required_approvals,
                approval_policy,
            )
        return int(row['id']) if row else 0

    async def get_approval_request(self, request_id: int) -> dict[str, Any] | None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM approval_requests WHERE id=? LIMIT 1", (request_id,))).fetchone()
            if row is None:
                return None
            data = dict(row)
            data['payload_json'] = _parse_json_value(data.get('payload_json'), {})
            data['result_json'] = _parse_json_value(data.get('result_json'), {})
            data['approvals_json'] = _parse_json_value(data.get('approvals_json'), [])
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM approval_requests WHERE id=$1 LIMIT 1", request_id)
        if row is None:
            return None
        data = dict(row)
        data['approvals_json'] = _parse_json_value(data.get('approvals_json'), [])
        return data

    async def list_approval_requests(self, *, status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            if status is None:
                rows = await (await conn.execute("SELECT * FROM approval_requests ORDER BY id DESC LIMIT ?", (limit,))).fetchall()
            else:
                rows = await (await conn.execute("SELECT * FROM approval_requests WHERE status=? ORDER BY id DESC LIMIT ?", (status, limit))).fetchall()
            result = []
            for row in rows:
                data = dict(row)
                data['payload_json'] = _parse_json_value(data.get('payload_json'), {})
                data['result_json'] = _parse_json_value(data.get('result_json'), {})
                data['approvals_json'] = _parse_json_value(data.get('approvals_json'), [])
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            if status is None:
                rows = await conn.fetch("SELECT * FROM approval_requests ORDER BY id DESC LIMIT $1", limit)
            else:
                rows = await conn.fetch("SELECT * FROM approval_requests WHERE status=$1 ORDER BY id DESC LIMIT $2", status, limit)
        result = []
        for row in rows:
            data = dict(row)
            data['approvals_json'] = _parse_json_value(data.get('approvals_json'), [])
            result.append(data)
        return result

    async def decide_approval_request(
        self,
        request_id: int,
        *,
        decision: str,
        acted_by: str,
        note: str = '',
        rejection_reason_code: str = '',
    ) -> dict[str, Any] | None:
        row = await self.get_approval_request(request_id)
        if row is None:
            return None
        if str(row.get('status') or '') != 'pending':
            return {'ok': False, 'reason': 'not_pending', 'row': row}
        expires_at = _parse_datetime(str(row.get('expires_at') or ''))
        if expires_at is not None and expires_at <= datetime.now(timezone.utc):
            await self._finalize_approval_request(request_id, status='expired', acted_by=acted_by, result_json={'note': note or '', 'expired': True}, rejection_reason_code='expired', finalized_by_rule='expiry')
            row = await self.get_approval_request(request_id)
            return {'ok': False, 'reason': 'expired', 'row': row}
        approvals = list(_parse_json_value(row.get('approvals_json'), []))
        approvals = [item for item in approvals if isinstance(item, dict)]
        approvals = [item for item in approvals if str(item.get('acted_by') or '') != acted_by]
        approvals.append({'acted_by': acted_by, 'decision': decision, 'note': note or '', 'acted_at': _utc_now()})
        required_approvals = max(1, int(row.get('required_approvals') or 1))
        approval_policy = str(row.get('approval_policy') or 'single_admin').strip().lower()
        if decision == 'rejected':
            await self._finalize_approval_request(request_id, status='rejected', acted_by=acted_by, result_json={'note': note or '', 'approvals': approvals}, approvals_json=approvals, rejection_reason_code=rejection_reason_code[:120], finalized_by_rule='single_rejection')
            row = await self.get_approval_request(request_id)
            return {'ok': True, 'final': True, 'status': 'rejected', 'row': row}
        unique_approvers = {str(item.get('acted_by') or '') for item in approvals if str(item.get('decision') or '') == 'approved'}
        if approval_policy == 'quorum' and len(unique_approvers) < required_approvals:
            await self._update_pending_approval_request(request_id, approvals_json=approvals, result_json={'note': note or '', 'approvals': approvals, 'waiting_for': required_approvals - len(unique_approvers)})
            row = await self.get_approval_request(request_id)
            return {'ok': True, 'final': False, 'status': 'pending', 'row': row, 'waiting_for': required_approvals - len(unique_approvers)}
        await self._finalize_approval_request(request_id, status='approved', acted_by=acted_by, result_json={'note': note or '', 'approvals': approvals}, approvals_json=approvals, finalized_by_rule='quorum' if approval_policy == 'quorum' and required_approvals > 1 else 'single_admin')
        row = await self.get_approval_request(request_id)
        return {'ok': True, 'final': True, 'status': 'approved', 'row': row}

    async def _update_pending_approval_request(self, request_id: int, *, approvals_json: list[dict[str, Any]], result_json: dict[str, Any]) -> None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "UPDATE approval_requests SET updated_at=datetime('now'), approvals_json=?, result_json=? WHERE id=? AND status='pending'",
                (_json(approvals_json), _json(result_json), request_id),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE approval_requests SET updated_at=NOW(), approvals_json=$1::jsonb, result_json=$2::jsonb WHERE id=$3 AND status='pending'",
                _json(approvals_json), _json(result_json), request_id,
            )

    async def _finalize_approval_request(self, request_id: int, *, status: str, acted_by: str, result_json: dict[str, Any], approvals_json: list[dict[str, Any]] | None = None, rejection_reason_code: str = '', finalized_by_rule: str = '') -> bool:
        approvals_json = approvals_json or []
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            cur = await conn.execute(
                "UPDATE approval_requests SET status=?, acted_by=?, acted_at=datetime('now'), updated_at=datetime('now'), result_json=?, approvals_json=?, rejection_reason_code=?, finalized_by_rule=? WHERE id=? AND status='pending'",
                (status, acted_by, _json(result_json), _json(approvals_json), rejection_reason_code[:120], finalized_by_rule[:120], request_id),
            )
            await conn.commit()
            return (cur.rowcount or 0) > 0
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE approval_requests SET status=$1, acted_by=$2, acted_at=NOW(), updated_at=NOW(), result_json=$3::jsonb, approvals_json=$4::jsonb, rejection_reason_code=$5, finalized_by_rule=$6 WHERE id=$7 AND status='pending'",
                status, acted_by, _json(result_json), _json(approvals_json), rejection_reason_code[:120], finalized_by_rule[:120], request_id,
            )
        return int(result.split()[-1]) > 0

    async def act_approval_request(self, request_id: int, *, status: str, acted_by: str, result_json: dict[str, Any] | None = None) -> bool:
        result_json = result_json or {}
        if status in {'approved', 'rejected'}:
            result = await self.decide_approval_request(request_id, decision=status, acted_by=acted_by, note=str(result_json.get('note') or ''), rejection_reason_code=str(result_json.get('rejection_reason_code') or ''))
            return bool(result and result.get('ok'))
        return await self._finalize_approval_request(request_id, status=status, acted_by=acted_by, result_json=result_json)

    async def list_expired_pending_approval_requests(self, *, limit: int = 50) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT * FROM approval_requests WHERE status='pending' AND expires_at IS NOT NULL AND expires_at <= datetime('now') ORDER BY expires_at ASC LIMIT ?", (limit,))).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                data = dict(row)
                data['payload_json'] = _parse_json_value(data.get('payload_json'), {})
                data['result_json'] = _parse_json_value(data.get('result_json'), {})
                data['approvals_json'] = _parse_json_value(data.get('approvals_json'), [])
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM approval_requests WHERE status='pending' AND expires_at IS NOT NULL AND expires_at <= NOW() ORDER BY expires_at ASC LIMIT $1", limit)
        result: list[dict[str, Any]] = []
        for row in rows:
            data = dict(row)
            data['approvals_json'] = _parse_json_value(data.get('approvals_json'), [])
            result.append(data)
        return result

    async def expire_pending_approval_requests(self, *, limit: int = 50, acted_by: str = 'system') -> list[int]:
        expired_ids: list[int] = []
        for row in await self.list_expired_pending_approval_requests(limit=limit):
            request_id = int(row.get('id') or 0)
            if not request_id:
                continue
            ok = await self._finalize_approval_request(
                request_id,
                status='expired',
                acted_by=acted_by,
                result_json={'expired': True, 'auto_expired': True, 'approvals': row.get('approvals_json') or []},
                approvals_json=list(row.get('approvals_json') or []),
                rejection_reason_code='expired',
                finalized_by_rule='expiry_sweeper',
            )
            if ok:
                expired_ids.append(request_id)
        return expired_ids


    async def upsert_platform_link(self, *, platform: str, platform_user_id: str, platform_username: str, guild_or_chat_id: str | None, minecraft_username: str, minecraft_uuid: str, metadata: dict[str, Any]) -> None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO platform_account_links(platform, platform_user_id, platform_username, guild_or_chat_id, minecraft_username, minecraft_uuid, metadata_json, linked_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now')) ON CONFLICT(platform, platform_user_id) DO UPDATE SET platform_username=excluded.platform_username, guild_or_chat_id=excluded.guild_or_chat_id, minecraft_username=excluded.minecraft_username, minecraft_uuid=excluded.minecraft_uuid, metadata_json=excluded.metadata_json, updated_at=datetime('now')",
                (platform, platform_user_id, platform_username, guild_or_chat_id, minecraft_username, minecraft_uuid, _json(metadata)),
            )
            await conn.commit()
        else:
            pool = self.storage.database.pool
            assert pool is not None
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO platform_account_links(platform, platform_user_id, platform_username, guild_or_chat_id, minecraft_username, minecraft_uuid, metadata_json, linked_at, updated_at) VALUES($1, $2, $3, $4, $5, $6, $7::jsonb, NOW(), NOW()) ON CONFLICT(platform, platform_user_id) DO UPDATE SET platform_username=EXCLUDED.platform_username, guild_or_chat_id=EXCLUDED.guild_or_chat_id, minecraft_username=EXCLUDED.minecraft_username, minecraft_uuid=EXCLUDED.minecraft_uuid, metadata_json=EXCLUDED.metadata_json, updated_at=NOW()",
                    platform, platform_user_id, platform_username, guild_or_chat_id, minecraft_username, minecraft_uuid, _json(metadata),
                )
        await self.sync_identity(
            minecraft_uuid=minecraft_uuid,
            minecraft_username=minecraft_username,
            discord_user_id=platform_user_id if platform == 'discord' else None,
            discord_username=platform_username if platform == 'discord' else None,
            telegram_user_id=platform_user_id if platform == 'telegram' else None,
            telegram_username=platform_username if platform == 'telegram' else None,
            vk_user_id=platform_user_id if platform == 'vk' else None,
            workspace_actor_id=platform_user_id if platform == 'workspace' else None,
            metadata=metadata,
        )

    async def remove_platform_link(self, *, platform: str, platform_user_id: str) -> None:
        existing = await self.get_platform_link(platform=platform, platform_user_id=platform_user_id)
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute("DELETE FROM platform_account_links WHERE platform=? AND platform_user_id=?", (platform, platform_user_id))
            if existing and existing.get('minecraft_uuid'):
                minecraft_uuid = str(existing['minecraft_uuid'])
                field_updates = {
                    'discord': "discord_user_id=NULL, discord_username=NULL",
                    'telegram': "telegram_user_id=NULL, telegram_username=NULL",
                    'vk': "vk_user_id=NULL",
                    'workspace': "workspace_actor_id=NULL",
                }
                update_clause = field_updates.get(platform)
                if update_clause:
                    await conn.execute(f"UPDATE community_identities SET {update_clause}, updated_at=datetime('now') WHERE minecraft_uuid=?", (minecraft_uuid,))
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM platform_account_links WHERE platform=$1 AND platform_user_id=$2", platform, platform_user_id)
            if existing and existing.get('minecraft_uuid'):
                minecraft_uuid = str(existing['minecraft_uuid'])
                field_updates = {
                    'discord': "discord_user_id=NULL, discord_username=NULL",
                    'telegram': "telegram_user_id=NULL, telegram_username=NULL",
                    'vk': "vk_user_id=NULL",
                    'workspace': "workspace_actor_id=NULL",
                }
                update_clause = field_updates.get(platform)
                if update_clause:
                    await conn.execute(f"UPDATE community_identities SET {update_clause}, updated_at=NOW() WHERE minecraft_uuid=$1", minecraft_uuid)

    async def get_platform_link(self, *, platform: str, platform_user_id: str) -> dict[str, Any] | None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM platform_account_links WHERE platform=? AND platform_user_id=? LIMIT 1", (platform, platform_user_id))).fetchone()
            if row is None:
                return None
            data = dict(row)
            data['metadata_json'] = json.loads(data.get('metadata_json') or '{}')
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM platform_account_links WHERE platform=$1 AND platform_user_id=$2 LIMIT 1", platform, platform_user_id)
        return dict(row) if row else None

    async def sync_identity(
        self,
        *,
        minecraft_uuid: str,
        minecraft_username: str,
        discord_user_id: str | None = None,
        discord_username: str | None = None,
        telegram_user_id: str | None = None,
        telegram_username: str | None = None,
        vk_user_id: str | None = None,
        workspace_actor_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        metadata = metadata or {}
        telegram_data = await self.get_telegram_identity_by_minecraft_uuid(minecraft_uuid)
        if telegram_data is not None:
            telegram_user_id = telegram_user_id or str(telegram_data.get('telegram_user_id') or '')
            telegram_username = telegram_username or str(telegram_data.get('telegram_username') or '')
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO community_identities(minecraft_uuid, minecraft_username, discord_user_id, discord_username, telegram_user_id, telegram_username, vk_user_id, workspace_actor_id, metadata_json, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now')) ON CONFLICT(minecraft_uuid) DO UPDATE SET minecraft_username=CASE WHEN excluded.minecraft_username!='' THEN excluded.minecraft_username ELSE community_identities.minecraft_username END, discord_user_id=COALESCE(NULLIF(excluded.discord_user_id,''), community_identities.discord_user_id), discord_username=COALESCE(NULLIF(excluded.discord_username,''), community_identities.discord_username), telegram_user_id=COALESCE(NULLIF(excluded.telegram_user_id,''), community_identities.telegram_user_id), telegram_username=COALESCE(NULLIF(excluded.telegram_username,''), community_identities.telegram_username), vk_user_id=COALESCE(NULLIF(excluded.vk_user_id,''), community_identities.vk_user_id), workspace_actor_id=COALESCE(NULLIF(excluded.workspace_actor_id,''), community_identities.workspace_actor_id), metadata_json=excluded.metadata_json, updated_at=datetime('now')",
                (minecraft_uuid, minecraft_username, discord_user_id or '', discord_username or '', telegram_user_id or '', telegram_username or '', vk_user_id or '', workspace_actor_id or '', _json(metadata)),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO community_identities(minecraft_uuid, minecraft_username, discord_user_id, discord_username, telegram_user_id, telegram_username, vk_user_id, workspace_actor_id, metadata_json, updated_at) VALUES($1, $2, NULLIF($3,''), NULLIF($4,''), NULLIF($5,''), NULLIF($6,''), NULLIF($7,''), NULLIF($8,''), $9::jsonb, NOW()) ON CONFLICT(minecraft_uuid) DO UPDATE SET minecraft_username=CASE WHEN EXCLUDED.minecraft_username!='' THEN EXCLUDED.minecraft_username ELSE community_identities.minecraft_username END, discord_user_id=COALESCE(EXCLUDED.discord_user_id, community_identities.discord_user_id), discord_username=COALESCE(EXCLUDED.discord_username, community_identities.discord_username), telegram_user_id=COALESCE(EXCLUDED.telegram_user_id, community_identities.telegram_user_id), telegram_username=COALESCE(EXCLUDED.telegram_username, community_identities.telegram_username), vk_user_id=COALESCE(EXCLUDED.vk_user_id, community_identities.vk_user_id), workspace_actor_id=COALESCE(EXCLUDED.workspace_actor_id, community_identities.workspace_actor_id), metadata_json=EXCLUDED.metadata_json, updated_at=NOW()",
                minecraft_uuid,
                minecraft_username,
                discord_user_id or '',
                discord_username or '',
                telegram_user_id or '',
                telegram_username or '',
                vk_user_id or '',
                workspace_actor_id or '',
                _json(metadata),
            )

    async def get_identity_by_minecraft_uuid(self, minecraft_uuid: str) -> dict[str, Any] | None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM community_identities WHERE minecraft_uuid=? LIMIT 1", (minecraft_uuid,))).fetchone()
            if row is None:
                return None
            data = dict(row)
            data['metadata_json'] = json.loads(data.get('metadata_json') or '{}')
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM community_identities WHERE minecraft_uuid=$1 LIMIT 1", minecraft_uuid)
        return dict(row) if row else None

    async def get_identity_by_discord_user_id(self, discord_user_id: str) -> dict[str, Any] | None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM community_identities WHERE discord_user_id=? LIMIT 1", (discord_user_id,))).fetchone()
            if row is None:
                return None
            data = dict(row)
            data['metadata_json'] = json.loads(data.get('metadata_json') or '{}')
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM community_identities WHERE discord_user_id=$1 LIMIT 1", discord_user_id)
        return dict(row) if row else None

    async def upsert_panel_binding(self, *, guild_id: str, panel_type: str, channel_id: str, message_id: str, version: str, metadata: dict[str, Any] | None = None) -> None:
        metadata = metadata or {}
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO panel_registry(guild_id, panel_type, channel_id, message_id, version, metadata_json, updated_at) VALUES(?, ?, ?, ?, ?, ?, datetime('now')) ON CONFLICT(guild_id, panel_type) DO UPDATE SET channel_id=excluded.channel_id, message_id=excluded.message_id, version=excluded.version, metadata_json=excluded.metadata_json, updated_at=datetime('now')",
                (guild_id, panel_type, channel_id, message_id, version, _json(metadata)),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO panel_registry(guild_id, panel_type, channel_id, message_id, version, metadata_json, updated_at) VALUES($1, $2, $3, $4, $5, $6::jsonb, NOW()) ON CONFLICT(guild_id, panel_type) DO UPDATE SET channel_id=EXCLUDED.channel_id, message_id=EXCLUDED.message_id, version=EXCLUDED.version, metadata_json=EXCLUDED.metadata_json, updated_at=NOW()",
                guild_id, panel_type, channel_id, message_id, version, _json(metadata),
            )

    async def get_panel_binding(self, *, guild_id: str, panel_type: str) -> dict[str, Any] | None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM panel_registry WHERE guild_id=? AND panel_type=? LIMIT 1", (guild_id, panel_type))).fetchone()
            if row is None:
                return None
            data = dict(row)
            data['metadata_json'] = json.loads(data.get('metadata_json') or '{}')
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM panel_registry WHERE guild_id=$1 AND panel_type=$2 LIMIT 1", guild_id, panel_type)
        return dict(row) if row else None

    async def list_recent_platform_link_events(self, *, player_uuid: str | None = None, platform_user_id: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            if player_uuid:
                rows = await (await conn.execute("SELECT * FROM platform_link_events WHERE player_uuid=? ORDER BY id DESC LIMIT ?", (player_uuid, limit))).fetchall()
            elif platform_user_id:
                rows = await (await conn.execute("SELECT * FROM platform_link_events WHERE platform_user_id=? ORDER BY id DESC LIMIT ?", (platform_user_id, limit))).fetchall()
            else:
                rows = await (await conn.execute("SELECT * FROM platform_link_events ORDER BY id DESC LIMIT ?", (limit,))).fetchall()
            return [dict(row) for row in rows]
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            if player_uuid:
                rows = await conn.fetch("SELECT * FROM platform_link_events WHERE player_uuid=$1 ORDER BY id DESC LIMIT $2", player_uuid, limit)
            elif platform_user_id:
                rows = await conn.fetch("SELECT * FROM platform_link_events WHERE platform_user_id=$1 ORDER BY id DESC LIMIT $2", platform_user_id, limit)
            else:
                rows = await conn.fetch("SELECT * FROM platform_link_events ORDER BY id DESC LIMIT $1", limit)
        return [dict(row) for row in rows]

    async def get_telegram_identity_by_minecraft_uuid(self, minecraft_uuid: str) -> dict[str, Any] | None:
        try:
            if isinstance(self.storage.database, SQLiteBackend):
                conn = self.storage.database.conn
                assert conn is not None
                row = await (await conn.execute("SELECT user_id, username, player_name, player_uuid FROM linked_accounts WHERE player_uuid=? LIMIT 1", (minecraft_uuid,))).fetchone()
                if row is None:
                    return None
                data = dict(row)
                return {
                    'telegram_user_id': str(data.get('user_id') or ''),
                    'telegram_username': str(data.get('username') or ''),
                    'minecraft_username': str(data.get('player_name') or ''),
                    'minecraft_uuid': str(data.get('player_uuid') or ''),
                }
            pool = self.storage.database.pool
            assert pool is not None
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT user_id, username, player_name, player_uuid FROM linked_accounts WHERE player_uuid=$1 LIMIT 1", minecraft_uuid)
            if row is None:
                return None
            data = dict(row)
            return {
                'telegram_user_id': str(data.get('user_id') or ''),
                'telegram_username': str(data.get('username') or ''),
                'minecraft_username': str(data.get('player_name') or ''),
                'minecraft_uuid': str(data.get('player_uuid') or ''),
            }
        except Exception:
            return None

    async def add_platform_link_event(
        self,
        *,
        platform: str,
        event: str,
        platform_user_id: str | None,
        admin_user_id: str | None,
        player_name: str | None,
        player_uuid: str | None,
        details: dict[str, Any],
    ) -> None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO platform_link_events(platform, event, platform_user_id, admin_user_id, player_name, player_uuid, details_json) VALUES(?, ?, ?, ?, ?, ?, ?)",
                (platform, event, platform_user_id, admin_user_id, player_name, player_uuid, _json(details)),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO platform_link_events(platform, event, platform_user_id, admin_user_id, player_name, player_uuid, details_json) VALUES($1, $2, $3, $4, $5, $6, $7::jsonb)",
                platform,
                event,
                platform_user_id,
                admin_user_id,
                player_name,
                player_uuid,
                _json(details),
            )


    async def list_panel_bindings(self, *, guild_id: str) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT * FROM panel_registry WHERE guild_id=? ORDER BY panel_type", (guild_id,))).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                try:
                    data['metadata_json']=json.loads(data.get('metadata_json') or '{}')
                except Exception:
                    data['metadata_json']={}
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM panel_registry WHERE guild_id=$1 ORDER BY panel_type", guild_id)
        return [dict(row) for row in rows]

    async def upsert_layout_alias_binding(self, *, guild_id: str, alias: str, resource_type: str, discord_id: str, metadata: dict[str, Any] | None = None) -> None:
        metadata = metadata or {}
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO layout_alias_bindings(guild_id, alias, resource_type, discord_id, metadata_json, updated_at) VALUES(?, ?, ?, ?, ?, datetime('now')) ON CONFLICT(guild_id, alias, resource_type) DO UPDATE SET discord_id=excluded.discord_id, metadata_json=excluded.metadata_json, updated_at=datetime('now')",
                (guild_id, alias, resource_type, discord_id, _json(metadata)),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO layout_alias_bindings(guild_id, alias, resource_type, discord_id, metadata_json, updated_at) VALUES($1, $2, $3, $4, $5::jsonb, NOW()) ON CONFLICT(guild_id, alias, resource_type) DO UPDATE SET discord_id=EXCLUDED.discord_id, metadata_json=EXCLUDED.metadata_json, updated_at=NOW()",
                guild_id, alias, resource_type, discord_id, _json(metadata),
            )

    async def list_layout_alias_bindings(self, *, guild_id: str, resource_type: str | None = None) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            if resource_type:
                rows = await (await conn.execute("SELECT * FROM layout_alias_bindings WHERE guild_id=? AND resource_type=? ORDER BY alias", (guild_id, resource_type))).fetchall()
            else:
                rows = await (await conn.execute("SELECT * FROM layout_alias_bindings WHERE guild_id=? ORDER BY resource_type, alias", (guild_id,))).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                try:
                    data['metadata_json']=json.loads(data.get('metadata_json') or '{}')
                except Exception:
                    data['metadata_json']={}
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            if resource_type:
                rows = await conn.fetch("SELECT * FROM layout_alias_bindings WHERE guild_id=$1 AND resource_type=$2 ORDER BY alias", guild_id, resource_type)
            else:
                rows = await conn.fetch("SELECT * FROM layout_alias_bindings WHERE guild_id=$1 ORDER BY resource_type, alias", guild_id)
        return [dict(row) for row in rows]

    async def register_forum_topic(self, *, thread_id: str, guild_id: str, forum_channel_id: str, topic_kind: str, owner_user_id: str | None, title: str, tags: list[str] | None = None, metadata: dict[str, Any] | None = None, auto_close_after_seconds: int | None = None) -> None:
        tags = tags or []
        metadata = metadata or {}
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO forum_topic_registry(thread_id, guild_id, forum_channel_id, topic_kind, owner_user_id, status, title, tags_json, metadata_json, updated_at, auto_close_after_seconds) VALUES(?, ?, ?, ?, ?, 'open', ?, ?, ?, datetime('now'), ?) ON CONFLICT(thread_id) DO UPDATE SET guild_id=excluded.guild_id, forum_channel_id=excluded.forum_channel_id, topic_kind=excluded.topic_kind, owner_user_id=excluded.owner_user_id, title=excluded.title, tags_json=excluded.tags_json, metadata_json=excluded.metadata_json, updated_at=datetime('now'), auto_close_after_seconds=excluded.auto_close_after_seconds",
                (thread_id, guild_id, forum_channel_id, topic_kind, owner_user_id, title, _json(tags), _json(metadata), auto_close_after_seconds),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO forum_topic_registry(thread_id, guild_id, forum_channel_id, topic_kind, owner_user_id, status, title, tags_json, metadata_json, updated_at, auto_close_after_seconds) VALUES($1,$2,$3,$4,$5,'open',$6,$7::jsonb,$8::jsonb,NOW(),$9) ON CONFLICT(thread_id) DO UPDATE SET guild_id=EXCLUDED.guild_id, forum_channel_id=EXCLUDED.forum_channel_id, topic_kind=EXCLUDED.topic_kind, owner_user_id=EXCLUDED.owner_user_id, title=EXCLUDED.title, tags_json=EXCLUDED.tags_json, metadata_json=EXCLUDED.metadata_json, updated_at=NOW(), auto_close_after_seconds=EXCLUDED.auto_close_after_seconds",
                thread_id, guild_id, forum_channel_id, topic_kind, owner_user_id, title, _json(tags), _json(metadata), auto_close_after_seconds,
            )

    async def get_forum_topic(self, thread_id: str) -> dict[str, Any] | None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM forum_topic_registry WHERE thread_id=? LIMIT 1", (thread_id,))).fetchone()
            if row is None:
                return None
            data=dict(row)
            data['tags_json']=json.loads(data.get('tags_json') or '[]')
            data['metadata_json']=json.loads(data.get('metadata_json') or '{}')
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM forum_topic_registry WHERE thread_id=$1 LIMIT 1", thread_id)
        return dict(row) if row else None

    async def update_forum_topic_state(self, *, thread_id: str, status: str, tags: list[str] | None = None, metadata: dict[str, Any] | None = None, closed: bool = False) -> None:
        tags = tags or []
        metadata = metadata or {}
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute("UPDATE forum_topic_registry SET status=?, tags_json=?, metadata_json=?, updated_at=datetime('now'), closed_at=CASE WHEN ? THEN datetime('now') ELSE closed_at END WHERE thread_id=?", (status, _json(tags), _json(metadata), 1 if closed else 0, thread_id))
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute("UPDATE forum_topic_registry SET status=$1, tags_json=$2::jsonb, metadata_json=$3::jsonb, updated_at=NOW(), closed_at=CASE WHEN $4 THEN NOW() ELSE closed_at END WHERE thread_id=$5", status, _json(tags), _json(metadata), closed, thread_id)

    async def list_stale_forum_topics(self) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn=self.storage.database.conn
            assert conn is not None
            rows=await (await conn.execute("SELECT * FROM forum_topic_registry WHERE status IN ('open','in_review') AND auto_close_after_seconds IS NOT NULL AND updated_at <= datetime('now', '-' || auto_close_after_seconds || ' seconds') ORDER BY updated_at ASC LIMIT 50")).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                data['tags_json']=json.loads(data.get('tags_json') or '[]')
                data['metadata_json']=json.loads(data.get('metadata_json') or '{}')
                result.append(data)
            return result
        pool=self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows=await conn.fetch("SELECT * FROM forum_topic_registry WHERE status IN ('open','in_review') AND auto_close_after_seconds IS NOT NULL AND updated_at <= (NOW() - (auto_close_after_seconds || ' seconds')::interval) ORDER BY updated_at ASC LIMIT 50")
        return [dict(row) for row in rows]

    async def schedule_job(self, *, job_type: str, run_at: str, payload: dict[str, Any], guild_id: str | None = None, channel_id: str | None = None, created_by: str | None = None, dedupe_key: str | None = None) -> int:
        dedupe_key = str(dedupe_key or _fingerprint_json({'job_type': job_type, 'guild_id': guild_id or '', 'channel_id': channel_id or '', 'run_at': run_at, 'payload': payload}))[:190]
        existing = await self.find_scheduled_job_by_dedupe_key(dedupe_key)
        if existing is not None:
            return int(existing.get('id') or 0)
        if isinstance(self.storage.database, SQLiteBackend):
            conn=self.storage.database.conn
            assert conn is not None
            cur=await conn.execute("INSERT INTO scheduled_jobs(job_type, guild_id, channel_id, payload_json, run_at, created_by, status, updated_at, dedupe_key) VALUES(?, ?, ?, ?, ?, ?, 'pending', datetime('now'), ?)", (job_type, guild_id, channel_id, _json(payload), run_at, created_by, dedupe_key))
            await conn.commit()
            return int(cur.lastrowid or 0)
        pool=self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row=await conn.fetchrow("INSERT INTO scheduled_jobs(job_type, guild_id, channel_id, payload_json, run_at, created_by, status, updated_at, dedupe_key) VALUES($1,$2,$3,$4::jsonb,$5::timestamptz,$6,'pending',NOW(),$7) RETURNING id", job_type, guild_id, channel_id, _json(payload), run_at, created_by, dedupe_key)
        return int(row['id']) if row else 0

    async def create_scheduled_job(self, *, job_type: str, guild_id: str | None = None, channel_id: str | None = None, payload: dict[str, Any] | None = None, run_at: str, created_by: str | None = None, dedupe_key: str | None = None) -> int:
        return await self.schedule_job(job_type=job_type, run_at=run_at, payload=payload or {}, guild_id=guild_id, channel_id=channel_id, created_by=created_by, dedupe_key=dedupe_key)

    async def find_scheduled_job_by_dedupe_key(self, dedupe_key: str) -> dict[str, Any] | None:
        if not dedupe_key:
            return None
        if isinstance(self.storage.database, SQLiteBackend):
            conn=self.storage.database.conn
            assert conn is not None
            row=await (await conn.execute("SELECT * FROM scheduled_jobs WHERE dedupe_key=? LIMIT 1", (dedupe_key,))).fetchone()
            if row is None:
                return None
            data=dict(row)
            try:
                data['payload_json']=json.loads(data.get('payload_json') or '{}')
            except Exception:
                data['payload_json']={}
            return data
        pool=self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row=await conn.fetchrow("SELECT * FROM scheduled_jobs WHERE dedupe_key=$1 LIMIT 1", dedupe_key)
        return dict(row) if row else None

    async def list_due_scheduled_jobs(self, *, limit: int = 25) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn=self.storage.database.conn
            assert conn is not None
            rows=await (await conn.execute("SELECT * FROM scheduled_jobs WHERE status IN ('pending','retry') AND COALESCE(next_retry_at, run_at) <= datetime('now') ORDER BY COALESCE(next_retry_at, run_at) ASC LIMIT ?", (limit,))).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                data['payload_json']=json.loads(data.get('payload_json') or '{}')
                result.append(data)
            return result
        pool=self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows=await conn.fetch("SELECT * FROM scheduled_jobs WHERE status = ANY($1::text[]) AND COALESCE(next_retry_at, run_at) <= NOW() ORDER BY COALESCE(next_retry_at, run_at) ASC LIMIT $2", ['pending','retry'], limit)
        return [dict(row) for row in rows]

    async def mark_scheduled_job(self, job_id: int, *, status: str, error: str = '', backoff_seconds: int = 0, dead_letter_reason_code: str = '') -> None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn=self.storage.database.conn
            assert conn is not None
            if status == 'sent':
                await conn.execute("UPDATE scheduled_jobs SET status=?, updated_at=datetime('now'), sent_at=datetime('now'), last_error=?, attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, datetime('now')), last_attempt_at=datetime('now'), next_retry_at=NULL, backoff_seconds=0, dead_letter_reason_code='' WHERE id=?", (status, error[:2000], job_id))
            elif status == 'retry':
                await conn.execute("UPDATE scheduled_jobs SET status=?, updated_at=datetime('now'), last_error=?, attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, datetime('now')), last_attempt_at=datetime('now'), next_retry_at=datetime('now', '+' || ? || ' seconds'), backoff_seconds=?, dead_letter_reason_code='' WHERE id=?", (status, error[:2000], int(backoff_seconds or 0), int(backoff_seconds or 0), job_id))
            elif status == 'dead_letter':
                await conn.execute("UPDATE scheduled_jobs SET status=?, updated_at=datetime('now'), last_error=?, attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, datetime('now')), last_attempt_at=datetime('now'), next_retry_at=NULL, backoff_seconds=?, dead_letter_reason_code=? WHERE id=?", (status, error[:2000], int(backoff_seconds or 0), dead_letter_reason_code[:120], job_id))
            else:
                await conn.execute("UPDATE scheduled_jobs SET status=?, updated_at=datetime('now'), last_error=?, next_retry_at=NULL, backoff_seconds=0, dead_letter_reason_code=? WHERE id=?", (status, error[:2000], dead_letter_reason_code[:120], job_id))
            await conn.commit()
            return
        pool=self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            if status == 'sent':
                await conn.execute("UPDATE scheduled_jobs SET status=$1, updated_at=NOW(), sent_at=NOW(), last_error=$2, attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, NOW()), last_attempt_at=NOW(), next_retry_at=NULL, backoff_seconds=0, dead_letter_reason_code='' WHERE id=$3", status, error[:2000], job_id)
            elif status == 'retry':
                await conn.execute("UPDATE scheduled_jobs SET status=$1, updated_at=NOW(), last_error=$2, attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, NOW()), last_attempt_at=NOW(), next_retry_at=NOW() + ($3::int * INTERVAL '1 second'), backoff_seconds=$3, dead_letter_reason_code='' WHERE id=$4", status, error[:2000], int(backoff_seconds or 0), job_id)
            elif status == 'dead_letter':
                await conn.execute("UPDATE scheduled_jobs SET status=$1, updated_at=NOW(), last_error=$2, attempt_count=attempt_count+1, first_attempt_at=COALESCE(first_attempt_at, NOW()), last_attempt_at=NOW(), next_retry_at=NULL, backoff_seconds=$3, dead_letter_reason_code=$4 WHERE id=$5", status, error[:2000], int(backoff_seconds or 0), dead_letter_reason_code[:120], job_id)
            else:
                await conn.execute("UPDATE scheduled_jobs SET status=$1, updated_at=NOW(), last_error=$2, next_retry_at=NULL, backoff_seconds=0, dead_letter_reason_code=$3 WHERE id=$4", status, error[:2000], dead_letter_reason_code[:120], job_id)

    async def upsert_subscription_preferences(self, *, platform: str, platform_user_id: str, preferences: dict[str, Any], minecraft_uuid: str | None = None) -> None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn=self.storage.database.conn
            assert conn is not None
            await conn.execute("INSERT INTO subscription_preferences(platform, platform_user_id, minecraft_uuid, preferences_json, updated_at) VALUES(?, ?, ?, ?, datetime('now')) ON CONFLICT(platform, platform_user_id) DO UPDATE SET minecraft_uuid=excluded.minecraft_uuid, preferences_json=excluded.preferences_json, updated_at=datetime('now')", (platform, platform_user_id, minecraft_uuid, _json(preferences)))
            await conn.commit()
            return
        pool=self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO subscription_preferences(platform, platform_user_id, minecraft_uuid, preferences_json, updated_at) VALUES($1,$2,$3,$4::jsonb,NOW()) ON CONFLICT(platform, platform_user_id) DO UPDATE SET minecraft_uuid=EXCLUDED.minecraft_uuid, preferences_json=EXCLUDED.preferences_json, updated_at=NOW()", platform, platform_user_id, minecraft_uuid, _json(preferences))

    async def get_subscription_preferences(self, *, platform: str, platform_user_id: str) -> dict[str, Any] | None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn=self.storage.database.conn
            assert conn is not None
            row=await (await conn.execute("SELECT preferences_json, minecraft_uuid FROM subscription_preferences WHERE platform=? AND platform_user_id=? LIMIT 1", (platform, platform_user_id))).fetchone()
            if row is None:
                return None
            return {'preferences': json.loads(row['preferences_json'] or '{}'), 'minecraft_uuid': row['minecraft_uuid']}
        pool=self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row=await conn.fetchrow("SELECT preferences_json, minecraft_uuid FROM subscription_preferences WHERE platform=$1 AND platform_user_id=$2 LIMIT 1", platform, platform_user_id)
        if row is None:
            return None
        return {'preferences': row['preferences_json'], 'minecraft_uuid': row['minecraft_uuid']}

    async def record_rules_acceptance(self, *, guild_id: str, discord_user_id: str, accepted_rules_version: str, panel_version: str, metadata: dict[str, Any] | None = None) -> None:
        metadata = metadata or {}
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO rules_acceptance(guild_id, discord_user_id, accepted_rules_version, panel_version, accepted_at, metadata_json) VALUES(?, ?, ?, ?, datetime('now'), ?) ON CONFLICT(guild_id, discord_user_id) DO UPDATE SET accepted_rules_version=excluded.accepted_rules_version, panel_version=excluded.panel_version, accepted_at=datetime('now'), metadata_json=excluded.metadata_json",
                (guild_id, discord_user_id, accepted_rules_version, panel_version, _json(metadata)),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO rules_acceptance(guild_id, discord_user_id, accepted_rules_version, panel_version, accepted_at, metadata_json) VALUES($1,$2,$3,$4,NOW(),$5::jsonb) ON CONFLICT(guild_id, discord_user_id) DO UPDATE SET accepted_rules_version=EXCLUDED.accepted_rules_version, panel_version=EXCLUDED.panel_version, accepted_at=NOW(), metadata_json=EXCLUDED.metadata_json",
                guild_id, discord_user_id, accepted_rules_version, panel_version, _json(metadata),
            )

    async def get_rules_acceptance(self, *, guild_id: str, discord_user_id: str) -> dict[str, Any] | None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM rules_acceptance WHERE guild_id=? AND discord_user_id=? LIMIT 1", (guild_id, discord_user_id))).fetchone()
            if row is None:
                return None
            data = dict(row)
            data['metadata_json'] = json.loads(data.get('metadata_json') or '{}')
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM rules_acceptance WHERE guild_id=$1 AND discord_user_id=$2 LIMIT 1", guild_id, discord_user_id)
        return dict(row) if row else None

    async def log_panel_drift(self, *, guild_id: str, panel_type: str, old_version: str, new_version: str, reason: str, details: dict[str, Any] | None = None) -> None:
        details = details or {}
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO panel_drift_log(guild_id, panel_type, old_version, new_version, reason, created_at, details_json) VALUES(?, ?, ?, ?, ?, datetime('now'), ?)",
                (guild_id, panel_type, old_version, new_version, reason, _json(details)),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO panel_drift_log(guild_id, panel_type, old_version, new_version, reason, created_at, details_json) VALUES($1,$2,$3,$4,$5,NOW(),$6::jsonb)",
                guild_id, panel_type, old_version, new_version, reason, _json(details),
            )

    async def list_recent_panel_drift(self, *, guild_id: str, limit: int = 10) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT * FROM panel_drift_log WHERE guild_id=? ORDER BY created_at DESC LIMIT ?", (guild_id, limit))).fetchall()
            result=[]
            for row in rows:
                data = dict(row)
                data['details_json'] = json.loads(data.get('details_json') or '{}')
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM panel_drift_log WHERE guild_id=$1 ORDER BY created_at DESC LIMIT $2", guild_id, limit)
        return [dict(row) for row in rows]

    async def assign_forum_topic_owner(self, *, thread_id: str, staff_user_id: str, staff_name: str) -> None:
        current = await self.get_forum_topic(thread_id)
        if current is None:
            return
        metadata = current.get('metadata_json') or {}
        metadata.update({
            'staff_owner_user_id': staff_user_id,
            'staff_owner_name': staff_name,
            'staff_owner_assigned_at': _utc_now(),
        })
        await self.update_forum_topic_state(thread_id=thread_id, status=str(current.get('status') or 'open'), tags=list(current.get('tags_json') or []), metadata=metadata, closed=bool(current.get('closed_at')))

    async def note_forum_staff_response(self, *, thread_id: str, staff_user_id: str, staff_name: str) -> None:
        current = await self.get_forum_topic(thread_id)
        if current is None:
            return
        metadata = current.get('metadata_json') or {}
        metadata['last_staff_response_at'] = _utc_now()
        metadata['last_staff_response_by'] = staff_user_id
        metadata['last_staff_response_name'] = staff_name
        if 'staff_owner_user_id' not in metadata:
            metadata['staff_owner_user_id'] = staff_user_id
            metadata['staff_owner_name'] = staff_name
            metadata['staff_owner_assigned_at'] = _utc_now()
        await self.update_forum_topic_state(thread_id=thread_id, status=str(current.get('status') or 'open'), tags=list(current.get('tags_json') or []), metadata=metadata, closed=bool(current.get('closed_at')))

    async def list_topics_needing_escalation(self, *, topic_kind: str, older_than_hours: int, limit: int = 25) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        topics: list[dict[str, Any]] = []
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT * FROM forum_topic_registry WHERE topic_kind=? AND status IN ('open','in_review') ORDER BY updated_at ASC LIMIT ?", (topic_kind, limit * 4))).fetchall()
            topics = []
            for row in rows:
                data = dict(row)
                data['tags_json'] = json.loads(data.get('tags_json') or '[]')
                data['metadata_json'] = json.loads(data.get('metadata_json') or '{}')
                topics.append(data)
        else:
            pool = self.storage.database.pool
            assert pool is not None
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT * FROM forum_topic_registry WHERE topic_kind=$1 AND status IN ('open','in_review') ORDER BY updated_at ASC LIMIT $2", topic_kind, limit * 4)
            topics = [dict(row) for row in rows]
        result=[]
        for topic in topics:
            metadata = topic.get('metadata_json') or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except Exception:
                    metadata = {}
            if metadata.get('escalated_at'):
                continue
            last_staff = metadata.get('last_staff_response_at') or topic.get('updated_at') or topic.get('created_at')
            try:
                dt = datetime.fromisoformat(str(last_staff).replace('Z', '+00:00').replace(' ', 'T'))
            except Exception:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if now - dt >= timedelta(hours=older_than_hours):
                result.append(topic)
            if len(result) >= limit:
                break
        return result

    async def mark_topic_escalated(self, *, thread_id: str, reason: str) -> None:
        current = await self.get_forum_topic(thread_id)
        if current is None:
            return
        metadata = current.get('metadata_json') or {}
        metadata['escalated_at'] = _utc_now()
        metadata['escalation_reason'] = reason
        await self.update_forum_topic_state(thread_id=thread_id, status=str(current.get('status') or 'open'), tags=list(current.get('tags_json') or []), metadata=metadata, closed=bool(current.get('closed_at')))

    async def get_external_sync_delivery_stats(self, *, since_hours: int | None = None) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            if since_hours and since_hours > 0:
                rows = await (await conn.execute("SELECT destination, status, COUNT(*) AS total, MAX(updated_at) AS last_seen, MAX(CASE WHEN status='sent' THEN updated_at END) AS last_success, MAX(CASE WHEN status!='sent' THEN updated_at END) AS last_failure, MAX(CASE WHEN status!='sent' THEN last_error END) AS last_error FROM external_sync_events WHERE updated_at >= datetime('now', '-' || ? || ' hours') GROUP BY destination, status ORDER BY destination, status", (since_hours,))).fetchall()
            else:
                rows = await (await conn.execute("SELECT destination, status, COUNT(*) AS total, MAX(updated_at) AS last_seen, MAX(CASE WHEN status='sent' THEN updated_at END) AS last_success, MAX(CASE WHEN status!='sent' THEN updated_at END) AS last_failure, MAX(CASE WHEN status!='sent' THEN last_error END) AS last_error FROM external_sync_events GROUP BY destination, status ORDER BY destination, status")).fetchall()
            return [dict(row) for row in rows]
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            if since_hours and since_hours > 0:
                rows = await conn.fetch("SELECT destination, status, COUNT(*) AS total, MAX(updated_at) AS last_seen, MAX(CASE WHEN status='sent' THEN updated_at END) AS last_success, MAX(CASE WHEN status!='sent' THEN updated_at END) AS last_failure, MAX(CASE WHEN status!='sent' THEN last_error END) AS last_error FROM external_sync_events WHERE updated_at >= NOW() - ($1::int * INTERVAL '1 hour') GROUP BY destination, status ORDER BY destination, status", since_hours)
            else:
                rows = await conn.fetch("SELECT destination, status, COUNT(*) AS total, MAX(updated_at) AS last_seen, MAX(CASE WHEN status='sent' THEN updated_at END) AS last_success, MAX(CASE WHEN status!='sent' THEN updated_at END) AS last_failure, MAX(CASE WHEN status!='sent' THEN last_error END) AS last_error FROM external_sync_events GROUP BY destination, status ORDER BY destination, status")
        return [dict(row) for row in rows]

    async def get_rules_acceptance_stats(self, *, guild_id: str, current_rules_version: str) -> dict[str, int]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            total = await (await conn.execute("SELECT COUNT(*) FROM rules_acceptance WHERE guild_id=?", (guild_id,))).fetchone()
            current = await (await conn.execute("SELECT COUNT(*) FROM rules_acceptance WHERE guild_id=? AND accepted_rules_version=?", (guild_id, current_rules_version))).fetchone()
            return {"total": int(total[0]) if total else 0, "current_version": int(current[0]) if current else 0}
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM rules_acceptance WHERE guild_id=$1", guild_id)
            current = await conn.fetchval("SELECT COUNT(*) FROM rules_acceptance WHERE guild_id=$1 AND accepted_rules_version=$2", guild_id, current_rules_version)
        return {"total": int(total or 0), "current_version": int(current or 0)}

    async def get_subscription_preferences_stats(self, *, platform: str = 'discord') -> dict[str, int]:
        rows: list[dict[str, Any]] = []
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            fetched = await (await conn.execute("SELECT preferences_json FROM subscription_preferences WHERE platform=?", (platform,))).fetchall()
            rows = [{'preferences_json': row['preferences_json']} for row in fetched]
        else:
            pool = self.storage.database.pool
            assert pool is not None
            async with pool.acquire() as conn:
                fetched = await conn.fetch("SELECT preferences_json FROM subscription_preferences WHERE platform=$1", platform)
            rows = [dict(row) for row in fetched]
        stats: dict[str, int] = {}
        for row in rows:
            prefs = row.get('preferences_json') or {}
            if isinstance(prefs, str):
                try:
                    prefs = json.loads(prefs)
                except Exception:
                    prefs = {}
            for role_id in prefs.get('interest_roles') or []:
                key = str(role_id)
                stats[key] = stats.get(key, 0) + 1
        return stats

    async def list_subscription_preferences(self, *, platform: str = 'discord', limit: int = 500) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT platform, platform_user_id, minecraft_uuid, preferences_json, updated_at FROM subscription_preferences WHERE platform=? ORDER BY updated_at DESC LIMIT ?", (platform, limit))).fetchall()
            result = []
            for row in rows:
                data = dict(row)
                data['preferences_json'] = _parse_json_value(data.get('preferences_json'), {})
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT platform, platform_user_id, minecraft_uuid, preferences_json, updated_at FROM subscription_preferences WHERE platform=$1 ORDER BY updated_at DESC LIMIT $2", platform, limit)
        result = []
        for row in rows:
            data = dict(row)
            data['preferences_json'] = _parse_json_value(data.get('preferences_json'), {})
            result.append(data)
        return result

    async def list_matching_subscription_targets(
        self,
        *,
        platform: str = 'discord',
        interest_role_ids: list[int] | tuple[int, ...] | None = None,
        digest_kind: str | None = None,
        event_kind: str | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        interest = {str(int(item)) for item in (interest_role_ids or []) if str(item).isdigit()}
        digest_key = str(digest_kind or '').strip().lower()
        event_key = str(event_kind or '').strip().lower()
        result: list[dict[str, Any]] = []
        for row in await self.list_subscription_preferences(platform=platform, limit=limit):
            prefs = row.get('preferences_json') or {}
            selected_roles = {str(item) for item in prefs.get('interest_roles') or []}
            selected_digests = {str(item).strip().lower() for item in prefs.get('digests') or []}
            selected_events = {str(item).strip().lower() for item in prefs.get('event_kinds') or []}

            def _event_selected() -> bool:
                if not event_key or not selected_events:
                    return False
                if event_key in selected_events or '*' in selected_events or 'community.*' in selected_events:
                    return True
                for candidate in selected_events:
                    if candidate.endswith('.*') and event_key.startswith(candidate[:-1]):
                        return True
                    if candidate.endswith('.') and event_key.startswith(candidate):
                        return True
                    if candidate and event_key.startswith(candidate + '.'):
                        return True
                return False

            matched = False
            if interest and selected_roles.intersection(interest):
                matched = True
            if digest_key and (digest_key in selected_digests or '*' in selected_digests):
                matched = True
            if _event_selected():
                matched = True
            if matched:
                result.append(row)
        return result

    async def list_rules_reacceptance_candidates(self, *, guild_id: str, current_rules_version: str, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT * FROM rules_acceptance WHERE guild_id=? AND accepted_rules_version!=? ORDER BY accepted_at ASC LIMIT ?", (guild_id, current_rules_version, limit))).fetchall()
            result = []
            for row in rows:
                data = dict(row)
                data['metadata_json'] = _parse_json_value(data.get('metadata_json'), {})
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM rules_acceptance WHERE guild_id=$1 AND accepted_rules_version!=$2 ORDER BY accepted_at ASC LIMIT $3", guild_id, current_rules_version, limit)
        result = []
        for row in rows:
            data = dict(row)
            data['metadata_json'] = _parse_json_value(data.get('metadata_json'), {})
            result.append(data)
        return result


    def schema_parity_issues(self) -> list[str]:
        issues: list[str] = []

        def _table_block(schema: str, table: str) -> str:
            match = re.search(rf"CREATE TABLE IF NOT EXISTS {re.escape(table)} \((.*?)\);", schema, re.S)
            return match.group(1) if match else ''

        def _table_columns(schema: str, table: str) -> set[str]:
            block = _table_block(schema, table)
            columns: set[str] = set()
            for raw in block.splitlines():
                line = raw.strip().rstrip(',')
                if not line or line.upper().startswith(('PRIMARY KEY', 'UNIQUE', 'CONSTRAINT', 'FOREIGN KEY')):
                    continue
                token = line.split()[0]
                if token and token.upper() not in {'PRIMARY', 'UNIQUE', 'CONSTRAINT', 'FOREIGN'}:
                    columns.add(token)
            return columns

        def _column_line(schema: str, table: str, column: str) -> str:
            block = _table_block(schema, table)
            for raw in block.splitlines():
                line = raw.strip().rstrip(',')
                if line.startswith(f'{column} '):
                    return line
            return ''

        sqlite_indexes = re.findall(r"CREATE INDEX IF NOT EXISTS\s+([A-Za-z0-9_]+)", SQLITE_SCHEMA)
        postgres_indexes = re.findall(r"CREATE INDEX IF NOT EXISTS\s+([A-Za-z0-9_]+)", POSTGRES_SCHEMA)
        for source, names in (("sqlite", sqlite_indexes), ("postgres", postgres_indexes)):
            seen: set[str] = set()
            for name in names:
                if name in seen:
                    issues.append(f'{source}:duplicate_index:{name}')
                seen.add(name)

        if 'JSONB' in SQLITE_SCHEMA or 'TIMESTAMPTZ' in SQLITE_SCHEMA or "'{}'::jsonb" in SQLITE_SCHEMA or 'NOW()' in SQLITE_SCHEMA:
            issues.append('sqlite:contains_postgres_tokens')

        required_tables = {'schema_meta', 'approval_requests', 'forum_topic_registry', 'scheduled_jobs', 'subscription_preferences', 'rules_acceptance', 'bridge_destination_state', 'bridge_comment_mirror', 'external_discussion_mirror', 'external_content_mirror', 'legacy_layout_resources', 'community_schema_migrations'}
        for source_name, schema in (("sqlite", SQLITE_SCHEMA), ("postgres", POSTGRES_SCHEMA)):
            for table in required_tables:
                if f"CREATE TABLE IF NOT EXISTS {table} (" not in schema:
                    issues.append(f'{source_name}:missing_table:{table}')

        required_columns = {
            'approval_requests': {'expires_at', 'required_approvals', 'approval_policy', 'approvals_json', 'rejection_reason_code', 'finalized_by_rule'},
            'scheduled_jobs': {'next_retry_at', 'attempt_count', 'dead_letter_reason_code', 'backoff_seconds', 'dedupe_key'},
            'external_sync_events': {'next_retry_at', 'attempt_count', 'dead_letter_reason_code', 'backoff_seconds', 'dedupe_key'},
            'subscription_preferences': {'preferences_json', 'minecraft_uuid'},
            'rules_acceptance': {'accepted_rules_version', 'panel_version', 'metadata_json', 'accepted_at'},
            'bridge_destination_state': {'circuit_state', 'consecutive_failures', 'metadata_json', 'circuit_open_until'},
            'bridge_comment_mirror': {'thread_id', 'source_platform', 'external_comment_id', 'discord_message_id', 'metadata_json', 'updated_at'},
            'external_discussion_mirror': {'source_platform', 'external_topic_id', 'topic_kind', 'discord_object_id', 'channel_id', 'metadata_json', 'updated_at'},
            'external_content_mirror': {'source_platform', 'content_kind', 'external_content_id', 'discord_channel_id', 'discord_message_id', 'metadata_json', 'updated_at'},
            'legacy_layout_resources': {'guild_id', 'resource_type', 'discord_id', 'resource_name', 'status', 'marked_at', 'review_after', 'delete_after', 'metadata_json'},
            'layout_alias_bindings': {'guild_id', 'alias', 'resource_type', 'discord_id', 'metadata_json'},
            'panel_registry': {'guild_id', 'panel_type', 'channel_id', 'message_id', 'version', 'metadata_json'},
            'forum_topic_registry': {'thread_id', 'guild_id', 'forum_channel_id', 'topic_kind', 'owner_user_id', 'status', 'title', 'tags_json', 'metadata_json', 'auto_close_after_seconds'},
            'panel_drift_log': {'guild_id', 'panel_type', 'old_version', 'new_version', 'reason', 'details_json'},
            'community_schema_migrations': {'version', 'name', 'applied_at', 'source'},
        }
        for table, required in required_columns.items():
            sqlite_cols = _table_columns(SQLITE_SCHEMA, table)
            postgres_cols = _table_columns(POSTGRES_SCHEMA, table)
            for column in sorted(required - sqlite_cols):
                issues.append(f'sqlite:missing_column:{table}.{column}')
            for column in sorted(required - postgres_cols):
                issues.append(f'postgres:missing_column:{table}.{column}')
            for column in sorted((sqlite_cols ^ postgres_cols) - {'id'}):
                issues.append(f'schema:column_parity:{table}.{column}')

        required_indexes = {'idx_external_sync_events_due_retry', 'idx_scheduled_jobs_due_retry', 'idx_layout_alias_bindings_discord_id', 'idx_subscription_preferences_uuid', 'idx_bridge_comment_mirror_message_id', 'idx_external_discussion_mirror_discord_object_id', 'idx_external_content_mirror_message_id', 'idx_legacy_layout_resources_status', 'idx_panel_registry_channel_id', 'idx_forum_topic_registry_status_updated_at'}
        for index in sorted(required_indexes - set(sqlite_indexes)):
            issues.append(f'sqlite:missing_index:{index}')
        for index in sorted(required_indexes - set(postgres_indexes)):
            issues.append(f'postgres:missing_index:{index}')

        type_expectations = {
            ('sqlite', 'bridge_destination_state', 'metadata_json'): 'TEXT',
            ('sqlite', 'bridge_comment_mirror', 'metadata_json'): 'TEXT',
            ('sqlite', 'bridge_destination_state', 'updated_at'): 'TEXT',
            ('sqlite', 'bridge_comment_mirror', 'updated_at'): 'TEXT',
            ('postgres', 'bridge_destination_state', 'metadata_json'): 'JSONB',
            ('postgres', 'bridge_comment_mirror', 'metadata_json'): 'JSONB',
            ('postgres', 'bridge_destination_state', 'updated_at'): 'TIMESTAMPTZ',
            ('postgres', 'bridge_comment_mirror', 'updated_at'): 'TIMESTAMPTZ',
            ('sqlite', 'external_discussion_mirror', 'metadata_json'): 'TEXT',
            ('sqlite', 'external_discussion_mirror', 'updated_at'): 'TEXT',
            ('sqlite', 'external_content_mirror', 'metadata_json'): 'TEXT',
            ('sqlite', 'external_content_mirror', 'updated_at'): 'TEXT',
            ('sqlite', 'legacy_layout_resources', 'metadata_json'): 'TEXT',
            ('sqlite', 'legacy_layout_resources', 'marked_at'): 'TEXT',
            ('postgres', 'external_discussion_mirror', 'metadata_json'): 'JSONB',
            ('postgres', 'external_discussion_mirror', 'updated_at'): 'TIMESTAMPTZ',
            ('postgres', 'external_content_mirror', 'metadata_json'): 'JSONB',
            ('postgres', 'external_content_mirror', 'updated_at'): 'TIMESTAMPTZ',
            ('postgres', 'legacy_layout_resources', 'metadata_json'): 'JSONB',
            ('postgres', 'legacy_layout_resources', 'marked_at'): 'TIMESTAMPTZ',
        }
        schema_map = {'sqlite': SQLITE_SCHEMA, 'postgres': POSTGRES_SCHEMA}
        for (source, table, column), expected_token in type_expectations.items():
            line = _column_line(schema_map[source], table, column)
            if not line or expected_token not in line.upper():
                issues.append(f'{source}:column_type:{table}.{column}:{expected_token.lower()}')
        default_expectations = {
            ('sqlite', 'bridge_destination_state', 'updated_at'): "datetime('now')",
            ('sqlite', 'bridge_comment_mirror', 'updated_at'): "datetime('now')",
            ('postgres', 'bridge_destination_state', 'updated_at'): 'NOW()',
            ('postgres', 'bridge_comment_mirror', 'updated_at'): 'NOW()',
            ('sqlite', 'external_discussion_mirror', 'updated_at'): "datetime('now')",
            ('sqlite', 'external_content_mirror', 'updated_at'): "datetime('now')",
            ('sqlite', 'legacy_layout_resources', 'marked_at'): "datetime('now')",
            ('postgres', 'external_discussion_mirror', 'updated_at'): 'NOW()',
            ('postgres', 'external_content_mirror', 'updated_at'): 'NOW()',
            ('postgres', 'legacy_layout_resources', 'marked_at'): 'NOW()',
        }
        for (source, table, column), token in default_expectations.items():
            line = _column_line(schema_map[source], table, column)
            if not line or token.lower() not in line.lower():
                issues.append(f'{source}:column_default:{table}.{column}:{token}')

        return sorted(dict.fromkeys(issues))
    async def list_failed_external_sync_events(self, *, limit: int = 25, destination: str | None = None, event_kind: str | None = None, since_hours: int | None = None) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            clauses = ["status!='sent'"]
            params: list[Any] = []
            if destination:
                clauses.append('destination=?')
                params.append(destination)
            if event_kind:
                clauses.append('event_kind=?')
                params.append(event_kind)
            if since_hours and since_hours > 0:
                clauses.append("updated_at >= datetime('now', '-' || ? || ' hours')")
                params.append(since_hours)
            sql = "SELECT * FROM external_sync_events WHERE " + " AND ".join(clauses) + " ORDER BY updated_at DESC LIMIT ?"
            params.append(limit)
            rows = await (await conn.execute(sql, tuple(params))).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                try:
                    data['payload_json']=json.loads(data.get('payload_json') or '{}')
                except Exception:
                    data['payload_json']={}
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        clauses = ["status!='sent'"]
        params: list[Any] = []
        if destination:
            params.append(destination)
            clauses.append(f"destination=${len(params)}")
        if event_kind:
            params.append(event_kind)
            clauses.append(f"event_kind=${len(params)}")
        if since_hours and since_hours > 0:
            params.append(since_hours)
            clauses.append(f"updated_at >= NOW() - (${len(params)}::int * INTERVAL '1 hour')")
        params.append(limit)
        sql = "SELECT * FROM external_sync_events WHERE " + " AND ".join(clauses) + f" ORDER BY updated_at DESC LIMIT ${len(params)}"
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(row) for row in rows]

    async def requeue_external_sync_event(self, event_id: int) -> bool:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            cur = await conn.execute("UPDATE external_sync_events SET status='pending', updated_at=datetime('now'), last_error='', next_retry_at=NULL, backoff_seconds=0, dead_letter_reason_code='' WHERE id=?", (event_id,))
            await conn.commit()
            return (cur.rowcount or 0) > 0
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            status = await conn.execute("UPDATE external_sync_events SET status='pending', updated_at=NOW(), last_error='', next_retry_at=NULL, backoff_seconds=0, dead_letter_reason_code='' WHERE id=$1", event_id)
        return status.endswith('1') if isinstance(status, str) else True

    async def list_forum_topics(self, *, statuses: tuple[str, ...] | None = None, limit: int = 50) -> list[dict[str, Any]]:
        statuses = statuses or ('open', 'in_review')
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            placeholders = ','.join('?' for _ in statuses)
            rows = await (await conn.execute(f"SELECT * FROM forum_topic_registry WHERE status IN ({placeholders}) ORDER BY updated_at ASC LIMIT ?", (*statuses, limit))).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                data['tags_json']=json.loads(data.get('tags_json') or '[]')
                data['metadata_json']=json.loads(data.get('metadata_json') or '{}')
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM forum_topic_registry WHERE status = ANY($1::text[]) ORDER BY updated_at ASC LIMIT $2", list(statuses), limit)
        return [dict(row) for row in rows]



    async def get_external_sync_event(self, event_id: int) -> dict[str, Any] | None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM external_sync_events WHERE id=? LIMIT 1", (event_id,))).fetchone()
            if row is None:
                return None
            data = dict(row)
            try:
                data['payload_json'] = json.loads(data.get('payload_json') or '{}')
            except Exception:
                data['payload_json'] = {}
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM external_sync_events WHERE id=$1 LIMIT 1", event_id)
        return dict(row) if row else None

    async def list_scheduled_jobs(self, *, statuses: tuple[str, ...] | None = None, limit: int = 25, since_hours: int | None = None, job_type: str | None = None) -> list[dict[str, Any]]:
        statuses = statuses or ('pending', 'retry')
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            placeholders = ','.join('?' for _ in statuses)
            clauses = [f"status IN ({placeholders})"]
            params: list[Any] = list(statuses)
            if job_type:
                clauses.append('job_type=?')
                params.append(job_type)
            if since_hours and since_hours > 0:
                clauses.append("updated_at >= datetime('now', '-' || ? || ' hours')")
                params.append(since_hours)
            sql = f"SELECT * FROM scheduled_jobs WHERE {' AND '.join(clauses)} ORDER BY COALESCE(next_retry_at, run_at) ASC, run_at ASC LIMIT ?"
            params.append(limit)
            rows = await (await conn.execute(sql, tuple(params))).fetchall()
            result=[]
            for row in rows:
                data = dict(row)
                try:
                    data['payload_json'] = json.loads(data.get('payload_json') or '{}')
                except Exception:
                    data['payload_json'] = {}
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        clauses = ["status = ANY($1::text[])"]
        params: list[Any] = [list(statuses)]
        if job_type:
            params.append(job_type)
            clauses.append(f"job_type=${len(params)}")
        if since_hours and since_hours > 0:
            params.append(since_hours)
            clauses.append(f"updated_at >= NOW() - (${len(params)}::int * INTERVAL '1 hour')")
        params.append(limit)
        sql = "SELECT * FROM scheduled_jobs WHERE " + " AND ".join(clauses) + f" ORDER BY COALESCE(next_retry_at, run_at) ASC, run_at ASC LIMIT ${len(params)}"
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)
        return [dict(row) for row in rows]

    async def get_scheduled_job(self, job_id: int) -> dict[str, Any] | None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM scheduled_jobs WHERE id=? LIMIT 1", (job_id,))).fetchone()
            if row is None:
                return None
            data = dict(row)
            try:
                data['payload_json'] = json.loads(data.get('payload_json') or '{}')
            except Exception:
                data['payload_json'] = {}
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM scheduled_jobs WHERE id=$1 LIMIT 1", job_id)
        return dict(row) if row else None

    async def cancel_scheduled_job(self, job_id: int) -> bool:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            cur = await conn.execute("UPDATE scheduled_jobs SET status='cancelled', updated_at=datetime('now') WHERE id=? AND status IN ('pending','retry')", (job_id,))
            await conn.commit()
            return (cur.rowcount or 0) > 0
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            status = await conn.execute("UPDATE scheduled_jobs SET status='cancelled', updated_at=NOW() WHERE id=$1 AND status IN ('pending','retry')", job_id)
        return status.endswith('1') if isinstance(status, str) else True

    async def reschedule_scheduled_job(self, job_id: int, *, run_at: str) -> bool:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            cur = await conn.execute("UPDATE scheduled_jobs SET run_at=?, status='pending', updated_at=datetime('now'), next_retry_at=NULL, backoff_seconds=0, dead_letter_reason_code='', last_error='' WHERE id=?", (run_at, job_id))
            await conn.commit()
            return (cur.rowcount or 0) > 0
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            status = await conn.execute("UPDATE scheduled_jobs SET run_at=$1::timestamptz, status='pending', updated_at=NOW(), next_retry_at=NULL, backoff_seconds=0, dead_letter_reason_code='', last_error='' WHERE id=$2", run_at, job_id)
        return status.endswith('1') if isinstance(status, str) else True

    async def list_unowned_forum_topics(self, *, limit: int = 20) -> list[dict[str, Any]]:
        rows = await self.list_forum_topics(statuses=('open','in_review'), limit=max(limit * 3, limit))
        result=[]
        for row in rows:
            metadata = row.get('metadata_json') or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except Exception:
                    metadata = {}
            if not metadata.get('staff_owner_user_id'):
                result.append(row)
            if len(result) >= limit:
                break
        return result

    async def find_duplicate_forum_topic(self, *, guild_id: str, topic_kind: str, owner_user_id: str, title: str, target_user_id: str | None = None, limit: int = 30) -> dict[str, Any] | None:
        rows = await self.list_forum_topics(statuses=('open','in_review'), limit=limit)
        normalized_title = _normalize_duplicate_text(title)
        for row in rows:
            if str(row.get('guild_id') or '') != guild_id:
                continue
            if str(row.get('topic_kind') or '') != topic_kind:
                continue
            metadata = row.get('metadata_json') or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except Exception:
                    metadata = {}
            if target_user_id and str(metadata.get('target_user_id') or '') == target_user_id:
                return row
            row_title = str(row.get('title') or '')
            same_owner = str(row.get('owner_user_id') or '') == owner_user_id
            if same_owner and _normalize_duplicate_text(row_title) == normalized_title:
                return row
            if same_owner and _duplicate_similarity(row_title, title) >= 0.7:
                return row
            if topic_kind in {'appeal', 'report', 'guild_recruitment'} and _duplicate_similarity(row_title, title) >= 0.85:
                return row
        return None



    async def get_bridge_destination_state(self, destination: str) -> dict[str, Any] | None:
        destination = str(destination or '').strip()
        if not destination:
            return None
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM bridge_destination_state WHERE destination=? LIMIT 1", (destination,))).fetchone()
            if row is None:
                return None
            data = dict(row)
            data['metadata_json'] = json.loads(data.get('metadata_json') or '{}')
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM bridge_destination_state WHERE destination=$1 LIMIT 1", destination)
        return dict(row) if row else None

    async def list_bridge_destination_states(self) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT * FROM bridge_destination_state ORDER BY destination")).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                data['metadata_json']=json.loads(data.get('metadata_json') or '{}')
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM bridge_destination_state ORDER BY destination")
        return [dict(row) for row in rows]

    async def update_bridge_destination_state(self, *, destination: str, circuit_state: str, consecutive_failures: int, last_error: str = '', circuit_open_until: str | None = None, success: bool = False, metadata: dict[str, Any] | None = None) -> None:
        destination = str(destination or '').strip()
        if not destination:
            return
        metadata = metadata or {}
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO bridge_destination_state(destination, circuit_state, consecutive_failures, circuit_open_until, last_success_at, last_failure_at, last_error, metadata_json, updated_at) VALUES(?, ?, ?, ?, CASE WHEN ? THEN datetime('now') ELSE NULL END, CASE WHEN ? THEN NULL ELSE datetime('now') END, ?, ?, datetime('now')) ON CONFLICT(destination) DO UPDATE SET circuit_state=excluded.circuit_state, consecutive_failures=excluded.consecutive_failures, circuit_open_until=excluded.circuit_open_until, last_success_at=CASE WHEN excluded.last_success_at IS NOT NULL THEN excluded.last_success_at ELSE bridge_destination_state.last_success_at END, last_failure_at=CASE WHEN excluded.last_failure_at IS NOT NULL THEN excluded.last_failure_at ELSE bridge_destination_state.last_failure_at END, last_error=excluded.last_error, metadata_json=excluded.metadata_json, updated_at=datetime('now')",
                (destination, circuit_state, int(consecutive_failures or 0), circuit_open_until, bool(success), bool(success), last_error[:2000], _json(metadata)),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO bridge_destination_state(destination, circuit_state, consecutive_failures, circuit_open_until, last_success_at, last_failure_at, last_error, metadata_json, updated_at) VALUES($1, $2, $3, $4::timestamptz, CASE WHEN $5 THEN NOW() ELSE NULL END, CASE WHEN $5 THEN NULL ELSE NOW() END, $6, $7::jsonb, NOW()) ON CONFLICT(destination) DO UPDATE SET circuit_state=EXCLUDED.circuit_state, consecutive_failures=EXCLUDED.consecutive_failures, circuit_open_until=EXCLUDED.circuit_open_until, last_success_at=COALESCE(EXCLUDED.last_success_at, bridge_destination_state.last_success_at), last_failure_at=COALESCE(EXCLUDED.last_failure_at, bridge_destination_state.last_failure_at), last_error=EXCLUDED.last_error, metadata_json=EXCLUDED.metadata_json, updated_at=NOW()",
                destination, circuit_state, int(consecutive_failures or 0), circuit_open_until, bool(success), last_error[:2000], _json(metadata),
            )

    async def get_external_sync_event_by_dedupe_key(self, dedupe_key: str) -> dict[str, Any] | None:
        key = str(dedupe_key or '').strip()
        if not key:
            return None
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM external_sync_events WHERE dedupe_key=? LIMIT 1", (key,))).fetchone()
            if row is None:
                return None
            data = dict(row)
            data['payload_json'] = _parse_json_value(data.get('payload_json'), {})
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM external_sync_events WHERE dedupe_key=$1 LIMIT 1", key)
        return dict(row) if row else None

    async def upsert_bridge_comment_mirror(self, *, thread_id: str, source_platform: str, external_comment_id: str, discord_message_id: str, metadata: dict[str, Any] | None = None) -> None:
        thread_id = str(thread_id or '').strip()
        source_platform = str(source_platform or 'external').strip().lower() or 'external'
        external_comment_id = str(external_comment_id or '').strip()
        discord_message_id = str(discord_message_id or '').strip()
        metadata = metadata or {}
        if not thread_id or not external_comment_id or not discord_message_id:
            return
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO bridge_comment_mirror(thread_id, source_platform, external_comment_id, discord_message_id, metadata_json, updated_at) VALUES(?, ?, ?, ?, ?, datetime('now')) ON CONFLICT(thread_id, source_platform, external_comment_id) DO UPDATE SET discord_message_id=excluded.discord_message_id, metadata_json=excluded.metadata_json, updated_at=datetime('now')",
                (thread_id, source_platform, external_comment_id, discord_message_id, _json(metadata)),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO bridge_comment_mirror(thread_id, source_platform, external_comment_id, discord_message_id, metadata_json, updated_at) VALUES($1,$2,$3,$4,$5::jsonb,NOW()) ON CONFLICT(thread_id, source_platform, external_comment_id) DO UPDATE SET discord_message_id=EXCLUDED.discord_message_id, metadata_json=EXCLUDED.metadata_json, updated_at=NOW()",
                thread_id, source_platform, external_comment_id, discord_message_id, _json(metadata),
            )

    async def get_bridge_comment_mirror_by_external(self, *, thread_id: str, external_comment_id: str, source_platform: str = 'external') -> dict[str, Any] | None:
        thread_id = str(thread_id or '').strip()
        external_comment_id = str(external_comment_id or '').strip()
        source_platform = str(source_platform or 'external').strip().lower() or 'external'
        if not thread_id or not external_comment_id:
            return None
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM bridge_comment_mirror WHERE thread_id=? AND source_platform=? AND external_comment_id=? LIMIT 1", (thread_id, source_platform, external_comment_id))).fetchone()
            if row is None:
                return None
            data = dict(row)
            data['metadata_json'] = _parse_json_value(data.get('metadata_json'), {})
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM bridge_comment_mirror WHERE thread_id=$1 AND source_platform=$2 AND external_comment_id=$3 LIMIT 1", thread_id, source_platform, external_comment_id)
        return dict(row) if row else None

    async def get_bridge_comment_mirror_by_message(self, *, discord_message_id: str) -> dict[str, Any] | None:
        message_id = str(discord_message_id or '').strip()
        if not message_id:
            return None
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM bridge_comment_mirror WHERE discord_message_id=? LIMIT 1", (message_id,))).fetchone()
            if row is None:
                return None
            data = dict(row)
            data['metadata_json'] = _parse_json_value(data.get('metadata_json'), {})
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM bridge_comment_mirror WHERE discord_message_id=$1 LIMIT 1", message_id)
        return dict(row) if row else None

    async def delete_bridge_comment_mirror(self, *, thread_id: str, external_comment_id: str, source_platform: str = 'external') -> bool:
        thread_id = str(thread_id or '').strip()
        external_comment_id = str(external_comment_id or '').strip()
        source_platform = str(source_platform or 'external').strip().lower() or 'external'
        if not thread_id or not external_comment_id:
            return False
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            cur = await conn.execute("DELETE FROM bridge_comment_mirror WHERE thread_id=? AND source_platform=? AND external_comment_id=?", (thread_id, source_platform, external_comment_id))
            await conn.commit()
            return (cur.rowcount or 0) > 0
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            status = await conn.execute("DELETE FROM bridge_comment_mirror WHERE thread_id=$1 AND source_platform=$2 AND external_comment_id=$3", thread_id, source_platform, external_comment_id)
        return status.endswith('1') if isinstance(status, str) else True


    async def upsert_external_discussion_mirror(self, *, source_platform: str, external_topic_id: str, topic_kind: str, discord_object_id: str, channel_id: str = '', metadata: dict[str, Any] | None = None) -> None:
        source_platform = str(source_platform or 'external').strip().lower() or 'external'
        external_topic_id = str(external_topic_id or '').strip()
        discord_object_id = str(discord_object_id or '').strip()
        if not external_topic_id or not discord_object_id:
            return
        metadata = metadata or {}
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO external_discussion_mirror(source_platform, external_topic_id, topic_kind, discord_object_id, channel_id, metadata_json, updated_at) VALUES(?, ?, ?, ?, ?, ?, datetime('now')) ON CONFLICT(source_platform, external_topic_id) DO UPDATE SET topic_kind=excluded.topic_kind, discord_object_id=excluded.discord_object_id, channel_id=excluded.channel_id, metadata_json=excluded.metadata_json, updated_at=datetime('now')",
                (source_platform, external_topic_id, str(topic_kind or ''), discord_object_id, str(channel_id or ''), _json(metadata)),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO external_discussion_mirror(source_platform, external_topic_id, topic_kind, discord_object_id, channel_id, metadata_json, updated_at) VALUES($1,$2,$3,$4,$5,$6::jsonb,NOW()) ON CONFLICT(source_platform, external_topic_id) DO UPDATE SET topic_kind=EXCLUDED.topic_kind, discord_object_id=EXCLUDED.discord_object_id, channel_id=EXCLUDED.channel_id, metadata_json=EXCLUDED.metadata_json, updated_at=NOW()",
                source_platform, external_topic_id, str(topic_kind or ''), discord_object_id, str(channel_id or ''), _json(metadata),
            )

    async def get_external_discussion_mirror(self, *, source_platform: str, external_topic_id: str) -> dict[str, Any] | None:
        source_platform = str(source_platform or 'external').strip().lower() or 'external'
        external_topic_id = str(external_topic_id or '').strip()
        if not external_topic_id:
            return None
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM external_discussion_mirror WHERE source_platform=? AND external_topic_id=? LIMIT 1", (source_platform, external_topic_id))).fetchone()
            if row is None:
                return None
            data = dict(row)
            data['metadata_json'] = _parse_json_value(data.get('metadata_json'), {})
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM external_discussion_mirror WHERE source_platform=$1 AND external_topic_id=$2 LIMIT 1", source_platform, external_topic_id)
        if row is None:
            return None
        data = dict(row)
        data['metadata_json'] = _parse_json_value(data.get('metadata_json'), {})
        return data

    async def upsert_external_content_mirror(self, *, source_platform: str, content_kind: str, external_content_id: str, discord_channel_id: str, discord_message_id: str, metadata: dict[str, Any] | None = None) -> None:
        source_platform = str(source_platform or 'external').strip().lower() or 'external'
        content_kind = str(content_kind or '').strip().lower()
        external_content_id = str(external_content_id or '').strip()
        discord_message_id = str(discord_message_id or '').strip()
        if not content_kind or not external_content_id or not discord_message_id:
            return
        metadata = metadata or {}
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO external_content_mirror(source_platform, content_kind, external_content_id, discord_channel_id, discord_message_id, metadata_json, updated_at) VALUES(?, ?, ?, ?, ?, ?, datetime('now')) ON CONFLICT(source_platform, content_kind, external_content_id) DO UPDATE SET discord_channel_id=excluded.discord_channel_id, discord_message_id=excluded.discord_message_id, metadata_json=excluded.metadata_json, updated_at=datetime('now')",
                (source_platform, content_kind, external_content_id, str(discord_channel_id or ''), discord_message_id, _json(metadata)),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO external_content_mirror(source_platform, content_kind, external_content_id, discord_channel_id, discord_message_id, metadata_json, updated_at) VALUES($1,$2,$3,$4,$5,$6::jsonb,NOW()) ON CONFLICT(source_platform, content_kind, external_content_id) DO UPDATE SET discord_channel_id=EXCLUDED.discord_channel_id, discord_message_id=EXCLUDED.discord_message_id, metadata_json=EXCLUDED.metadata_json, updated_at=NOW()",
                source_platform, content_kind, external_content_id, str(discord_channel_id or ''), discord_message_id, _json(metadata),
            )

    async def get_external_content_mirror(self, *, source_platform: str, content_kind: str, external_content_id: str) -> dict[str, Any] | None:
        source_platform = str(source_platform or 'external').strip().lower() or 'external'
        content_kind = str(content_kind or '').strip().lower()
        external_content_id = str(external_content_id or '').strip()
        if not content_kind or not external_content_id:
            return None
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT * FROM external_content_mirror WHERE source_platform=? AND content_kind=? AND external_content_id=? LIMIT 1", (source_platform, content_kind, external_content_id))).fetchone()
            if row is None:
                return None
            data = dict(row)
            data['metadata_json'] = _parse_json_value(data.get('metadata_json'), {})
            return data
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM external_content_mirror WHERE source_platform=$1 AND content_kind=$2 AND external_content_id=$3 LIMIT 1", source_platform, content_kind, external_content_id)
        if row is None:
            return None
        data = dict(row)
        data['metadata_json'] = _parse_json_value(data.get('metadata_json'), {})
        return data

    async def delete_external_content_mirror(self, *, source_platform: str, content_kind: str, external_content_id: str) -> bool:
        source_platform = str(source_platform or 'external').strip().lower() or 'external'
        content_kind = str(content_kind or '').strip().lower()
        external_content_id = str(external_content_id or '').strip()
        if not content_kind or not external_content_id:
            return False
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            cur = await conn.execute("DELETE FROM external_content_mirror WHERE source_platform=? AND content_kind=? AND external_content_id=?", (source_platform, content_kind, external_content_id))
            await conn.commit()
            return (cur.rowcount or 0) > 0
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            status = await conn.execute("DELETE FROM external_content_mirror WHERE source_platform=$1 AND content_kind=$2 AND external_content_id=$3", source_platform, content_kind, external_content_id)
        return status.endswith('1') if isinstance(status, str) else True

    async def list_bridge_comment_mirrors(self, *, limit: int = 250) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT * FROM bridge_comment_mirror ORDER BY updated_at DESC LIMIT ?", (limit,))).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                data['metadata_json']=_parse_json_value(data.get('metadata_json'), {})
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM bridge_comment_mirror ORDER BY updated_at DESC LIMIT $1", limit)
        result=[]
        for row in rows:
            data=dict(row)
            data['metadata_json']=_parse_json_value(data.get('metadata_json'), {})
            result.append(data)
        return result

    async def list_external_discussion_mirrors(self, *, limit: int = 250) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT * FROM external_discussion_mirror ORDER BY updated_at DESC LIMIT ?", (limit,))).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                data['metadata_json']=_parse_json_value(data.get('metadata_json'), {})
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM external_discussion_mirror ORDER BY updated_at DESC LIMIT $1", limit)
        result=[]
        for row in rows:
            data=dict(row)
            data['metadata_json']=_parse_json_value(data.get('metadata_json'), {})
            result.append(data)
        return result

    async def list_external_content_mirrors(self, *, limit: int = 250) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT * FROM external_content_mirror ORDER BY updated_at DESC LIMIT ?", (limit,))).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                data['metadata_json']=_parse_json_value(data.get('metadata_json'), {})
                result.append(data)
            return result
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM external_content_mirror ORDER BY updated_at DESC LIMIT $1", limit)
        result=[]
        for row in rows:
            data=dict(row)
            data['metadata_json']=_parse_json_value(data.get('metadata_json'), {})
            result.append(data)
        return result

    async def upsert_legacy_layout_resource(self, *, guild_id: str, resource_type: str, discord_id: str, resource_name: str, status: str = 'legacy', review_after: str | None = None, delete_after: str | None = None, metadata: dict[str, Any] | None = None) -> None:
        guild_id = str(guild_id or '').strip()
        resource_type = str(resource_type or '').strip().lower()
        discord_id = str(discord_id or '').strip()
        if not guild_id or not resource_type or not discord_id:
            return
        metadata = metadata or {}
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute(
                "INSERT INTO legacy_layout_resources(guild_id, resource_type, discord_id, resource_name, status, marked_at, review_after, delete_after, metadata_json) VALUES(?, ?, ?, ?, ?, datetime('now'), ?, ?, ?) ON CONFLICT(guild_id, resource_type, discord_id) DO UPDATE SET resource_name=excluded.resource_name, status=excluded.status, review_after=excluded.review_after, delete_after=excluded.delete_after, metadata_json=excluded.metadata_json",
                (guild_id, resource_type, discord_id, str(resource_name or ''), str(status or 'legacy'), review_after, delete_after, _json(metadata)),
            )
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO legacy_layout_resources(guild_id, resource_type, discord_id, resource_name, status, marked_at, review_after, delete_after, metadata_json) VALUES($1,$2,$3,$4,$5,NOW(),$6::timestamptz,$7::timestamptz,$8::jsonb) ON CONFLICT(guild_id, resource_type, discord_id) DO UPDATE SET resource_name=EXCLUDED.resource_name, status=EXCLUDED.status, review_after=EXCLUDED.review_after, delete_after=EXCLUDED.delete_after, metadata_json=EXCLUDED.metadata_json",
                guild_id, resource_type, discord_id, str(resource_name or ''), str(status or 'legacy'), review_after, delete_after, _json(metadata),
            )

    async def list_legacy_layout_resources(self, *, guild_id: str | None = None, due_only: bool = False, limit: int = 50) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if guild_id:
            clauses.append('guild_id=?')
            params.append(str(guild_id))
        if due_only:
            clauses.append("status='legacy' AND review_after IS NOT NULL AND review_after <= datetime('now')")
        sql_where = (' WHERE ' + ' AND '.join(clauses)) if clauses else ''
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT * FROM legacy_layout_resources" + sql_where + " ORDER BY COALESCE(delete_after, review_after, marked_at) ASC LIMIT ?", (*params, limit))).fetchall()
            result=[]
            for row in rows:
                data=dict(row)
                data['metadata_json']=_parse_json_value(data.get('metadata_json'), {})
                result.append(data)
            return result
        clauses_pg: list[str] = []
        params_pg: list[Any] = []
        if guild_id:
            clauses_pg.append(f"guild_id=${len(params_pg)+1}")
            params_pg.append(str(guild_id))
        if due_only:
            clauses_pg.append("status='legacy' AND review_after IS NOT NULL AND review_after <= NOW()")
        where_pg = (' WHERE ' + ' AND '.join(clauses_pg)) if clauses_pg else ''
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM legacy_layout_resources" + where_pg + f" ORDER BY COALESCE(delete_after, review_after, marked_at) ASC LIMIT ${len(params_pg)+1}", *params_pg, limit)
        result=[]
        for row in rows:
            data=dict(row)
            data['metadata_json']=_parse_json_value(data.get('metadata_json'), {})
            result.append(data)
        return result

    async def update_legacy_layout_resource_status(self, *, guild_id: str, resource_type: str, discord_id: str, status: str) -> None:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute("UPDATE legacy_layout_resources SET status=? WHERE guild_id=? AND resource_type=? AND discord_id=?", (status, guild_id, resource_type, discord_id))
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute("UPDATE legacy_layout_resources SET status=$1 WHERE guild_id=$2 AND resource_type=$3 AND discord_id=$4", status, guild_id, resource_type, discord_id)

    async def update_rules_acceptance_metadata(self, *, guild_id: str, discord_user_id: str, metadata: dict[str, Any]) -> None:
        payload = metadata or {}
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            await conn.execute("UPDATE rules_acceptance SET metadata_json=?, accepted_at=accepted_at WHERE guild_id=? AND discord_user_id=?", (_json(payload), guild_id, discord_user_id))
            await conn.commit()
            return
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            await conn.execute("UPDATE rules_acceptance SET metadata_json=$1::jsonb WHERE guild_id=$2 AND discord_user_id=$3", _json(payload), guild_id, discord_user_id)

    async def schema_migration_plan(self) -> dict[str, Any]:
        applied_rows = await self.list_community_schema_migrations(limit=200)
        applied_versions = sorted({int(row.get('version') or 0) for row in applied_rows if int(row.get('version') or 0) > 0})
        expected_versions = [int(version) for version, _name in COMMUNITY_SCHEMA_MIGRATIONS]
        expected_map = {int(version): str(name) for version, name in COMMUNITY_SCHEMA_MIGRATIONS}
        pending_versions = [version for version in expected_versions if version not in applied_versions]
        descriptors = []
        for version in expected_versions:
            descriptors.append({
                'version': int(version),
                'name': expected_map.get(version, ''),
                'status': 'applied' if version in applied_versions else 'pending',
            })
        return {
            'current': max(applied_versions) if applied_versions else 0,
            'expected': max(expected_versions) if expected_versions else 0,
            'applied_versions': applied_versions,
            'pending_versions': pending_versions,
            'pending_names': [expected_map.get(version, '') for version in pending_versions],
            'descriptors': descriptors,
            'schema_version_matches_expected': (max(applied_versions) if applied_versions else 0) == (max(expected_versions) if expected_versions else 0),
        }

    async def get_schema_version(self) -> int:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            row = await (await conn.execute("SELECT value FROM schema_meta WHERE key='nmdiscordbot_schema_version' LIMIT 1")).fetchone()
            return int(str(row[0])) if row and str(row[0]).isdigit() else 0
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            value = await conn.fetchval("SELECT value FROM schema_meta WHERE key='nmdiscordbot_schema_version' LIMIT 1")
        return int(str(value)) if value is not None and str(value).isdigit() else 0

    async def list_schema_meta(self) -> dict[str, str]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT key, value FROM schema_meta ORDER BY key")).fetchall()
            return {str(row['key']): str(row['value']) for row in rows}
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM schema_meta ORDER BY key")
        return {str(row['key']): str(row['value']) for row in rows}

    async def list_schema_meta_ledger(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT key, value, source, changed_at FROM schema_meta_ledger ORDER BY id DESC LIMIT ?", (limit,))).fetchall()
            return [dict(row) for row in rows]
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value, source, changed_at FROM schema_meta_ledger ORDER BY id DESC LIMIT $1", limit)
        return [dict(row) for row in rows]


    async def list_community_schema_migrations(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if isinstance(self.storage.database, SQLiteBackend):
            conn = self.storage.database.conn
            assert conn is not None
            rows = await (await conn.execute("SELECT version, name, applied_at, source FROM community_schema_migrations ORDER BY version DESC LIMIT ?", (limit,))).fetchall()
            return [dict(row) for row in rows]
        pool = self.storage.database.pool
        assert pool is not None
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT version, name, applied_at, source FROM community_schema_migrations ORDER BY version DESC LIMIT $1", limit)
        return [dict(row) for row in rows]