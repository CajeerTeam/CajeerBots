"""Базовый контракт Cajeer Bots.

Revision ID: 0001_core
Revises:
Create Date: 2026-04-29
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_core"
down_revision = None
branch_labels = None
depends_on = None

DB_CONTRACT_VERSION = "cajeer.bots.db.v1"


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS shared")
    op.create_table(
        "platform_schema",
        sa.Column("component", sa.String(128), primary_key=True),
        sa.Column("version", sa.String(128), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        schema="shared",
    )
    op.execute(
        "INSERT INTO shared.platform_schema(component, version) "
        f"VALUES ('cajeer-bots-db', '{DB_CONTRACT_VERSION}') "
        "ON CONFLICT (component) DO UPDATE SET version = EXCLUDED.version, updated_at = NOW()"
    )
    op.create_table(
        "event_bus",
        sa.Column("event_id", sa.String(64), primary_key=True),
        sa.Column("trace_id", sa.String(64), nullable=False, index=True),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("event_type", sa.String(128), nullable=False, index=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="new", index=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_by", sa.String(128), nullable=True, index=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True, index=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        schema="shared",
    )
    op.create_table(
        "delivery_queue",
        sa.Column("delivery_id", sa.String(64), primary_key=True),
        sa.Column("adapter", sa.String(64), nullable=False, index=True),
        sa.Column("target", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending", index=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("trace_id", sa.String(64), nullable=True, index=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_by", sa.String(128), nullable=True, index=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True, index=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("rate_limit_bucket", sa.String(128), nullable=True, index=True),
        schema="shared",
    )
    op.create_table(
        "dead_letters",
        sa.Column("dead_letter_id", sa.String(64), primary_key=True),
        sa.Column("event_id", sa.String(64), nullable=False, index=True),
        sa.Column("trace_id", sa.String(64), nullable=True, index=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("retried_at", sa.DateTime(timezone=True), nullable=True),
        schema="shared",
    )
    op.create_table(
        "idempotency_keys",
        sa.Column("key", sa.String(128), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        schema="shared",
    )
    op.create_table(
        "audit_log",
        sa.Column("audit_id", sa.String(64), primary_key=True),
        sa.Column("actor_type", sa.String(64), nullable=False),
        sa.Column("actor_id", sa.String(255), nullable=False),
        sa.Column("action", sa.String(128), nullable=False, index=True),
        sa.Column("resource", sa.String(255), nullable=False),
        sa.Column("result", sa.String(32), nullable=False, index=True),
        sa.Column("trace_id", sa.String(64), nullable=True, index=True),
        sa.Column("ip", sa.String(64), nullable=True),
        sa.Column("user_agent", sa.String(512), nullable=True),
        sa.Column("message", sa.Text(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        schema="shared",
    )

    op.create_table(
        "users",
        sa.Column("user_id", sa.String(64), primary_key=True),
        sa.Column("display_name", sa.String(255), nullable=True),
        sa.Column("workspace_user_id", sa.String(128), nullable=True, index=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        schema="shared",
    )
    op.create_table(
        "platform_accounts",
        sa.Column("platform", sa.String(64), primary_key=True),
        sa.Column("platform_user_id", sa.String(128), primary_key=True),
        sa.Column("user_id", sa.String(64), nullable=False, index=True),
        sa.Column("username", sa.String(255), nullable=True),
        sa.Column("display_name", sa.String(255), nullable=True),
        sa.Column("profile", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        schema="shared",
    )
    op.create_table("roles", sa.Column("role_id", sa.String(64), primary_key=True), sa.Column("title", sa.String(255), nullable=False), sa.Column("source", sa.String(64), nullable=False, server_default="local"), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")), schema="shared")
    op.create_table("role_permissions", sa.Column("role_id", sa.String(64), primary_key=True), sa.Column("permission", sa.String(128), primary_key=True), schema="shared")
    op.create_table("user_roles", sa.Column("user_id", sa.String(64), primary_key=True), sa.Column("role_id", sa.String(64), primary_key=True), sa.Column("granted_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")), schema="shared")
    op.create_table("support_tickets", sa.Column("ticket_id", sa.String(64), primary_key=True), sa.Column("user_id", sa.String(64), nullable=True, index=True), sa.Column("platform", sa.String(64), nullable=False), sa.Column("platform_chat_id", sa.String(128), nullable=False, index=True), sa.Column("status", sa.String(32), nullable=False, server_default="open", index=True), sa.Column("subject", sa.String(255), nullable=False, server_default=""), sa.Column("assigned_to", sa.String(128), nullable=True, index=True), sa.Column("history", postgresql.JSONB(astext_type=sa.Text()), nullable=False), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")), sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")), schema="shared")
    op.create_table("moderation_actions", sa.Column("action_id", sa.String(64), primary_key=True), sa.Column("platform", sa.String(64), nullable=False, index=True), sa.Column("target_id", sa.String(128), nullable=False, index=True), sa.Column("action", sa.String(64), nullable=False), sa.Column("reason", sa.Text(), nullable=False, server_default=""), sa.Column("actor_id", sa.String(128), nullable=True), sa.Column("trace_id", sa.String(64), nullable=True, index=True), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")), schema="shared")
    op.create_table("announcements", sa.Column("announcement_id", sa.String(64), primary_key=True), sa.Column("status", sa.String(32), nullable=False, server_default="draft", index=True), sa.Column("title", sa.String(255), nullable=False, server_default=""), sa.Column("body", sa.Text(), nullable=False, server_default=""), sa.Column("targets", postgresql.JSONB(astext_type=sa.Text()), nullable=False), sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True, index=True), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")), schema="shared")
    op.create_table("user_profiles", sa.Column("user_id", sa.String(64), primary_key=True), sa.Column("profile", postgresql.JSONB(astext_type=sa.Text()), nullable=False), sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")), schema="shared")
    op.create_table("workspace_links", sa.Column("link_id", sa.String(64), primary_key=True), sa.Column("user_id", sa.String(64), nullable=False, index=True), sa.Column("workspace_user_id", sa.String(128), nullable=False, index=True), sa.Column("source", sa.String(64), nullable=False, server_default="workspace"), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")), schema="shared")
    op.create_table("scheduled_jobs", sa.Column("job_id", sa.String(64), primary_key=True), sa.Column("job_type", sa.String(128), nullable=False, index=True), sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False), sa.Column("status", sa.String(32), nullable=False, server_default="pending", index=True), sa.Column("run_at", sa.DateTime(timezone=True), nullable=False, index=True), sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True), sa.Column("locked_by", sa.String(128), nullable=True, index=True), sa.Column("last_error", sa.Text(), nullable=True), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")), schema="shared")
    op.create_table("outbound_messages", sa.Column("message_id", sa.String(64), primary_key=True), sa.Column("delivery_id", sa.String(64), nullable=False, index=True), sa.Column("adapter", sa.String(64), nullable=False, index=True), sa.Column("target", sa.String(255), nullable=False, index=True), sa.Column("text_hash", sa.String(128), nullable=False), sa.Column("status", sa.String(32), nullable=False, index=True), sa.Column("platform_message_id", sa.String(128), nullable=True), sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"), sa.Column("last_error", sa.Text(), nullable=True), sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True), sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")), schema="shared")

    op.create_table(
        "adapter_state",
        sa.Column("adapter", sa.String(64), primary_key=True),
        sa.Column("instance_id", sa.String(128), primary_key=True),
        sa.Column("state", sa.String(32), nullable=False, index=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        schema="shared",
    )


def downgrade() -> None:
    for table in ["outbound_messages", "scheduled_jobs", "workspace_links", "user_profiles", "announcements", "moderation_actions", "support_tickets", "user_roles", "role_permissions", "roles", "platform_accounts", "users", "adapter_state", "audit_log", "idempotency_keys", "dead_letters", "delivery_queue", "event_bus", "platform_schema"]:
        op.drop_table(table, schema="shared")
