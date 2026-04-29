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


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS shared")
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
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        schema="shared",
    )
    op.create_table(
        "delivery_queue",
        sa.Column("delivery_id", sa.String(64), primary_key=True),
        sa.Column("adapter", sa.String(64), nullable=False),
        sa.Column("target", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        schema="shared",
    )
    op.create_table(
        "dead_letters",
        sa.Column("dead_letter_id", sa.String(64), primary_key=True),
        sa.Column("event_id", sa.String(64), nullable=False, index=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
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
        sa.Column("action", sa.String(128), nullable=False),
        sa.Column("resource", sa.String(255), nullable=False),
        sa.Column("result", sa.String(32), nullable=False),
        sa.Column("trace_id", sa.String(64), nullable=True),
        sa.Column("ip", sa.String(64), nullable=True),
        sa.Column("user_agent", sa.String(512), nullable=True),
        sa.Column("message", sa.Text(), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        schema="shared",
    )


def downgrade() -> None:
    for table in ["audit_log", "idempotency_keys", "dead_letters", "delivery_queue", "event_bus"]:
        op.drop_table(table, schema="shared")
