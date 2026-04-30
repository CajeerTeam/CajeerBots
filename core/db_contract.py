from __future__ import annotations

REQUIRED_TABLES: dict[str, set[str]] = {
    "platform_schema": {"component", "version", "updated_at"},
    "event_bus": {"event_id", "trace_id", "source", "event_type", "payload", "status", "created_at", "locked_at", "locked_by", "delivered_at", "attempts", "next_attempt_at", "last_error"},
    "delivery_queue": {"delivery_id", "adapter", "target", "payload", "status", "attempts", "max_attempts", "trace_id", "created_at", "locked_at", "locked_by", "next_attempt_at", "sent_at", "failed_at", "last_error", "rate_limit_bucket"},
    "dead_letters": {"dead_letter_id", "event_id", "trace_id", "payload", "reason", "created_at", "retried_at"},
    "idempotency_keys": {"key", "created_at", "expires_at"},
    "audit_log": {"audit_id", "actor_type", "actor_id", "action", "resource", "result", "trace_id", "ip", "user_agent", "message", "created_at"},
    "adapter_state": {"adapter", "instance_id", "state", "last_error", "updated_at"},
    "users": {"user_id", "display_name", "workspace_user_id", "created_at", "updated_at"},
    "platform_accounts": {"platform", "platform_user_id", "user_id", "username", "display_name", "profile", "created_at", "updated_at"},
    "roles": {"role_id", "title", "source", "created_at"},
    "role_permissions": {"role_id", "permission"},
    "user_roles": {"user_id", "role_id", "granted_at"},
    "support_tickets": {"ticket_id", "user_id", "platform", "platform_chat_id", "status", "subject", "assigned_to", "history", "created_at", "updated_at"},
    "moderation_actions": {"action_id", "platform", "target_id", "action", "reason", "actor_id", "trace_id", "created_at"},
    "announcements": {"announcement_id", "status", "title", "body", "targets", "scheduled_at", "created_at"},
    "user_profiles": {"user_id", "profile", "updated_at"},
    "workspace_links": {"link_id", "user_id", "workspace_user_id", "source", "created_at"},
    "scheduled_jobs": {"job_id", "job_type", "payload", "status", "run_at", "locked_at", "locked_by", "attempts", "max_attempts", "last_error", "completed_at", "failed_at", "created_at", "updated_at"},
    "outbound_messages": {"message_id", "delivery_id", "adapter", "target", "text_hash", "status", "platform_message_id", "attempts", "trace_id", "last_error", "sent_at", "created_at"},
}
