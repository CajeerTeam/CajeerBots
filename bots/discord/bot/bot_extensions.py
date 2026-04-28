from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable

BOT_EXTENSION_METHOD_GROUPS: dict[str, tuple[str, ...]] = {
    "content_state": (
        "load_content_snapshot", "get_runtime_marker", "set_runtime_marker",
        "refresh_layout_alias_cache", "remember_layout_alias_binding",
        "export_operational_state", "capture_operational_backup", "get_maintenance_mode",
    ),
    "forums_and_staff": (
        "build_thread_transcript", "notify_topic_owner_change", "apply_thread_status",
        "_normalized_member_role_names", "has_staff_scope", "is_staff",
    ),
    "lifecycle": (
        "setup_hook", "on_ready", "close", "sync_verified_role", "record_audit",
        "_validate_startup_resources", "_cleanup_loop",
    ),
    "forum_helpers": (
        "_ensure_forum_tags", "_forum_tag_names_for_kind", "_interest_ping_mentions",
        "_subscription_event_mentions", "_sync_topic_presentation",
    ),
    "bridge_and_transport": (
        "_relay_loop", "_run_relay_iteration", "_relay_status_change", "_relay_announcements",
        "_relay_events", "_bridge_semantic_kind", "_bridge_destinations",
        "_bridge_destinations_for_event", "queue_bridge_event", "queue_bridge_admin_action",
        "_external_sync_loop", "_deliver_external_sync_row", "handle_incoming_transport_event",
        "handle_incoming_admin_event", "_filter_bridge_payload", "_bridge_destination_label",
        "_bridge_policy_allows",
    ),
    "layout_panels_metrics_scheduler": (
        "build_metrics_text", "_runtime_drift_loop", "_runtime_drift_cycle",
        "_get_forum_channel", "_get_stage_channel", "_interest_role_ids",
        "publish_panel", "_resolve_thread", "_reconcile_panels", "_send_stage_announcement",
        "_send_event_reminder", "send_staff_digest", "_scheduler_loop", "_send_staff_notice",
        "_approval_expiry_loop", "_run_rules_reacceptance_cycle", "_rules_reacceptance_loop",
        "_run_escalation_cycle", "_send_topic_escalation", "_get_audit_channel_ids",
        "_get_message_channel", "_top_level_slash_command_count", "_grouped_alias_root_count",
        "_remove_legacy_flat_staff_commands", "_apply_command_surface_mode",
    ),
}


def flatten_bot_extension_methods() -> tuple[str, ...]:
    seen: dict[str, None] = {}
    for names in BOT_EXTENSION_METHOD_GROUPS.values():
        for name in names:
            seen.setdefault(name, None)
    return tuple(seen.keys())


def bind_bot_extensions(bot_cls: type[Any], namespace: Mapping[str, Any]) -> tuple[str, ...]:
    bound: list[str] = []
    for method_name in flatten_bot_extension_methods():
        candidate = namespace.get(method_name)
        if callable(candidate):
            setattr(bot_cls, method_name, candidate)
            bound.append(method_name)
    return tuple(bound)
