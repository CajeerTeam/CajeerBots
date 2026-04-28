from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


_COMMAND_ALIAS_MAP: dict[str, str] = {
    'topic status': 'topic status',
    'topic export': 'topic export',
    'topic owner': 'topic owner',
    'bridge retry': 'bridge retry',
    'bridge history': 'bridge history',
    'ops status': 'ops status',
    'ops health': 'ops health',
    'ops ping': 'ops ping',
    'content reload': 'content reload',
    'forum policy': 'forum policy',
    'state export': 'state export',
    'state restore': 'state restore',
    'approval queue': 'approval queue',
    'approval process': 'approval process',
    'layout repair': 'layout repair',
    'layout drift': 'layout drift',
    'identity status': 'identity status',
    'identity unlink': 'identity unlink',
    'digest send': 'digest send',
    'digest schedule': 'digest schedule',
}



LEGACY_FLAT_STAFF_COMMANDS: tuple[str, ...] = (
    'announce', 'announcement_update', 'announcement_delete', 'devlog_publish', 'devlog_update', 'devlog_delete',
    'topic_update', 'approval_recent', 'approval_decide', 'panel_publish', 'stage_announce', 'event_reminder',
    'audit_search', 'audit_export', 'audit_action_export', 'ops_status', 'runtime_policy_snapshot',
    'bridge_policy', 'bridge_status', 'bridge_event_status', 'bridge_event_retry', 'bridge_retry_quick',
    'bridge_dead_letters', 'bridge_dead_letter_requeue', 'topic_status', 'topic_claim', 'topic_triage',
    'topic_export', 'topics_overdue', 'content_reload', 'panel_preview', 'panel_restore', 'scheduled_jobs',
    'scheduled_job_cancel', 'scheduled_job_reschedule', 'scheduled_job_run', 'layout_repair', 'layout_legacy_review',
    'layout_legacy_cleanup', 'cleanup_status', 'history_snapshot', 'staff_scope_map', 'boot_diagnostics',
    'staff_digest_now', 'staff_digest_schedule', 'staff_digest_calendar', 'maintenance_mode', 'forum_policy_view',
    'forum_policy_set', 'cleanup_preview', 'ingress_keys', 'state_export', 'state_restore',
    'job_dead_letters', 'job_dead_letter_retry', 'discord_scheduled_event', 'world_signal_publish',
    'rules_reacceptance_status', 'rules_reacceptance_nudge', 'targeted_digest_now', 'targeted_digest_schedule',
    'targeted_digest_calendar', 'capability_report', 'command_surface_report',
)

def command_alias_map() -> dict[str, str]:
    return dict(_COMMAND_ALIAS_MAP)


def parse_governance_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    for candidate in (raw, raw.replace('Z', '+00:00')):
        try:
            dt = datetime.fromisoformat(candidate)
        except ValueError:
            dt = None
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    try:
        return datetime.strptime(raw, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def rules_reacceptance_state(
    row: dict[str, Any],
    *,
    current_rules_version: str,
    grace_hours: int,
    reminder_hours: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    metadata = dict(row.get('metadata_json') or {}) if isinstance(row, dict) else {}
    accepted_at = parse_governance_datetime(str(row.get('accepted_at') or ''))
    last_nudged_at = parse_governance_datetime(str(metadata.get('rules_reacceptance_nudged_at') or ''))
    enforced_at = parse_governance_datetime(str(metadata.get('rules_reacceptance_enforced_at') or ''))
    outdated = str(row.get('accepted_rules_version') or '').strip() != str(current_rules_version or '').strip()
    grace_deadline = accepted_at + timedelta(hours=max(1, int(grace_hours or 1))) if accepted_at else None
    reminder_deadline = last_nudged_at + timedelta(hours=max(1, int(reminder_hours or 1))) if last_nudged_at else None
    return {
        'outdated': outdated,
        'metadata': metadata,
        'accepted_at': accepted_at,
        'last_nudged_at': last_nudged_at,
        'enforced_at': enforced_at,
        'grace_deadline': grace_deadline,
        'reminder_due': bool(outdated and (last_nudged_at is None or (reminder_deadline is not None and reminder_deadline <= now))),
        'enforcement_due': bool(outdated and grace_deadline is not None and grace_deadline <= now and enforced_at is None),
    }
