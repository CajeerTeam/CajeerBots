from __future__ import annotations

from typing import Any

from .bot_governance import command_alias_map

INTEREST_EVENT_ROUTING: dict[str, list[str]] = {
    'community.world_signal.created': ['lore'],
    'community.event.reminder': ['events'],
    'community.stage.announcement': ['events'],
    'community.guild_recruitment.created': ['guilds'],
    'community.guild_recruitment.bumped': ['guilds'],
    'community.chronicle.created': ['lore'],
    'community.chronicle.status_changed': ['lore'],
    'community.chronicle.comment.appended': ['lore'],
    'community.lore_discussion.created': ['lore'],
    'community.lore_discussion.comment.appended': ['lore'],
    'community.announcement.created': ['news'],
    'community.announcement.updated': ['news'],
    'community.announcement.deleted': ['news'],
    'community.devlog.created': ['devlogs'],
    'community.devlog.updated': ['devlogs'],
    'community.devlog.deleted': ['devlogs'],
    'community.support.created': ['news'],
    'community.bug_report.created': ['gameplay'],
    'community.suggestion.created': ['gameplay'],
    'community.appeal.created': ['news'],
    'community.appeal.status_changed': ['news'],
    'community.appeal.comment.appended': ['news'],
    'community.report.created': ['news'],
    'community.report.status_changed': ['news'],
    'community.report.comment.appended': ['news'],
    'community.report.comment.edited': ['news'],
    'community.report.comment.deleted': ['news'],
}


def routed_interest_aliases(event_kind: str) -> list[str]:
    event_key = str(event_kind or '').strip().lower()
    if not event_key:
        return []
    if event_key in INTEREST_EVENT_ROUTING:
        return list(INTEREST_EVENT_ROUTING[event_key])
    prefix_map = {
        'community.world_signal.': ['lore'],
        'community.event.': ['events'],
        'community.stage.': ['events'],
        'community.guild_recruitment.': ['guilds'],
        'community.chronicle.': ['lore'],
        'community.lore_discussion.': ['lore'],
        'community.announcement.': ['news'],
        'community.devlog.': ['devlogs'],
        'community.support.': ['news'],
        'community.bug_report.': ['gameplay'],
        'community.suggestion.': ['gameplay'],
        'community.appeal.': ['news'],
        'community.report.': ['news'],
    }
    for prefix, aliases in prefix_map.items():
        if event_key.startswith(prefix):
            return list(aliases)
    return []


def digest_job_payload(*, digest_kind: str, recurrence_hours: int | None = None, remaining_occurrences: int | None = None) -> dict[str, Any]:
    payload = {'kind': 'targeted_digest', 'digest_kind': str(digest_kind or 'staff').strip().lower() or 'staff'}
    if int(recurrence_hours or 0) > 0:
        payload['recurrence_hours'] = int(recurrence_hours or 0)
    if remaining_occurrences is not None and int(remaining_occurrences or 0) > 0:
        payload['remaining_occurrences'] = int(remaining_occurrences or 0)
    return payload


def command_surface_policy(*, grouped_count: int, flat_count: int, mode: str = 'compat', removed_flat_aliases: list[str] | None = None, missing_permission_gates: list[str] | None = None) -> dict[str, Any]:
    alias_map = command_alias_map()
    normalized_mode = str(mode or 'compat').strip().lower() or 'compat'
    legacy = max(0, int(flat_count or 0) - int(grouped_count or 0))
    return {
        'official_surface': 'grouped',
        'command_surface_mode': normalized_mode,
        'grouped_command_count': int(grouped_count or 0),
        'flat_root_command_count': int(flat_count or 0),
        'legacy_flat_aliases_estimated': int(legacy),
        'compatibility_mode': 'grouped-primary-flat-compat' if normalized_mode == 'compat' else normalized_mode,
        'flat_aliases_deprecated': bool(alias_map),
        'flat_alias_map': alias_map,
        'removed_flat_aliases': sorted(removed_flat_aliases or []),
        'missing_permission_gates': sorted(missing_permission_gates or []),
    }


def required_subscription_event_kinds() -> list[str]:
    required = {
        'community.announcement.created',
        'community.announcement.updated',
        'community.devlog.created',
        'community.devlog.updated',
        'community.world_signal.created',
        'community.event.reminder',
        'community.stage.announcement',
        'community.guild_recruitment.created',
        'community.chronicle.created',
        'community.lore_discussion.created',
        'community.appeal.created',
        'community.report.created',
        'community.support.created',
        'community.bug_report.created',
        'community.suggestion.created',
    }
    return sorted(required)
