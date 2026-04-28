from __future__ import annotations

from typing import Any



def topic_kind_to_event_prefix(topic_kind: str) -> str:
    mapping = {
        'support': 'community.support',
        'bug': 'community.bug_report',
        'suggestion': 'community.suggestion',
        'appeal': 'community.appeal',
        'guild_recruitment': 'community.guild_recruitment',
        'report': 'community.report',
        'chronicle': 'community.chronicle',
        'lore_discussion': 'community.lore_discussion',
    }
    return mapping.get(str(topic_kind or '').strip().lower(), f"community.{str(topic_kind or '').strip().lower()}")



def build_outbound_comment_payload(message: Any, topic: dict[str, Any], *, action: str = 'comment.appended') -> tuple[str, dict[str, Any]]:
    topic_kind = str(topic.get('topic_kind') or '').strip().lower()
    action = str(action or 'comment.appended').strip().lower() or 'comment.appended'
    event_kind = f"{topic_kind_to_event_prefix(topic_kind)}.{action}"
    attachments = []
    for attachment in getattr(message, 'attachments', []) or []:
        attachments.append({
            'filename': getattr(attachment, 'filename', ''),
            'url': getattr(attachment, 'url', ''),
            'size': int(getattr(attachment, 'size', 0) or 0),
            'content_type': str(getattr(attachment, 'content_type', '') or ''),
        })
    payload = {
        'thread_id': str(topic.get('thread_id') or getattr(message.channel, 'id', '')),
        'topic_kind': topic_kind,
        'title': str(topic.get('title') or getattr(message.channel, 'name', '')),
        'comment': str((getattr(message, 'content', None) or '')).strip(),
        'message_id': str(getattr(message, 'id', '') or ''),
        'external_comment_id': str(getattr(message, 'id', '') or ''),
        'actor_user_id': str(getattr(getattr(message, 'author', None), 'id', '') or ''),
        'actor_name': str(getattr(getattr(message, 'author', None), 'display_name', None) or getattr(getattr(message, 'author', None), 'name', '') or ''),
        'attachments': attachments,
        'source_platform': 'discord',
        'edited_at': '',
        'deleted_at': '',
    }
    return event_kind, payload



def targeted_digest_lines(*, digest_kind: str, overdue_support: list[dict[str, Any]] | None = None, overdue_appeals: list[dict[str, Any]] | None = None, failed_bridge: list[dict[str, Any]] | None = None, rules_outdated_count: int = 0, stale_approvals: int = 0) -> list[str]:
    digest_kind = str(digest_kind or '').strip().lower()
    overdue_support = overdue_support or []
    overdue_appeals = overdue_appeals or []
    failed_bridge = failed_bridge or []
    lines: list[str] = []
    if digest_kind in {'support', 'staff'} and overdue_support:
        lines.append(f'Темы поддержки без реакции: {len(overdue_support)}')
    if digest_kind in {'appeals', 'staff'} and overdue_appeals:
        lines.append(f'Апелляции без реакции: {len(overdue_appeals)}')
    if digest_kind in {'bridge', 'staff'} and failed_bridge:
        lines.append(f'Bridge-события с ошибками: {len(failed_bridge)}')
    if digest_kind in {'rules', 'staff'} and rules_outdated_count:
        lines.append(f'Участников с устаревшей версией правил: {rules_outdated_count}')
    if digest_kind in {'approval', 'staff'} and stale_approvals:
        lines.append(f'Просроченных approval-запросов: {stale_approvals}')
    return lines



def build_capability_report(*, runtime_version: str, build_info: dict[str, Any], grouped_command_count: int, schema_version: int, schema_parity_issues: list[str], migration_count: int, extra_checks: dict[str, Any] | None = None, flat_command_count: int = 0) -> dict[str, Any]:
    features = {str(item) for item in build_info.get('features') or []}
    checks = {
        'slash_groups_declared': 'command-groups' in features,
        'approval_queue_declared': 'approval-queue' in features,
        'targeted_digests_declared': 'staff-digest' in features,
        'runtime_version_matches_build': str(build_info.get('version') or '').strip() == runtime_version,
        'schema_parity_ok': not schema_parity_issues,
        'grouped_surface_primary': int(grouped_command_count or 0) > 0,
    }
    if extra_checks:
        checks.update(extra_checks)
    declared_runtime = {
        'command_groups': 'command-groups' in features,
        'approval_expiry': 'approval-expiry-sweeper' in features or 'approval-expiry' in features,
        'comment_lifecycle': 'comment-edit-delete-parity' in features,
        'state_restore_symmetry': 'state-restore-symmetry' in features or 'state-restore' in features,
        'subscription_routing': 'subscription-live-routing' in features or 'staff-digest' in features,
    }
    readiness = {
        'runtime_version': runtime_version,
        'build_version': str(build_info.get('version') or '').strip(),
        'schema_version': int(schema_version or 0),
        'migration_count': int(migration_count or 0),
        'grouped_command_count': int(grouped_command_count or 0),
        'flat_command_count': int(flat_command_count or 0),
        'schema_parity_issues': schema_parity_issues,
        'checks': checks,
        'declared_runtime': declared_runtime,
    }
    readiness['ready'] = bool(readiness['checks'].get('runtime_version_matches_build')) and not schema_parity_issues and int(grouped_command_count or 0) > 0
    return readiness
