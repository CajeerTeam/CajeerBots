from __future__ import annotations

from typing import Any

from .event_contracts import declared_transport_event_types

_IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp'}


def extract_external_topic_id(payload: dict[str, Any]) -> str:
    for key in ('external_topic_id', 'topic_id', 'discussion_id', 'external_id', 'report_id'):
        value = str(payload.get(key) or '').strip()
        if value:
            return value
    return ''


def extract_external_content_id(payload: dict[str, Any], *, content_kind: str = '') -> str:
    preferred = {
        'announcement': ('external_announcement_id', 'announcement_id', 'external_message_id', 'message_id', 'id'),
        'devlog': ('external_devlog_id', 'devlog_id', 'external_message_id', 'message_id', 'id'),
    }.get(str(content_kind or '').strip().lower(), ('external_message_id', 'message_id', 'id'))
    for key in preferred:
        value = str(payload.get(key) or '').strip()
        if value:
            return value
    return ''


def external_attachment_details(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    attachments = payload.get('attachments') if isinstance(payload.get('attachments'), list) else []
    normalized: list[dict[str, Any]] = []
    image_urls: list[str] = []
    link_lines: list[str] = []
    for item in attachments[:8]:
        if not isinstance(item, dict):
            continue
        url = str(item.get('url') or '').strip()
        filename = str(item.get('filename') or '').strip()
        content_type = str(item.get('content_type') or '').strip().lower()
        normalized.append({'url': url, 'filename': filename, 'content_type': content_type})
        lower_name = filename.lower()
        is_image = bool(url and (content_type.startswith('image/') or any(lower_name.endswith(ext) for ext in _IMAGE_EXTENSIONS)))
        if is_image:
            image_urls.append(url)
        if url and filename:
            link_lines.append(f'[{filename}]({url})')
        elif url:
            link_lines.append(url)
        elif filename:
            link_lines.append(filename)
    return normalized, image_urls, link_lines


def handled_transport_event_types() -> list[str]:
    events = {
        'community.announcement.created',
        'community.announcement.updated',
        'community.announcement.deleted',
        'community.devlog.created',
        'community.devlog.updated',
        'community.devlog.deleted',
        'community.event.created',
        'community.world_signal.created',
        'identity.telegram.linked',
        'identity.vk.linked',
        'identity.workspace.linked',
        'identity.telegram.unlinked',
        'identity.vk.unlinked',
        'identity.workspace.unlinked',
        'identity.sync',
        'community.support.created',
        'community.support.updated',
        'community.support.closed',
        'community.support.reopened',
        'community.support.status_changed',
        'community.support.claimed',
        'community.support.unclaimed',
        'community.support.owner_changed',
        'community.support.comment.appended',
        'community.support.comment.edited',
        'community.support.comment.deleted',
        'community.bug_report.created',
        'community.bug_report.updated',
        'community.bug_report.closed',
        'community.bug_report.reopened',
        'community.bug_report.status_changed',
        'community.bug_report.claimed',
        'community.bug_report.unclaimed',
        'community.bug_report.owner_changed',
        'community.bug_report.comment.appended',
        'community.bug_report.comment.edited',
        'community.bug_report.comment.deleted',
        'community.suggestion.created',
        'community.suggestion.updated',
        'community.suggestion.closed',
        'community.suggestion.reopened',
        'community.suggestion.status_changed',
        'community.suggestion.comment.appended',
        'community.suggestion.comment.edited',
        'community.suggestion.comment.deleted',
        'community.appeal.created',
        'community.appeal.updated',
        'community.appeal.closed',
        'community.appeal.reopened',
        'community.appeal.status_changed',
        'community.appeal.claimed',
        'community.appeal.unclaimed',
        'community.appeal.owner_changed',
        'community.appeal.comment.appended',
        'community.appeal.comment.edited',
        'community.appeal.comment.deleted',
        'community.guild_recruitment.created',
        'community.guild_recruitment.updated',
        'community.guild_recruitment.closed',
        'community.guild_recruitment.reopened',
        'community.guild_recruitment.paused',
        'community.guild_recruitment.bumped',
        'community.guild_recruitment.status_changed',
        'community.guild_recruitment.comment.appended',
        'community.guild_recruitment.comment.edited',
        'community.guild_recruitment.comment.deleted',
        'community.chronicle.created',
        'community.chronicle.updated',
        'community.chronicle.status_changed',
        'community.chronicle.comment.appended',
        'community.chronicle.comment.edited',
        'community.chronicle.comment.deleted',
        'community.lore_discussion.created',
        'community.lore_discussion.updated',
        'community.lore_discussion.closed',
        'community.lore_discussion.reopened',
        'community.lore_discussion.status_changed',
        'community.lore_discussion.comment.appended',
        'community.lore_discussion.comment.edited',
        'community.lore_discussion.comment.deleted',
        'community.report.created',
        'community.report.updated',
        'community.report.closed',
        'community.report.reopened',
        'community.report.status_changed',
        'community.report.claimed',
        'community.report.unclaimed',
        'community.report.owner_changed',
        'community.report.comment.appended',
        'community.report.comment.edited',
        'community.report.comment.deleted',
    }
    return sorted(events)


def transport_contract_coverage_snapshot() -> dict[str, list[str]]:
    declared = set(declared_transport_event_types())
    handled = set(handled_transport_event_types())
    return {
        'declared_only': sorted(declared - handled),
        'handled_only': sorted(handled - declared),
        'intersection': sorted(declared & handled),
    }
