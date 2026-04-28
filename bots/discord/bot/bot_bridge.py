from __future__ import annotations

from typing import Any

from .bot_transport import external_attachment_details


def extract_external_comment_id(payload: dict[str, Any]) -> str:
    for key in ('external_comment_id', 'comment_id', 'message_id'):
        value = str(payload.get(key) or '').strip()
        if value:
            return value
    return ''


def render_external_comment_body(payload: dict[str, Any], *, max_length: int = 1800) -> str:
    text = str(payload.get('comment') or payload.get('body') or payload.get('text') or '').strip()
    actor_name = str(payload.get('actor_name') or payload.get('staff_owner_name') or '').strip()
    _attachments, _image_urls, attachment_lines = external_attachment_details(payload)
    lines: list[str] = []
    if actor_name:
        lines.append(f'От внешней системы / {actor_name}')
    if text:
        lines.append(text)
    if attachment_lines:
        lines.append('Вложения: ' + ' | '.join(attachment_lines[:5]))
    rendered = '\n'.join(line for line in lines if line).strip() or 'Комментарий без текста.'
    return rendered[:max_length]


def external_comment_attachments(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    return external_attachment_details(payload)


def build_external_comment_notice(action: str, payload: dict[str, Any]) -> str:
    external_comment_id = extract_external_comment_id(payload)
    suffix = f' #{external_comment_id}' if external_comment_id else ''
    if action == 'comment.edited':
        return f'✏️ Внешняя система обновила комментарий{suffix}.'
    if action == 'comment.deleted':
        return f'🗑️ Внешняя система удалила комментарий{suffix}.'
    return f'💬 Внешняя система добавила комментарий{suffix}.'
