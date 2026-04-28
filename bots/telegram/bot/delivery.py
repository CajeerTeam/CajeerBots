from __future__ import annotations

import asyncio
import html
import re
import shlex
import time
from dataclasses import dataclass, field
from typing import Any, Iterable

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

MAX_TEXT_LEN = 4096
MAX_CAPTION_LEN = 1024
_SPLIT_RE = re.compile(r"\s*--\s*", re.UNICODE)


@dataclass(slots=True)
class OutgoingPayload:
    text: str
    media_kind: str = ''
    media_ref: str = ''
    message_thread_id: int | None = None
    disable_notification: bool = False
    reply_markup: InlineKeyboardMarkup | None = None
    parse_mode: str | None = None
    silent: bool = False
    priority: int = 0
    delivery_tag: str = ''
    dry_run: bool = False


@dataclass(slots=True)
class DeliveryResult:
    chat_id: int
    ok: bool
    error: str = ''
    dry_run: bool = False


def build_inline_buttons(buttons: list[dict[str, str]] | None) -> InlineKeyboardMarkup | None:
    if not buttons:
        return None
    rows: list[list[InlineKeyboardButton]] = []
    current: list[InlineKeyboardButton] = []
    for item in buttons:
        text = str(item.get('text') or '').strip()
        if not text:
            continue
        if item.get('callback_data'):
            button = InlineKeyboardButton(text=text, callback_data=str(item['callback_data']))
        elif item.get('url'):
            button = InlineKeyboardButton(text=text, url=str(item['url']))
        else:
            continue
        current.append(button)
        if len(current) >= 2:
            rows.append(current)
            current = []
    if current:
        rows.append(current)
    return InlineKeyboardMarkup(rows) if rows else None


def _safe_chunks(text: str, limit: int) -> list[str]:
    text = text or ''
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        cut = remaining.rfind('\n', 0, limit)
        if cut <= 0:
            cut = remaining.rfind(' ', 0, limit)
        if cut <= 0:
            cut = limit
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def parse_delivery_tokens(raw: str) -> tuple[dict[str, Any], str]:
    options: dict[str, Any] = {
        'scope': 'all',
        'tags': [],
        'media_kind': '',
        'media_ref': '',
        'message_thread_id': None,
        'disable_notification': False,
        'dry_run': False,
        'priority': 0,
        'buttons': [],
    }
    if not raw:
        return options, ''
    normalized = raw.replace('—', '--').replace(' – ', ' -- ').replace('--', ' -- ')
    parts = _SPLIT_RE.split(normalized, maxsplit=1)
    if len(parts) == 1:
        before, after = raw.strip(), ''
    else:
        before, after = parts[0].strip(), parts[1].strip()
    try:
        tokens = shlex.split(before)
    except ValueError:
        tokens = before.split()
    message = after if after else raw.strip()
    if after:
        message = after
    elif any(token.startswith(('scope=', 'tags=', 'thread=', 'silent=', 'media=', 'dry_run=', 'priority=', 'button=')) for token in tokens):
        message = ''
    for token in tokens:
        if token.startswith('scope='):
            options['scope'] = token.split('=', 1)[1].strip().lower() or 'all'
        elif token.startswith('tags='):
            options['tags'] = [item.strip().lower() for item in token.split('=', 1)[1].split(',') if item.strip()]
        elif token.startswith('thread='):
            try:
                options['message_thread_id'] = int(token.split('=', 1)[1].strip())
            except ValueError:
                options['message_thread_id'] = None
        elif token.startswith('silent='):
            options['disable_notification'] = token.split('=', 1)[1].strip().lower() in {'1', 'true', 'yes', 'on'}
        elif token.startswith('dry_run='):
            options['dry_run'] = token.split('=', 1)[1].strip().lower() in {'1', 'true', 'yes', 'on'}
        elif token.startswith('priority='):
            try:
                options['priority'] = int(token.split('=', 1)[1].strip())
            except ValueError:
                options['priority'] = 0
        elif token.startswith('media='):
            value = token.split('=', 1)[1].strip()
            if ':' in value:
                options['media_kind'], options['media_ref'] = value.split(':', 1)
                options['media_kind'] = options['media_kind'].strip().lower()
                options['media_ref'] = options['media_ref'].strip()
        elif token.startswith('button='):
            value = token.split('=', 1)[1].strip()
            if '|' in value:
                text, url = value.split('|', 1)
                options['buttons'].append({'text': text.strip(), 'url': url.strip()})
    return options, message.strip()


async def send_payload(bot: Bot, *, chat_id: int, payload: OutgoingPayload, parse_mode: str, db=None) -> None:
    if payload.dry_run:
        return
    effective_parse_mode = payload.parse_mode or parse_mode
    kwargs = {
        'chat_id': chat_id,
        'disable_notification': payload.disable_notification or payload.silent,
    }
    if payload.message_thread_id is not None:
        kwargs['message_thread_id'] = payload.message_thread_id
    if payload.reply_markup is not None:
        kwargs['reply_markup'] = payload.reply_markup

    if payload.media_kind in {'photo', 'document', 'video', 'animation'} and payload.media_ref:
        caption_chunks = _safe_chunks(payload.text, MAX_CAPTION_LEN)
        caption = caption_chunks[0] if caption_chunks else ''
        send_map = {
            'photo': bot.send_photo,
            'document': bot.send_document,
            'video': bot.send_video,
            'animation': bot.send_animation,
        }
        send_arg = {
            'photo': 'photo',
            'document': 'document',
            'video': 'video',
            'animation': 'animation',
        }[payload.media_kind]
        media_kwargs = dict(kwargs)
        media_kwargs['caption'] = caption
        media_kwargs['parse_mode'] = effective_parse_mode
        media_value = payload.media_ref
        if db is not None and hasattr(db, 'get_media_file_id'):
            cached = db.get_media_file_id(payload.media_kind, payload.media_ref)
            if cached:
                media_value = cached
        media_kwargs[send_arg] = media_value
        sent = await send_map[payload.media_kind](**media_kwargs)
        if db is not None and hasattr(db, 'set_media_file_id'):
            file_id = ''
            if payload.media_kind == 'photo' and getattr(sent, 'photo', None):
                file_id = sent.photo[-1].file_id
            elif payload.media_kind == 'document' and getattr(sent, 'document', None):
                file_id = sent.document.file_id
            elif payload.media_kind == 'video' and getattr(sent, 'video', None):
                file_id = sent.video.file_id
            elif payload.media_kind == 'animation' and getattr(sent, 'animation', None):
                file_id = sent.animation.file_id
            if file_id:
                db.set_media_file_id(payload.media_kind, payload.media_ref, file_id)
        for extra in caption_chunks[1:]:
            await bot.send_message(chat_id=chat_id, text=extra, parse_mode=effective_parse_mode, disable_notification=kwargs['disable_notification'], message_thread_id=payload.message_thread_id)
        return

    for idx, chunk in enumerate(_safe_chunks(payload.text, MAX_TEXT_LEN)):
        send_kwargs = dict(kwargs)
        if idx > 0:
            send_kwargs.pop('reply_markup', None)
        await bot.send_message(text=chunk, parse_mode=effective_parse_mode, **send_kwargs)


async def send_payloads_bounded(
    bot: Bot,
    deliveries: Iterable[tuple[int, OutgoingPayload]],
    *,
    parse_mode: str,
    max_concurrency: int = 5,
    max_per_minute: int = 120,
    paused_until_ts: float = 0.0,
    db=None,
) -> list[DeliveryResult]:
    items = sorted(list(deliveries), key=lambda item: getattr(item[1], "priority", 0), reverse=True)
    if not items:
        return []
    if paused_until_ts and time.time() < paused_until_ts:
        return [DeliveryResult(chat_id=chat_id, ok=False, error='delivery_paused') for chat_id, _ in items]

    semaphore = asyncio.Semaphore(max(1, max_concurrency))
    results: list[DeliveryResult] = [DeliveryResult(chat_id=chat_id, ok=False, error='not_started') for chat_id, _ in items]
    effective_per_minute = max(1, max_per_minute)
    batch_size = max(1, min(max_concurrency, effective_per_minute))
    sleep_per_batch = 60.0 * batch_size / float(effective_per_minute)

    async def _worker(index: int, chat_id: int, payload: OutgoingPayload) -> None:
        async with semaphore:
            try:
                await send_payload(bot, chat_id=chat_id, payload=payload, parse_mode=parse_mode, db=db)
                results[index] = DeliveryResult(chat_id=chat_id, ok=True, dry_run=payload.dry_run)
            except Exception as exc:  # pragma: no cover
                results[index] = DeliveryResult(chat_id=chat_id, ok=False, error=str(exc), dry_run=payload.dry_run)

    for start in range(0, len(items), batch_size):
        batch = items[start:start + batch_size]
        await asyncio.gather(*[_worker(start + idx, chat_id, payload) for idx, (chat_id, payload) in enumerate(batch)])
        if start + batch_size < len(items):
            await asyncio.sleep(min(max(sleep_per_batch, 0.0), 5.0))
    return results
