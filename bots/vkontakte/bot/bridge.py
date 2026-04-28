from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Mapping
from urllib.parse import urlparse

import requests

from nmbot.event_contracts import build_transport_event, parse_expires_at_epoch, validate_transport_event
from nmbot.storage import Storage
from nmbot.vk_api import VKClient

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class ReplayGuard:
    ttl_seconds: int = 600
    _seen: dict[str, int] = field(default_factory=dict)
    _lock: threading.RLock = field(default_factory=threading.RLock)

    def register(self, key: str) -> bool:
        now = int(time.time())
        with self._lock:
            expired = [item for item, expires_at in self._seen.items() if expires_at <= now]
            for item in expired:
                self._seen.pop(item, None)
            if key in self._seen:
                return False
            self._seen[key] = now + self.ttl_seconds
            return True

    def size(self) -> int:
        with self._lock:
            return len(self._seen)


def _header(headers: Mapping[str, str], name: str) -> str:
    lower = name.lower()
    for key, value in headers.items():
        if key.lower() == lower:
            return str(value)
    return ''


def verify_signed_request(*, path: str, raw_body: bytes, headers: Mapping[str, str], hmac_secret: str | Mapping[str, str], max_skew_seconds: int = 300) -> tuple[bool, str]:
    if isinstance(hmac_secret, Mapping):
        available = {str(key): str(value) for key, value in hmac_secret.items() if str(value).strip()}
        if not available:
            return True, 'unsigned-allowed'
    else:
        secret = str(hmac_secret or '').strip()
        if not secret:
            return True, 'unsigned-allowed'
        available = {'default': secret}

    timestamp = _header(headers, 'X-Timestamp').strip()
    nonce = _header(headers, 'X-Nonce').strip()
    signature = _header(headers, 'X-Signature').strip().lower()
    key_id = _header(headers, 'X-Key-Id').strip() or 'default'
    if not timestamp or not nonce or not signature:
        return False, 'missing_signature_headers'
    try:
        ts_value = int(timestamp)
    except ValueError:
        return False, 'bad_timestamp'
    if abs(int(time.time()) - ts_value) > max_skew_seconds:
        return False, 'timestamp_skew'

    body = raw_body.decode('utf-8')
    sign_payload = f'{path}\n{timestamp}\n{nonce}\n{body}'.encode('utf-8')
    candidates: list[tuple[str, str]] = []
    if key_id in available:
        candidates.append((key_id, available[key_id]))
    candidates.extend((name, secret) for name, secret in available.items() if name != key_id)
    for _name, secret in candidates:
        expected = hmac.new(secret.encode('utf-8'), sign_payload, hashlib.sha256).hexdigest().lower()
        if hmac.compare_digest(expected, signature):
            return True, nonce
    return False, 'bad_signature'


def bridge_auth_ok(settings: Any, *, path: str, raw_body: bytes, headers: Mapping[str, str], replay_guard: ReplayGuard | None = None) -> tuple[bool, str]:
    bearer = str(getattr(settings, 'bridge_inbound_bearer_token', '') or '').strip()
    if bearer:
        auth = _header(headers, 'Authorization').strip()
        if auth != f'Bearer {bearer}':
            return False, 'bad_bearer'
    hmac_secret = str(getattr(settings, 'bridge_inbound_hmac_secret', '') or '').strip()
    strict = bool(getattr(settings, 'bridge_ingress_strict_auth', True))
    if strict and not bearer and not hmac_secret:
        return False, 'bridge_auth_not_configured'

    ok, reason = verify_signed_request(path=path, raw_body=raw_body, headers=headers, hmac_secret=hmac_secret)
    if strict and not ok:
        return False, reason
    if ok and replay_guard is not None and reason not in {'unsigned-allowed', ''}:
        idempotency = _header(headers, 'X-Idempotency-Key').strip() or reason
        if not replay_guard.register(f'{path}:{idempotency}'):
            return False, 'replay_detected'
    return True, 'ok'


def _event_label(event_type: str) -> str:
    labels = {
        'community.announcement.created': '📣 Анонс',
        'community.announcement.updated': '📣 Обновление анонса',
        'community.devlog.created': '🛠 Devlog',
        'community.event.created': '📅 Событие',
        'community.world_signal.created': '🌌 Сигнал мира',
        'community.support.created': '🧰 Поддержка',
        'community.support.reply': '💬 Ответ поддержки',
    }
    return labels.get(event_type, event_type)


def _infer_tags(event_type: str, payload: Mapping[str, Any]) -> set[str]:
    tags: set[str] = set()
    raw_tags = payload.get('tags')
    if isinstance(raw_tags, list):
        tags.update(str(item).strip().lower() for item in raw_tags if str(item).strip())
    raw_tag = payload.get('tag')
    if raw_tag:
        tags.add(str(raw_tag).strip().lower())
    event_map = {
        'community.announcement.created': {'news'},
        'community.announcement.updated': {'news'},
        'community.devlog.created': {'devlogs'},
        'community.event.created': {'events'},
        'community.world_signal.created': {'world'},
        'community.support.reply': {'support'},
    }
    tags.update(event_map.get(event_type, set()))
    return tags


def _split_message(text: str, limit: int = 3500) -> list[str]:
    if len(text) <= limit:
        return [text]
    parts: list[str] = []
    current = ''
    for line in text.splitlines():
        candidate = f'{current}\n{line}'.strip() if current else line
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            parts.append(current)
            current = ''
        while len(line) > limit:
            parts.append(line[:limit])
            line = line[limit:]
        current = line
    if current:
        parts.append(current)
    return parts or [text[:limit]]


def render_vk_message(event: Mapping[str, Any]) -> str:
    event_type = str(event.get('event_type') or 'unknown')
    payload = event.get('payload') if isinstance(event.get('payload'), Mapping) else {}
    title = str(payload.get('title') or payload.get('kind') or 'NeverMine').strip()
    body = str(payload.get('body') or payload.get('text') or payload.get('description') or payload.get('details') or '').strip()
    url = str(payload.get('url') or '').strip()
    ticket_id = str(payload.get('ticket_id') or '').strip()
    attachments = payload.get('attachments') if isinstance(payload.get('attachments'), list) else []

    parts = [_event_label(event_type)]
    if title:
        parts.append(title)
    if ticket_id:
        parts.append(f'Тикет: {ticket_id}')
    if body:
        parts.extend(['', body])
    if attachments:
        parts.extend(['', f'Вложений: {len(attachments)}'])
    if url:
        parts.extend(['', f'Ссылка: {url}'])
    parts.extend(['', f'Источник: {event.get("source", "bridge")}'])
    return '\n'.join(parts).strip()


def should_accept_event(settings: Any, event: Mapping[str, Any]) -> bool:
    allowed = set(getattr(settings, 'bridge_allowed_event_types', frozenset()) or frozenset())
    event_type = str(event.get('event_type') or '').strip().lower()
    if allowed and event_type not in allowed:
        return False
    payload = event.get('payload') if isinstance(event.get('payload'), Mapping) else {}
    configured_tags = set(getattr(settings, 'bridge_target_tags', frozenset()) or frozenset())
    if configured_tags:
        incoming_tags = _infer_tags(event_type, payload)
        if not incoming_tags.intersection(configured_tags):
            return False
    return True


def _select_target_peers(settings: Any, event: Mapping[str, Any]) -> tuple[int, ...]:
    payload = event.get('payload') if isinstance(event.get('payload'), Mapping) else {}
    scope = str(getattr(settings, 'bridge_target_scope', 'all') or 'all').lower()
    all_peers = tuple(int(peer_id) for peer_id in (getattr(settings, 'bridge_target_peer_ids', tuple()) or tuple()))
    if scope == 'all':
        return all_peers
    if scope == 'private':
        return tuple(peer_id for peer_id in all_peers if peer_id < 2_000_000_000)
    if scope == 'groups':
        return tuple(peer_id for peer_id in all_peers if peer_id >= 2_000_000_000)
    if scope == 'current':
        candidate = payload.get('target_peer_id') or payload.get('peer_id')
        if candidate is None:
            return tuple()
        try:
            return (int(candidate),)
        except (TypeError, ValueError):
            return tuple()
    return all_peers


def _payload_attachments(payload: Mapping[str, Any]) -> str:
    attachments = payload.get('attachments')
    if isinstance(attachments, list):
        values = [str(item).strip() for item in attachments if str(item).strip()]
        return ','.join(values)
    if attachments:
        return str(attachments).strip()
    return ''


def _should_post_to_wall(settings: Any, event_type: str) -> bool:
    if not bool(getattr(settings, 'vk_wall_post_enabled', False)):
        return False
    return event_type in {
        'community.announcement.created',
        'community.announcement.updated',
        'community.devlog.created',
        'community.event.created',
        'community.world_signal.created',
    }


def deliver_event_to_vk(settings: Any, vk: VKClient, storage: Storage, event: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(event)
    errors = validate_transport_event(payload, max_future_skew_seconds=int(getattr(settings, 'event_max_future_skew_seconds', 300) or 300))
    if errors:
        raise ValueError(f'invalid bridge event: {", ".join(errors)}')
    event_id = str(payload.get('event_id') or '')
    event_type = str(payload.get('event_type') or '')
    source = str(payload.get('source') or 'bridge')
    if storage.has_processed_event(event_id):
        return {'status': 'duplicate', 'sent': 0, 'event_id': event_id}
    if not should_accept_event(settings, payload):
        storage.register_processed_event(event_id=event_id, source=source, event_type=event_type, expires_at=parse_expires_at_epoch(payload))
        return {'status': 'ignored', 'sent': 0, 'event_id': event_id}
    body_payload = payload.get('payload') if isinstance(payload.get('payload'), Mapping) else {}
    peer_ids = _select_target_peers(settings, payload)
    if not peer_ids:
        storage.register_processed_event(event_id=event_id, source=source, event_type=event_type, expires_at=parse_expires_at_epoch(payload))
        return {'status': 'ignored_no_target', 'sent': 0, 'event_id': event_id}
    text = render_vk_message(payload)
    attachments = _payload_attachments(body_payload)
    sent = 0
    for peer_id in peer_ids:
        try:
            for index, part in enumerate(_split_message(text)):
                vk.send_message(int(peer_id), part, attachment=attachments if index == 0 else '')
            sent += 1
        except Exception:
            LOGGER.exception('Failed to deliver bridge event to VK peer_id=%s', peer_id)
    if _should_post_to_wall(settings, event_type):
        try:
            vk.wall_post(int(getattr(settings, 'vk_group_id')), text, attachment=attachments)
        except Exception:
            LOGGER.exception('Failed to mirror bridge event to VK wall')
    if sent > 0:
        storage.register_processed_event(event_id=event_id, source=source, event_type=event_type, expires_at=parse_expires_at_epoch(payload))
        return {'status': 'delivered', 'sent': sent, 'event_id': event_id}
    return {'status': 'delivery_failed', 'sent': 0, 'event_id': event_id}


def make_signed_headers(url: str, body: str, *, bearer_token: str = '', hmac_secret: str = '', key_id: str = 'v1') -> dict[str, str]:
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    if bearer_token:
        headers['Authorization'] = f'Bearer {bearer_token}'
    if hmac_secret:
        path = urlparse(url).path or '/'
        timestamp = str(int(time.time()))
        nonce = str(uuid.uuid4())
        sign_payload = f'{path}\n{timestamp}\n{nonce}\n{body}'.encode('utf-8')
        headers['X-Key-Id'] = key_id
        headers['X-Timestamp'] = timestamp
        headers['X-Nonce'] = nonce
        headers['X-Signature'] = hmac.new(hmac_secret.encode('utf-8'), sign_payload, hashlib.sha256).hexdigest()
        headers['X-Action-Version'] = '1'
        headers['X-Idempotency-Key'] = nonce
    return headers


def prepare_discord_request(settings: Any, event: dict[str, Any]) -> dict[str, Any] | None:
    url = str(getattr(settings, 'discord_bridge_url', '') or '').strip()
    if not url:
        return None
    body_json = json.dumps(event, ensure_ascii=False, sort_keys=True)
    headers = make_signed_headers(
        url,
        body_json,
        bearer_token=str(getattr(settings, 'discord_bridge_bearer_token', '') or '').strip(),
        hmac_secret=str(getattr(settings, 'discord_bridge_hmac_secret', '') or '').strip(),
        key_id=str(getattr(settings, 'outbound_key_id', 'v1') or 'v1').strip(),
    )
    return {
        'url': url,
        'body_json': body_json,
        'headers': headers,
        'headers_json': json.dumps(headers, ensure_ascii=False, sort_keys=True),
        'timeout': int(getattr(settings, 'bridge_timeout_seconds', 5) or 5),
    }


def send_prepared_request(url: str, body_json: str, headers: str | Mapping[str, str], timeout: int) -> None:
    resolved_headers = json.loads(headers) if isinstance(headers, str) else dict(headers)
    response = requests.post(url, data=body_json.encode('utf-8'), headers=resolved_headers, timeout=timeout)
    response.raise_for_status()


def build_vk_support_event(*, user_id: int, peer_id: int, text: str, ticket_id: str, correlation_id: str, attachments: list[str] | None = None) -> dict[str, Any]:
    return build_transport_event(
        event_type='community.support.created',
        source='vk-bridge',
        payload={
            'title': 'Обращение из VK',
            'body': text,
            'actor_user_id': f'vk:{user_id}',
            'external_topic_id': f'vk:{peer_id}:{user_id}',
            'ticket_id': ticket_id,
            'correlation_id': correlation_id,
            'status': 'new',
            'tags': ['support'],
            'attachments': list(attachments or []),
        },
    )


def build_vk_support_reply_event(*, ticket_id: str, actor_user_id: int, text: str, peer_id: int, original_user_id: int, attachments: list[str] | None = None) -> dict[str, Any]:
    return build_transport_event(
        event_type='community.support.reply',
        source='vk-bridge',
        payload={
            'title': f'Ответ по тикету {ticket_id}',
            'body': text,
            'ticket_id': ticket_id,
            'peer_id': peer_id,
            'target_user_id': f'vk:{original_user_id}',
            'actor_user_id': f'vk:{actor_user_id}',
            'tags': ['support'],
            'attachments': list(attachments or []),
        },
    )


def build_vk_announcement_event(*, user_id: int, text: str, url: str = '', attachments: list[str] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        'title': 'Анонс NeverMine из VK',
        'text': text,
        'actor_user_id': f'vk:{user_id}',
        'tags': ['news'],
        'attachments': list(attachments or []),
    }
    if url:
        payload['url'] = url
    return build_transport_event(event_type='community.announcement.created', source='vk-bridge', payload=payload)
