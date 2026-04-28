from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
import os
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)
_EXPECTED_SCHEMA = '1'


@dataclass(slots=True)
class ServerStatus:
    ok: bool
    server_name: str = 'NeverMine'
    online: bool = False
    players_online: int | None = None
    max_players: int | None = None
    version: str = ''
    motd: str = ''
    latency_ms: int | None = None
    raw: dict[str, Any] | None = None


@dataclass(slots=True)
class FeedAnnouncement:
    event_id: str
    text: str
    nonce: str = ""
    tag: str = ''
    created_at: str = ''
    media_kind: str = ''
    media_ref: str = ''
    buttons: list[dict[str, str]] | None = None
    priority: int = 0
    silent: bool = False
    parse_mode: str = ''
    timestamp: str = ""
    shard: str = ''


@dataclass(slots=True)
class LinkVerificationResult:
    ok: bool
    player_uuid: str = ''
    player_name: str = ''
    raw: dict[str, Any] | None = None


@dataclass(slots=True)
class SecurityActionResult:
    ok: bool
    message: str = ''
    raw: dict[str, Any] | None = None


@dataclass(slots=True)
class CompatibilityResult:
    ok: bool
    api_version: str = ''
    message: str = ''
    raw: dict[str, Any] | None = None

@dataclass(slots=True)
class SecurityChallenge:
    challenge_id: str
    player_name: str
    telegram_user_id: int | None = None
    ip_address: str = ''
    server_name: str = ''
    action: str = '2fa'
    created_at: str = ''
    expires_at: str = ''
    title: str = ''
    message: str = ''
    metadata: dict[str, Any] | None = None
    nonce: str = ""
    timestamp: str = ""
    shard: str = ''


class _CircuitBreaker:
    def __init__(self, threshold: int, reset_seconds: int) -> None:
        self.threshold = max(1, threshold)
        self.reset_seconds = max(1, reset_seconds)
        self.failures = 0
        self.open_until = 0.0

    def can_request(self) -> bool:
        return time.monotonic() >= self.open_until

    def success(self) -> None:
        self.failures = 0
        self.open_until = 0.0

    def failure(self) -> None:
        self.failures += 1
        if self.failures >= self.threshold:
            self.open_until = time.monotonic() + self.reset_seconds


class ServerStatusClient:
    def __init__(
        self,
        status_url: str,
        timeout_seconds: float,
        *,
        cache_ttl_seconds: float = 10.0,
        retry_attempts: int = 2,
        retry_backoff_seconds: float = 0.75,
        bearer_token: str = '',
        hmac_secret: str = '',
        request_id_header: str = 'X-Request-ID',
        announcement_feed_url: str = '',
        link_verify_url: str = '',
        strict_schemas: bool = True,
        security_status_url: str = '',
        security_challenges_url: str = '',
        security_2fa_action_url: str = '',
        security_recovery_url: str = '',
        security_sessions_url: str = '',
        security_session_action_url: str = '',
        circuit_threshold: int = 4,
        circuit_reset_seconds: int = 120,
    ) -> None:
        self.status_url = status_url.strip()
        self.announcement_feed_url = announcement_feed_url.strip()
        self.link_verify_url = link_verify_url.strip()
        self.security_status_url = security_status_url.strip()
        self.security_challenges_url = security_challenges_url.strip()
        self.security_2fa_action_url = security_2fa_action_url.strip()
        self.security_recovery_url = security_recovery_url.strip()
        self.security_sessions_url = security_sessions_url.strip()
        self.security_session_action_url = security_session_action_url.strip()
        self.cache_ttl_seconds = max(cache_ttl_seconds, 0.0)
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.bearer_token = bearer_token.strip()
        self.hmac_secret = hmac_secret.strip()
        self.request_id_header = request_id_header.strip() or 'X-Request-ID'
        self.strict_schemas = strict_schemas
        self._client = httpx.AsyncClient(timeout=timeout_seconds)
        self._cached_status: ServerStatus | None = None
        self._cached_at = 0.0
        self._lock = asyncio.Lock()
        self._breakers: dict[str, _CircuitBreaker] = {}
        self._breaker_threshold = circuit_threshold
        self._breaker_reset = circuit_reset_seconds

    def _breaker(self, endpoint: str) -> _CircuitBreaker:
        breaker = self._breakers.get(endpoint)
        if breaker is None:
            breaker = _CircuitBreaker(self._breaker_threshold, self._breaker_reset)
            self._breakers[endpoint] = breaker
        return breaker

    def is_configured(self) -> bool:
        return bool(self.status_url)

    def feed_is_configured(self) -> bool:
        return bool(self.announcement_feed_url)

    def link_verify_is_configured(self) -> bool:
        return bool(self.link_verify_url)

    def security_is_configured(self) -> bool:
        return any([self.security_status_url, self.security_challenges_url, self.security_2fa_action_url, self.security_recovery_url, self.security_sessions_url, self.security_session_action_url])

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch_status(self, *, force: bool = False, shard: str = "") -> ServerStatus:
        if not self.status_url:
            return ServerStatus(ok=False, raw={'reason': 'status_url_not_configured'})
        async with self._lock:
            if not force and self._cached_status is not None and (time.monotonic() - self._cached_at) <= self.cache_ttl_seconds:
                return self._cached_status
            try:
                params = {'shard': shard} if shard else None
                data = await self._request_json_with_retries('GET', self.status_url, endpoint='status', params=params)
                status = self._parse_status(data)
            except (httpx.HTTPError, ValueError, RuntimeError) as exc:
                status = ServerStatus(ok=False, raw={'reason': str(exc)})
            self._cached_status = status
            self._cached_at = time.monotonic()
            return status

    async def fetch_announcements(self) -> list[FeedAnnouncement]:
        if not self.announcement_feed_url:
            return []
        data = await self._request_json_with_retries('GET', self.announcement_feed_url, endpoint='feed')
        return self._parse_feed(data)

    async def verify_link_code(self, *, code: str, telegram_user_id: int) -> LinkVerificationResult:
        if not self.link_verify_url:
            return LinkVerificationResult(ok=False, raw={'reason': 'link_verify_url_not_configured'})
        payload = {'schema_version': _EXPECTED_SCHEMA, 'code': code, 'telegram_user_id': telegram_user_id}
        data = await self._request_json_with_retries('POST', self.link_verify_url, endpoint='link_verify', json_body=payload)
        return self._parse_link_verify(data)

    async def get_security_status(self, *, telegram_user_id: int) -> SecurityActionResult:
        if not self.security_status_url:
            return SecurityActionResult(ok=False, message='security_status_url_not_configured')
        data = await self._request_json_with_retries('GET', self.security_status_url, endpoint='security_status', params={'telegram_user_id': str(telegram_user_id)})
        ok = bool(data.get('ok', True)) if isinstance(data, dict) else False
        return SecurityActionResult(ok=ok, message=str(data.get('message', '') if isinstance(data, dict) else ''), raw=data if isinstance(data, dict) else {'payload': data})

    async def fetch_security_challenges(self) -> list[SecurityChallenge]:
        if not self.security_challenges_url:
            return []
        data = await self._request_json_with_retries('GET', self.security_challenges_url, endpoint='security_challenges')
        items = data.get('items', data) if isinstance(data, dict) else data
        result: list[SecurityChallenge] = []
        if not isinstance(items, list):
            return result
        for item in items:
            if not isinstance(item, dict):
                continue
            result.append(
                SecurityChallenge(
                    challenge_id=str(item.get('challenge_id') or item.get('id') or ''),
                    player_name=str(item.get('player_name') or item.get('username') or '-'),
                    telegram_user_id=int(item.get('telegram_user_id')) if item.get('telegram_user_id') not in (None, '') else None,
                    ip_address=str(item.get('ip_address') or item.get('ip') or ''),
                    server_name=str(item.get('server_name') or item.get('server') or ''),
                    action=str(item.get('action') or '2fa'),
                    created_at=str(item.get('created_at') or ''),
                    expires_at=str(item.get('expires_at') or ''),
                    title=str(item.get('title') or ''),
                    message=str(item.get('message') or ''),
                    metadata=item if isinstance(item, dict) else None,
                    nonce=str(item.get('nonce') or item.get('idempotency_key') or ''),
                    timestamp=str(item.get('timestamp') or item.get('created_at') or ''),
                )
            )
        return [item for item in result if item.challenge_id]

    async def act_2fa_challenge(self, *, challenge_id: str, action: str, actor_user_id: int) -> SecurityActionResult:
        if not self.security_2fa_action_url:
            return SecurityActionResult(ok=False, message='security_2fa_action_url_not_configured')
        payload = {'schema_version': _EXPECTED_SCHEMA, 'challenge_id': challenge_id, 'action': action, 'actor_user_id': actor_user_id}
        data = await self._request_json_with_retries('POST', self.security_2fa_action_url, endpoint='security_2fa_action', json_body=payload)
        ok = bool(data.get('ok', True)) if isinstance(data, dict) else False
        return SecurityActionResult(ok=ok, message=str(data.get('message', '') if isinstance(data, dict) else ''), raw=data if isinstance(data, dict) else {'payload': data})

    async def request_password_recovery(self, *, telegram_user_id: int, player_name: str) -> SecurityActionResult:
        if not self.security_recovery_url:
            return SecurityActionResult(ok=False, message='security_recovery_url_not_configured')
        payload = {'schema_version': _EXPECTED_SCHEMA, 'telegram_user_id': telegram_user_id, 'player_name': player_name}
        data = await self._request_json_with_retries('POST', self.security_recovery_url, endpoint='security_recovery', json_body=payload)
        ok = bool(data.get('ok', True)) if isinstance(data, dict) else False
        return SecurityActionResult(ok=ok, message=str(data.get('message', '') if isinstance(data, dict) else ''), raw=data if isinstance(data, dict) else {'payload': data})

    async def list_security_sessions(self, *, telegram_user_id: int) -> SecurityActionResult:
        if not self.security_sessions_url:
            return SecurityActionResult(ok=False, message='security_sessions_url_not_configured')
        data = await self._request_json_with_retries('GET', self.security_sessions_url, endpoint='security_sessions', params={'telegram_user_id': str(telegram_user_id)})
        ok = bool(data.get('ok', True)) if isinstance(data, dict) else False
        return SecurityActionResult(ok=ok, message=str(data.get('message', '') if isinstance(data, dict) else ''), raw=data if isinstance(data, dict) else {'payload': data})

    async def act_security_session(self, *, telegram_user_id: int, action: str, session_id: str = '', scope: str = '') -> SecurityActionResult:
        if not self.security_session_action_url:
            return SecurityActionResult(ok=False, message='security_session_action_url_not_configured')
        payload = {'schema_version': _EXPECTED_SCHEMA, 'telegram_user_id': telegram_user_id, 'action': action, 'session_id': session_id, 'scope': scope}
        data = await self._request_json_with_retries('POST', self.security_session_action_url, endpoint='security_session_action', json_body=payload)
        ok = bool(data.get('ok', True)) if isinstance(data, dict) else False
        return SecurityActionResult(ok=ok, message=str(data.get('message', '') if isinstance(data, dict) else ''), raw=data if isinstance(data, dict) else {'payload': data})

    async def check_compatibility(self) -> CompatibilityResult:
        if not self.security_status_url:
            return CompatibilityResult(ok=True, api_version='unknown', message='security_status_url_not_configured', raw={})
        try:
            data = await self._request_json_with_retries('GET', self.security_status_url, endpoint='security_status', params={'probe': 'compatibility'})
        except Exception as exc:
            return CompatibilityResult(ok=False, api_version='unknown', message=str(exc), raw={'reason': str(exc)})
        api_version = str(data.get('api_version') or data.get('schema_version') or 'unknown') if isinstance(data, dict) else 'unknown'
        ok = api_version in {'unknown', _EXPECTED_SCHEMA, '1'}
        return CompatibilityResult(ok=ok, api_version=api_version, message=str(data.get('message') or '') if isinstance(data, dict) else '', raw=data if isinstance(data, dict) else {'payload': data})

    async def run_periodic_refresh(self, interval_seconds: float, stop_event: asyncio.Event) -> None:
        if interval_seconds <= 0 or not self.status_url:
            return
        while not stop_event.is_set():
            try:
                await self.fetch_status(force=True)
            except Exception:
                logger.exception('Background status refresh failed')
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except TimeoutError:
                continue

    async def _request_json_with_retries(self, method: str, url: str, *, endpoint: str, json_body: dict[str, Any] | None = None, params: dict[str, str] | None = None) -> Any:
        breaker = self._breaker(endpoint)
        if not breaker.can_request():
            raise RuntimeError(f'circuit_open:{endpoint}')
        attempts = max(self.retry_attempts + 1, 1)
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                response = await self._client.request(method, url, params=params, headers=self._build_headers(url, json_body=json_body), json=json_body)
                response.raise_for_status()
                breaker.success()
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
                last_exc = exc
                breaker.failure()
                logger.warning('Failed %s request to %s [attempt=%s/%s]: %s', endpoint, url, attempt, attempts, exc)
                if attempt < attempts and breaker.can_request():
                    await asyncio.sleep(max(self.retry_backoff_seconds, 0.0) * attempt)
        assert last_exc is not None
        raise last_exc

    def _build_headers(self, url: str, *, json_body: dict[str, Any] | None = None) -> dict[str, str]:
        headers = {'Accept': 'application/json', self.request_id_header: str(uuid.uuid4()), 'X-Schema-Version': _EXPECTED_SCHEMA}
        if self.bearer_token:
            headers['Authorization'] = f'Bearer {self.bearer_token}'
        if self.hmac_secret:
            body = '' if json_body is None else json.dumps(json_body, ensure_ascii=False, sort_keys=True)
            path = urlparse(url).path or '/'
            timestamp = str(int(time.time()))
            nonce = str(uuid.uuid4())
            key_id = (os.getenv('OUTBOUND_KEY_ID', 'v1').strip() or 'v1')
            payload = f'{path}\n{timestamp}\n{nonce}\n{body}'.encode('utf-8')
            headers['X-Key-Id'] = key_id
            headers['X-Timestamp'] = timestamp
            headers['X-Nonce'] = nonce
            headers['X-Signature'] = hmac.new(self.hmac_secret.encode('utf-8'), payload, hashlib.sha256).hexdigest()
        return headers

    def _parse_status(self, data: Any) -> ServerStatus:
        if isinstance(data, dict):
            if self.strict_schemas and data.get('schema_version') not in {None, _EXPECTED_SCHEMA}:
                raise ValueError('status schema mismatch')
            status = data.get('status') if isinstance(data.get('status'), dict) else data
            return ServerStatus(
                ok=bool(status.get('ok', status.get('online', False))),
                server_name=str(status.get('server_name') or status.get('name') or 'NeverMine'),
                online=bool(status.get('online', False)),
                players_online=int(status.get('players_online')) if status.get('players_online') is not None else None,
                max_players=int(status.get('max_players')) if status.get('max_players') is not None else None,
                version=str(status.get('version') or ''),
                motd=str(status.get('motd') or ''),
                latency_ms=int(status.get('latency_ms')) if status.get('latency_ms') is not None else None,
                raw=status,
            )
        raise ValueError('invalid status payload')

    def _parse_feed(self, data: Any) -> list[FeedAnnouncement]:
        if isinstance(data, dict):
            if self.strict_schemas and data.get('schema_version') not in {None, _EXPECTED_SCHEMA}:
                raise ValueError('feed schema mismatch')
            items = data.get('items', [])
        elif isinstance(data, list):
            items = data
        else:
            raise ValueError('invalid feed payload')
        announcements: list[FeedAnnouncement] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            announcements.append(FeedAnnouncement(
                event_id=str(item.get('event_id') or item.get('id') or ''),
                text=str(item.get('text') or item.get('message') or ''),
                tag=str(item.get('tag') or ''),
                created_at=str(item.get('created_at') or ''),
                media_kind=str(item.get('media_kind') or ''),
                media_ref=str(item.get('media_ref') or item.get('media_url') or ''),
                buttons=item.get('buttons') if isinstance(item.get('buttons'), list) else None,
                priority=int(item.get('priority') or 0),
                silent=bool(item.get('silent') or False),
                parse_mode=str(item.get('parse_mode') or ''),
                shard=str(item.get('shard') or item.get('server') or ''),
            ))
        return [item for item in announcements if item.event_id and item.text]

    def _parse_link_verify(self, data: Any) -> LinkVerificationResult:
        if isinstance(data, dict):
            if self.strict_schemas and data.get('schema_version') not in {None, _EXPECTED_SCHEMA}:
                raise ValueError('link_verify schema mismatch')
            return LinkVerificationResult(
                ok=bool(data.get('ok', False)),
                player_uuid=str(data.get('player_uuid') or ''),
                player_name=str(data.get('player_name') or ''),
                raw=data,
            )
        raise ValueError('invalid link verify payload')


async def push_external_event(url: str, payload: dict[str, Any], *, bearer_token: str = '', hmac_secret: str = '', key_id: str = 'v1', timeout_seconds: float = 5.0) -> bool:
    url = (url or '').strip()
    if not url:
        return False
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    if bearer_token:
        headers['Authorization'] = f'Bearer {bearer_token}'
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True)
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
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        response = await client.post(url, headers=headers, content=body.encode('utf-8'))
        response.raise_for_status()
        return True
