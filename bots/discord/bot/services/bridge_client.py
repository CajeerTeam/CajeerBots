from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from typing import Any, Mapping
from urllib.parse import urlparse

import aiohttp


async def push_external_event(
    session: aiohttp.ClientSession,
    url: str,
    payload: dict[str, Any],
    *,
    bearer_token: str = '',
    hmac_secret: str = '',
    key_id: str = 'v1',
    timeout_seconds: float = 5.0,
) -> bool:
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
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    async with session.post(url, headers=headers, data=body.encode('utf-8'), timeout=timeout) as response:
        response.raise_for_status()
        return True


def verify_signed_request(*, path: str, raw_body: bytes, headers: dict[str, str], hmac_secret: str | Mapping[str, str], max_skew_seconds: int = 300) -> tuple[bool, str]:
    if isinstance(hmac_secret, Mapping):
        available = {str(k): str(v) for k, v in hmac_secret.items() if str(v).strip()}
        if not available:
            return True, 'unsigned-allowed'
    else:
        secret = str(hmac_secret or '').strip()
        if not secret:
            return True, 'unsigned-allowed'
        available = {'default': secret}
    timestamp = (headers.get('X-Timestamp') or '').strip()
    nonce = (headers.get('X-Nonce') or '').strip()
    signature = (headers.get('X-Signature') or '').strip().lower()
    key_id = (headers.get('X-Key-Id') or 'default').strip() or 'default'
    if not timestamp or not nonce or not signature:
        return False, 'missing_signature_headers'
    try:
        ts_value = int(timestamp)
    except ValueError:
        return False, 'bad_timestamp'
    if abs(int(time.time()) - ts_value) > max_skew_seconds:
        return False, 'timestamp_skew'
    body = raw_body.decode('utf-8')
    payload = f'{path}\n{timestamp}\n{nonce}\n{body}'.encode('utf-8')
    candidates: list[tuple[str, str]] = []
    if key_id in available:
        candidates.append((key_id, available[key_id]))
    candidates.extend((name, secret) for name, secret in available.items() if name != key_id)
    for name, secret in candidates:
        expected = hmac.new(secret.encode('utf-8'), payload, hashlib.sha256).hexdigest().lower()
        if hmac.compare_digest(expected, signature):
            return True, nonce
    return False, 'bad_signature'
