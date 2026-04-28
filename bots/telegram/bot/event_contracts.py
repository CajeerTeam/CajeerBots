from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any

EVENT_CONTRACT_VERSION = '5'
EXTERNAL_ADMIN_CONTRACT_VERSION = 2
_REQUIRED_EVENT_KEYS = {'schema_version', 'event_id', 'event_type', 'source', 'issued_at', 'expires_at', 'payload'}
_REQUIRED_ADMIN_KEYS = _REQUIRED_EVENT_KEYS | {'idempotency_key', 'contract'}


def validate_transport_event(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    missing = sorted(_REQUIRED_EVENT_KEYS - set(payload))
    if missing:
        errors.extend([f'missing:{k}' for k in missing])
    if payload.get('schema_version') not in {EVENT_CONTRACT_VERSION, '3', '2', '1'}:
        errors.append('bad_schema_version')
    if not isinstance(payload.get('payload', {}), dict):
        errors.append('payload_not_object')
    return errors


def validate_admin_envelope(payload: dict[str, Any]) -> list[str]:
    errors = validate_transport_event(payload)
    missing = sorted(_REQUIRED_ADMIN_KEYS - set(payload))
    if missing:
        errors.extend([f'missing:{k}' for k in missing])
    contract = payload.get('contract') or {}
    if not isinstance(contract, dict):
        errors.append('bad_contract')
    else:
        if contract.get('kind') != 'external-admin':
            errors.append('bad_contract_kind')
        if int(contract.get('version') or 0) != EXTERNAL_ADMIN_CONTRACT_VERSION:
            errors.append('bad_contract_version')
    return errors


def build_transport_event(*, event_type: str, payload: dict[str, Any], source: str = 'telegram', ttl_seconds: int = 300) -> dict[str, Any]:
    now = datetime.utcnow()
    return {
        'schema_version': EVENT_CONTRACT_VERSION,
        'event_id': str(uuid.uuid4()),
        'event_type': event_type,
        'source': source,
        'issued_at': now.strftime('%Y-%m-%d %H:%M:%S'),
        'expires_at': (now + timedelta(seconds=max(30, ttl_seconds))).strftime('%Y-%m-%d %H:%M:%S'),
        'payload': payload,
    }


def normalize_admin_action(*, action: str, payload: dict[str, Any], actor_user_id: int = 0, ttl_seconds: int = 300) -> dict[str, Any]:
    env = build_transport_event(event_type=f'admin.{action}', payload=payload, source='telegram-admin', ttl_seconds=ttl_seconds)
    env['idempotency_key'] = env['event_id']
    env['actor_user_id'] = actor_user_id
    env['contract'] = {'kind': 'external-admin', 'version': EXTERNAL_ADMIN_CONTRACT_VERSION}
    env['ttl_seconds'] = max(30, ttl_seconds)
    return env


def build_signed_response(*, action: str, ok: bool, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        'schema_version': EVENT_CONTRACT_VERSION,
        'kind': 'external-admin-response',
        'version': EXTERNAL_ADMIN_CONTRACT_VERSION,
        'action': action,
        'ok': bool(ok),
        'payload': payload or {},
        'responded_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
    }
