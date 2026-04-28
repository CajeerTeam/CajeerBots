from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

EVENT_CONTRACT_VERSION = "5"
EXTERNAL_ADMIN_CONTRACT_VERSION = 2
_REQUIRED_EVENT_KEYS = {"schema_version", "event_id", "event_type", "source", "issued_at", "expires_at", "payload"}
_REQUIRED_ADMIN_KEYS = _REQUIRED_EVENT_KEYS | {"idempotency_key", "contract"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fmt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _require_string(payload: dict[str, Any], key: str, errors: list[str], *, max_length: int | None = None) -> None:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        errors.append(f'payload.{key}_required')
        return
    if max_length is not None and len(value.strip()) > max_length:
        errors.append(f'payload.{key}_too_long')


def _require_optional_string(payload: dict[str, Any], key: str, errors: list[str], *, max_length: int | None = None) -> None:
    value = payload.get(key)
    if value in {None, ''}:
        return
    if not isinstance(value, str):
        errors.append(f'payload.{key}_not_string')
        return
    if max_length is not None and len(value) > max_length:
        errors.append(f'payload.{key}_too_long')


def _require_mapping(payload: dict[str, Any], key: str, errors: list[str]) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        errors.append(f'payload.{key}_not_object')
        return {}
    return value


def _validate_identity_payload(payload: dict[str, Any], errors: list[str]) -> None:
    _require_string(payload, 'minecraft_uuid', errors, max_length=128)
    _require_optional_string(payload, 'minecraft_username', errors, max_length=64)


def _validate_discussion_created_payload(payload: dict[str, Any], errors: list[str]) -> None:
    _require_string(payload, 'title', errors, max_length=250)
    has_body = isinstance(payload.get('body'), str) and bool(str(payload.get('body') or '').strip())
    has_text = isinstance(payload.get('text'), str) and bool(str(payload.get('text') or '').strip())
    has_description = isinstance(payload.get('description'), str) and bool(str(payload.get('description') or '').strip())
    if not (has_body or has_text or has_description):
        errors.append('payload.body_or_text_required')
    _require_optional_string(payload, 'actor_user_id', errors, max_length=64)
    _require_optional_string(payload, 'thread_id', errors, max_length=64)


def _validate_discussion_lifecycle_payload(payload: dict[str, Any], errors: list[str]) -> None:
    _require_string(payload, 'thread_id', errors, max_length=64)
    _require_optional_string(payload, 'status', errors, max_length=64)
    _require_optional_string(payload, 'actor_user_id', errors, max_length=64)
    _require_optional_string(payload, 'staff_owner_user_id', errors, max_length=64)
    _require_optional_string(payload, 'comment', errors, max_length=4000)
    _require_optional_string(payload, 'message_id', errors, max_length=64)
    _require_optional_string(payload, 'external_comment_id', errors, max_length=64)
    _require_optional_string(payload, 'source_platform', errors, max_length=32)




def _validate_discussion_update_payload(payload: dict[str, Any], errors: list[str]) -> None:
    _require_string(payload, 'thread_id', errors, max_length=64)
    has_title = isinstance(payload.get('title'), str) and bool(str(payload.get('title') or '').strip())
    has_body = isinstance(payload.get('body'), str) and bool(str(payload.get('body') or '').strip())
    has_text = isinstance(payload.get('text'), str) and bool(str(payload.get('text') or '').strip())
    has_description = isinstance(payload.get('description'), str) and bool(str(payload.get('description') or '').strip())
    if not (has_title or has_body or has_text or has_description):
        errors.append('payload.title_or_body_required')
    _require_optional_string(payload, 'title', errors, max_length=250)
    _require_optional_string(payload, 'body', errors, max_length=4000)
    _require_optional_string(payload, 'text', errors, max_length=4000)
    _require_optional_string(payload, 'description', errors, max_length=4000)
    _require_optional_string(payload, 'actor_user_id', errors, max_length=64)
    _require_optional_string(payload, 'external_topic_id', errors, max_length=128)

def _validate_world_signal_payload(payload: dict[str, Any], errors: list[str]) -> None:
    _require_string(payload, 'title', errors, max_length=256)
    has_body = isinstance(payload.get('body'), str) and bool(str(payload.get('body') or '').strip())
    has_text = isinstance(payload.get('text'), str) and bool(str(payload.get('text') or '').strip())
    if not (has_body or has_text):
        errors.append('payload.body_or_text_required')


def _validate_admin_payload(payload: dict[str, Any], errors: list[str]) -> None:
    _require_optional_string(payload, 'kind', errors, max_length=120)


def _validate_report_created_payload(payload: dict[str, Any], errors: list[str]) -> None:
    has_body = isinstance(payload.get('details'), str) and bool(str(payload.get('details') or '').strip())
    has_text = isinstance(payload.get('body'), str) and bool(str(payload.get('body') or '').strip())
    if not (has_body or has_text):
        errors.append('payload.details_required')
    _require_optional_string(payload, 'reporter_id', errors, max_length=64)
    _require_optional_string(payload, 'target_id', errors, max_length=64)
    _require_optional_string(payload, 'thread_id', errors, max_length=64)
    _require_optional_string(payload, 'external_topic_id', errors, max_length=128)


def _validate_content_payload(payload: dict[str, Any], errors: list[str]) -> None:
    _require_string(payload, 'title', errors, max_length=256)
    has_text = isinstance(payload.get('text'), str) and bool(str(payload.get('text') or '').strip())
    has_description = isinstance(payload.get('description'), str) and bool(str(payload.get('description') or '').strip())
    if not (has_text or has_description):
        errors.append('payload.text_or_description_required')
    _require_optional_string(payload, 'url', errors, max_length=1024)
    _require_optional_string(payload, 'external_message_id', errors, max_length=128)


def _validate_content_delete_payload(payload: dict[str, Any], errors: list[str]) -> None:
    if not any(isinstance(payload.get(key), str) and str(payload.get(key) or '').strip() for key in ('external_message_id', 'announcement_id', 'devlog_id', 'message_id', 'id')):
        errors.append('payload.external_message_id_required')


def _validate_unlink_payload(payload: dict[str, Any], errors: list[str]) -> None:
    if not any(isinstance(payload.get(key), str) and str(payload.get(key) or '').strip() for key in ('telegram_user_id', 'vk_user_id', 'workspace_actor_id', 'workspace_user_id', 'discord_user_id')):
        errors.append('payload.platform_user_id_required')


PAYLOAD_VALIDATORS: dict[str, Callable[[dict[str, Any], list[str]], None]] = {
    'identity.telegram.linked': _validate_identity_payload,
    'identity.vk.linked': _validate_identity_payload,
    'identity.workspace.linked': _validate_identity_payload,
    'identity.telegram.unlinked': _validate_unlink_payload,
    'identity.vk.unlinked': _validate_unlink_payload,
    'identity.workspace.unlinked': _validate_unlink_payload,
    'identity.sync': _validate_identity_payload,
    'community.support.created': _validate_discussion_created_payload,
    'community.bug_report.created': _validate_discussion_created_payload,
    'community.suggestion.created': _validate_discussion_created_payload,
    'community.appeal.created': _validate_discussion_created_payload,
    'community.guild_recruitment.created': _validate_discussion_created_payload,
    'community.chronicle.created': _validate_discussion_created_payload,
    'community.lore_discussion.created': _validate_discussion_created_payload,
    'community.report.created': _validate_report_created_payload,
    'community.world_signal.created': _validate_world_signal_payload,
    'community.announcement.created': _validate_content_payload,
    'community.announcement.updated': _validate_content_payload,
    'community.announcement.deleted': _validate_content_delete_payload,
    'community.devlog.created': _validate_content_payload,
    'community.devlog.updated': _validate_content_payload,
    'community.devlog.deleted': _validate_content_delete_payload,
    'admin.approval.create': _validate_admin_payload,
}

for _kind in (
    'community.support.updated',
    'community.bug_report.updated',
    'community.suggestion.updated',
    'community.appeal.updated',
    'community.guild_recruitment.updated',
    'community.chronicle.updated',
    'community.lore_discussion.updated',
    'community.report.updated',
):
    PAYLOAD_VALIDATORS[_kind] = _validate_discussion_update_payload

for _kind in (
    'community.support.closed',
    'community.support.reopened',
    'community.support.status_changed',
    'community.support.claimed',
    'community.support.unclaimed',
    'community.support.owner_changed',
    'community.support.comment.appended',
    'community.support.comment.edited',
    'community.support.comment.deleted',
    'community.bug_report.closed',
    'community.bug_report.reopened',
    'community.bug_report.status_changed',
    'community.bug_report.claimed',
    'community.bug_report.unclaimed',
    'community.bug_report.owner_changed',
    'community.bug_report.comment.appended',
    'community.bug_report.comment.edited',
    'community.bug_report.comment.deleted',
    'community.suggestion.closed',
    'community.suggestion.reopened',
    'community.suggestion.status_changed',
    'community.suggestion.comment.appended',
    'community.suggestion.comment.edited',
    'community.suggestion.comment.deleted',
    'community.appeal.closed',
    'community.appeal.reopened',
    'community.appeal.status_changed',
    'community.appeal.claimed',
    'community.appeal.unclaimed',
    'community.appeal.owner_changed',
    'community.appeal.comment.appended',
    'community.appeal.comment.edited',
    'community.appeal.comment.deleted',
    'community.guild_recruitment.closed',
    'community.guild_recruitment.reopened',
    'community.guild_recruitment.paused',
    'community.guild_recruitment.bumped',
    'community.guild_recruitment.status_changed',
    'community.guild_recruitment.comment.appended',
    'community.guild_recruitment.comment.edited',
    'community.guild_recruitment.comment.deleted',
    'community.chronicle.status_changed',
    'community.chronicle.comment.appended',
    'community.chronicle.comment.edited',
    'community.chronicle.comment.deleted',
    'community.lore_discussion.closed',
    'community.lore_discussion.reopened',
    'community.lore_discussion.status_changed',
    'community.lore_discussion.comment.appended',
    'community.lore_discussion.comment.edited',
    'community.lore_discussion.comment.deleted',
    'community.report.closed',
    'community.report.reopened',
    'community.report.status_changed',
    'community.report.claimed',
    'community.report.unclaimed',
    'community.report.owner_changed',
    'community.report.comment.appended',
    'community.report.comment.edited',
    'community.report.comment.deleted',
):
    PAYLOAD_VALIDATORS[_kind] = _validate_discussion_lifecycle_payload


def validate_transport_event(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    missing = sorted(_REQUIRED_EVENT_KEYS - set(payload))
    if missing:
        errors.extend([f"missing:{k}" for k in missing])
    if payload.get("schema_version") not in {EVENT_CONTRACT_VERSION, "3", "2", "1"}:
        errors.append("bad_schema_version")
    if not isinstance(payload.get("payload", {}), dict):
        errors.append("payload_not_object")
        return errors
    event_type = str(payload.get('event_type') or '').strip()
    if not event_type:
        errors.append('bad_event_type')
        return errors
    validator = PAYLOAD_VALIDATORS.get(event_type)
    if validator is not None:
        validator(payload.get('payload') if isinstance(payload.get('payload'), dict) else {}, errors)
    return errors


def validate_admin_envelope(payload: dict[str, Any]) -> list[str]:
    errors = validate_transport_event(payload)
    missing = sorted(_REQUIRED_ADMIN_KEYS - set(payload))
    if missing:
        errors.extend([f"missing:{k}" for k in missing])
    contract = payload.get("contract") or {}
    if not isinstance(contract, dict):
        errors.append("bad_contract")
    else:
        if contract.get("kind") != "external-admin":
            errors.append("bad_contract_kind")
        if int(contract.get("version") or 0) != EXTERNAL_ADMIN_CONTRACT_VERSION:
            errors.append("bad_contract_version")
    return errors


def build_transport_event(*, event_type: str, payload: dict[str, Any], source: str = "discord", ttl_seconds: int = 300) -> dict[str, Any]:
    now = _utc_now()
    return {
        "schema_version": EVENT_CONTRACT_VERSION,
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "source": source,
        "issued_at": _fmt(now),
        "expires_at": _fmt(now + timedelta(seconds=max(30, ttl_seconds))),
        "payload": payload,
    }


def normalize_admin_action(*, action: str, payload: dict[str, Any], actor_user_id: int = 0, ttl_seconds: int = 300) -> dict[str, Any]:
    env = build_transport_event(event_type=f"admin.{action}", payload=payload, source="discord-admin", ttl_seconds=ttl_seconds)
    env["idempotency_key"] = env["event_id"]
    env["actor_user_id"] = actor_user_id
    env["contract"] = {"kind": "external-admin", "version": EXTERNAL_ADMIN_CONTRACT_VERSION}
    env["ttl_seconds"] = max(30, ttl_seconds)
    return env


def build_signed_response(*, action: str, ok: bool, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "schema_version": EVENT_CONTRACT_VERSION,
        "kind": "external-admin-response",
        "version": EXTERNAL_ADMIN_CONTRACT_VERSION,
        "action": action,
        "ok": bool(ok),
        "payload": payload or {},
        "responded_at": _fmt(_utc_now()),
    }


def declared_transport_event_types() -> list[str]:
    return sorted(PAYLOAD_VALIDATORS.keys())
