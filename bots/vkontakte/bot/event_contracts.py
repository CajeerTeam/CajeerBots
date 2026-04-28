from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

EVENT_CONTRACT_VERSION = "5"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fmt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_dt(raw: Any) -> datetime | None:
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value).astimezone(timezone.utc)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(str(raw).strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def build_transport_event(*, event_type: str, payload: dict[str, Any], source: str = "vk-bridge", ttl_seconds: int = 300) -> dict[str, Any]:
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


def validate_transport_event(payload: dict[str, Any], *, max_future_skew_seconds: int = 300) -> list[str]:
    errors: list[str] = []
    required = {"schema_version", "event_id", "event_type", "source", "issued_at", "expires_at", "payload"}
    missing = sorted(required - set(payload))
    if missing:
        errors.extend([f"missing:{key}" for key in missing])
    if str(payload.get("schema_version") or "") not in {EVENT_CONTRACT_VERSION, "3", "2", "1"}:
        errors.append("bad_schema_version")
    if not str(payload.get("event_id") or "").strip():
        errors.append("bad_event_id")
    if not str(payload.get("event_type") or "").strip():
        errors.append("bad_event_type")
    if not str(payload.get("source") or "").strip():
        errors.append("bad_source")
    if not isinstance(payload.get("payload", {}), dict):
        errors.append("payload_not_object")

    issued_at = _parse_dt(payload.get("issued_at"))
    expires_at = _parse_dt(payload.get("expires_at"))
    if issued_at is None:
        errors.append("bad_issued_at")
    if expires_at is None:
        errors.append("bad_expires_at")
    if issued_at is not None and expires_at is not None and expires_at <= issued_at:
        errors.append("bad_expiry_window")

    now = _utc_now()
    if issued_at is not None and issued_at - now > timedelta(seconds=max_future_skew_seconds):
        errors.append("issued_at_in_future")
    if expires_at is not None and expires_at <= now:
        errors.append("event_expired")
    return errors


def parse_expires_at_epoch(payload: dict[str, Any]) -> int | None:
    expires_at = _parse_dt(payload.get("expires_at"))
    return int(expires_at.timestamp()) if expires_at is not None else None
