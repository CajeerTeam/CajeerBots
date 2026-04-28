from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

_WEEKDAY_MAP = {
    'monday': 0,
    'tuesday': 1,
    'wednesday': 2,
    'thursday': 3,
    'friday': 4,
    'saturday': 5,
    'sunday': 6,
}


def _format_dt(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _parse_dt(value: str | None) -> datetime:
    raw = str(value or '').strip()
    if raw:
        for candidate in (raw, raw.replace('Z', '+00:00')):
            try:
                parsed = datetime.fromisoformat(candidate)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed.astimezone(timezone.utc)
            except ValueError:
                pass
        try:
            return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _safe_timezone(name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(str(name or 'Europe/Berlin').strip() or 'Europe/Berlin')
    except Exception:
        return ZoneInfo('UTC')


def _parse_hhmm(value: str | None) -> tuple[int, int]:
    raw = str(value or '').strip()
    if not raw:
        return 9, 0
    try:
        hour_raw, minute_raw = raw.split(':', 1)
        hour = max(0, min(23, int(hour_raw)))
        minute = max(0, min(59, int(minute_raw)))
        return hour, minute
    except Exception:
        return 9, 0


def build_scheduled_job_dedupe_key(*, job_type: str, guild_id: str, channel_id: str, run_at: str, payload: dict[str, Any]) -> str:
    base = {
        'job_type': str(job_type or ''),
        'guild_id': str(guild_id or ''),
        'channel_id': str(channel_id or ''),
        'run_at': str(run_at or ''),
        'payload_json': payload or {},
    }
    return hashlib.sha256(json.dumps(base, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()


def build_digest_schedule_payload(*, digest_kind: str = 'staff', recurrence_hours: int | None = None, remaining_occurrences: int | None = None, digest_scope: str = 'targeted') -> dict[str, Any]:
    payload: dict[str, Any] = {
        'kind': f'{str(digest_scope or "targeted").strip().lower() or "targeted"}_digest',
        'digest_kind': str(digest_kind or 'staff').strip().lower() or 'staff',
    }
    recurrence = int(recurrence_hours or 0)
    if recurrence > 0:
        payload['recurrence_hours'] = recurrence
    if remaining_occurrences is not None and int(remaining_occurrences) > 0:
        payload['remaining_occurrences'] = int(remaining_occurrences)
    return payload


def build_calendar_schedule_payload(*, digest_kind: str = 'staff', digest_scope: str = 'targeted', timezone_name: str = 'Europe/Berlin', local_time: str = '09:00', weekday: str | None = None, weekday_set: str | None = None, day_of_month: int | None = None, remaining_occurrences: int | None = None) -> dict[str, Any]:
    payload = build_digest_schedule_payload(digest_kind=digest_kind, digest_scope=digest_scope, recurrence_hours=None, remaining_occurrences=remaining_occurrences)
    payload['calendar_timezone'] = str(timezone_name or 'Europe/Berlin').strip() or 'Europe/Berlin'
    payload['calendar_time'] = str(local_time or '09:00').strip() or '09:00'
    if day_of_month is not None and int(day_of_month or 0) > 0:
        payload['calendar_mode'] = 'monthly'
        payload['calendar_day_of_month'] = max(1, min(28, int(day_of_month)))
    elif str(weekday_set or '').strip():
        payload['calendar_mode'] = 'weekly_set'
        payload['calendar_weekdays'] = [part.strip().lower() for part in str(weekday_set or '').split(',') if part.strip()]
    else:
        payload['calendar_mode'] = 'weekly' if str(weekday or '').strip() else 'daily'
    if weekday:
        payload['calendar_weekday'] = str(weekday).strip().lower()
    return payload


def recurrence_summary(*, recurrence_hours: int | None = None, remaining_occurrences: int | None = None, calendar_mode: str | None = None, calendar_time: str | None = None, calendar_weekday: str | None = None, timezone_name: str | None = None, calendar_weekdays: list[str] | None = None, calendar_day_of_month: int | None = None) -> str:
    if calendar_mode:
        tz_name = str(timezone_name or 'UTC').strip() or 'UTC'
        if calendar_mode == 'weekly':
            day = str(calendar_weekday or 'monday').strip().lower()
            base = f'каждую неделю ({day}) в {calendar_time or "09:00"} [{tz_name}]'
        elif calendar_mode == 'weekly_set':
            days = ', '.join(calendar_weekdays or [str(calendar_weekday or 'monday').strip().lower()])
            base = f'по дням недели ({days}) в {calendar_time or "09:00"} [{tz_name}]'
        elif calendar_mode == 'monthly':
            base = f'ежемесячно {int(calendar_day_of_month or 1)} числа в {calendar_time or "09:00"} [{tz_name}]'
        else:
            base = f'ежедневно в {calendar_time or "09:00"} [{tz_name}]'
        count = int(remaining_occurrences or 0)
        if count > 0:
            return f'{base}, ещё {count} запуск(ов)'
        return base
    recurrence = int(recurrence_hours or 0)
    if recurrence <= 0:
        return 'без повторов'
    count = int(remaining_occurrences or 0)
    if count > 0:
        return f'повтор каждые {recurrence} ч., ещё {count} запуск(ов)'
    return f'повтор каждые {recurrence} ч. без лимита'


def _next_calendar_run_from(*, base_dt: datetime, payload: dict[str, Any]) -> datetime | None:
    mode = str(payload.get('calendar_mode') or '').strip().lower()
    if mode not in {'daily', 'weekly', 'weekly_set', 'monthly'}:
        return None
    tz = _safe_timezone(payload.get('calendar_timezone'))
    hour, minute = _parse_hhmm(payload.get('calendar_time'))
    local_base = base_dt.astimezone(tz)
    candidate = local_base.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if mode == 'daily':
        if candidate <= local_base:
            candidate += timedelta(days=1)
        return candidate.astimezone(timezone.utc)
    if mode == 'weekly_set':
        raw_days = payload.get('calendar_weekdays') if isinstance(payload.get('calendar_weekdays'), list) else []
        weekdays = sorted({_WEEKDAY_MAP.get(str(day).strip().lower()) for day in raw_days if _WEEKDAY_MAP.get(str(day).strip().lower()) is not None})
        if not weekdays:
            weekdays = [local_base.weekday()]
        for offset in range(0, 8):
            day_candidate = (local_base + timedelta(days=offset)).replace(hour=hour, minute=minute, second=0, microsecond=0)
            if day_candidate.weekday() in weekdays and day_candidate > local_base:
                return day_candidate.astimezone(timezone.utc)
        return (candidate + timedelta(days=7)).astimezone(timezone.utc)
    if mode == 'monthly':
        target_day = max(1, min(28, int(payload.get('calendar_day_of_month') or 1)))
        month_candidate = local_base.replace(day=min(target_day, 28), hour=hour, minute=minute, second=0, microsecond=0)
        if month_candidate <= local_base:
            if month_candidate.month == 12:
                month_candidate = month_candidate.replace(year=month_candidate.year + 1, month=1, day=target_day)
            else:
                month_candidate = month_candidate.replace(month=month_candidate.month + 1, day=target_day)
        return month_candidate.astimezone(timezone.utc)
    weekday = _WEEKDAY_MAP.get(str(payload.get('calendar_weekday') or '').strip().lower())
    if weekday is None:
        weekday = local_base.weekday()
    delta_days = (weekday - local_base.weekday()) % 7
    candidate = candidate + timedelta(days=delta_days)
    if candidate <= local_base:
        candidate += timedelta(days=7)
    return candidate.astimezone(timezone.utc)


def next_recurring_schedule(*, job_type: str, payload: dict[str, Any], current_run_at: str, guild_id: str, channel_id: str) -> tuple[str, dict[str, Any], str] | None:
    remaining_raw = payload.get('remaining_occurrences')
    next_payload = dict(payload or {})
    if remaining_raw is not None:
        remaining = int(remaining_raw or 0)
        if remaining <= 1:
            return None
        next_payload['remaining_occurrences'] = remaining - 1
    next_payload['recurrence_iteration'] = int(payload.get('recurrence_iteration') or 0) + 1
    current_dt = _parse_dt(current_run_at)
    next_run_dt = _next_calendar_run_from(base_dt=current_dt, payload=payload)
    if next_run_dt is None:
        recurrence_hours = int(payload.get('recurrence_hours') or 0)
        if recurrence_hours <= 0:
            return None
        next_run_dt = current_dt + timedelta(hours=recurrence_hours)
    next_run_at = _format_dt(next_run_dt)
    dedupe_key = build_scheduled_job_dedupe_key(job_type=job_type, guild_id=guild_id, channel_id=channel_id, run_at=next_run_at, payload=next_payload)
    return next_run_at, next_payload, dedupe_key


def first_calendar_run_at(*, now: datetime | None = None, payload: dict[str, Any]) -> str:
    base_dt = now or datetime.now(timezone.utc)
    run_dt = _next_calendar_run_from(base_dt=base_dt - timedelta(minutes=1), payload=payload) or base_dt
    return _format_dt(run_dt)
