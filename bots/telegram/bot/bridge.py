from __future__ import annotations

import hashlib
import hmac
import html
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Mapping

from telegram.ext import Application

LOGGER = logging.getLogger(__name__)


def _header(headers: Mapping[str, str], name: str) -> str:
    lower = name.lower()
    for key, value in headers.items():
        if key.lower() == lower:
            return str(value)
    return ""


def verify_signed_request(
    *,
    path: str,
    raw_body: bytes,
    headers: Mapping[str, str],
    hmac_secret: str | Mapping[str, str],
    max_skew_seconds: int = 300,
) -> tuple[bool, str]:
    """Verify the HMAC format used by NMDiscordBot bridge_client.push_external_event."""

    if isinstance(hmac_secret, Mapping):
        available = {str(key): str(value) for key, value in hmac_secret.items() if str(value).strip()}
        if not available:
            return True, "unsigned-allowed"
    else:
        secret = str(hmac_secret or "").strip()
        if not secret:
            return True, "unsigned-allowed"
        available = {"default": secret}

    timestamp = _header(headers, "X-Timestamp").strip()
    nonce = _header(headers, "X-Nonce").strip()
    signature = _header(headers, "X-Signature").strip().lower()
    key_id = _header(headers, "X-Key-Id").strip() or "default"

    if not timestamp or not nonce or not signature:
        return False, "missing_signature_headers"
    try:
        ts_value = int(timestamp)
    except ValueError:
        return False, "bad_timestamp"
    if abs(int(time.time()) - ts_value) > max_skew_seconds:
        return False, "timestamp_skew"

    body = raw_body.decode("utf-8")
    sign_payload = f"{path}\n{timestamp}\n{nonce}\n{body}".encode("utf-8")

    candidates: list[tuple[str, str]] = []
    if key_id in available:
        candidates.append((key_id, available[key_id]))
    candidates.extend((name, secret) for name, secret in available.items() if name != key_id)

    for _name, secret in candidates:
        expected = hmac.new(secret.encode("utf-8"), sign_payload, hashlib.sha256).hexdigest().lower()
        if hmac.compare_digest(expected, signature):
            return True, nonce
    return False, "bad_signature"


def bridge_auth_ok(cfg: Any, *, path: str, raw_body: bytes, headers: Mapping[str, str]) -> tuple[bool, str]:
    bearer = str(getattr(cfg, "bridge_inbound_bearer_token", "") or "").strip()
    if bearer:
        auth = _header(headers, "Authorization").strip()
        if auth != f"Bearer {bearer}":
            return False, "bad_bearer"

    hmac_secret = str(getattr(cfg, "bridge_inbound_hmac_secret", "") or "").strip()
    strict = bool(getattr(cfg, "bridge_ingress_strict_auth", True))
    if strict and not bearer and not hmac_secret:
        return False, "bridge_auth_not_configured"

    ok, reason = verify_signed_request(path=path, raw_body=raw_body, headers=headers, hmac_secret=hmac_secret)
    if not ok and strict:
        return False, reason
    return True, reason


def _event_label(event_type: str, semantic_kind: str = "") -> str:
    key = semantic_kind or event_type
    labels = {
        "announcement_created": "📣 Анонс",
        "community.announcement.created": "📣 Анонс",
        "community.announcement.updated": "📣 Обновление анонса",
        "community.devlog.created": "🛠 Devlog",
        "community.devlog.updated": "🛠 Devlog обновлён",
        "event_created": "📅 Событие",
        "community.event.created": "📅 Событие",
        "community.event.reminder": "⏰ Напоминание",
        "stage_announcement": "🎤 Stage",
        "community.stage.announcement": "🎤 Stage",
        "community.world_signal.created": "🌌 Сигнал мира",
        "support_created": "🆘 Поддержка",
        "community.support.created": "🆘 Поддержка",
        "bug_created": "🐞 Баг-репорт",
        "community.bug_report.created": "🐞 Баг-репорт",
        "suggestion_created": "💡 Предложение",
        "community.suggestion.created": "💡 Предложение",
        "report_created": "🚨 Репорт",
        "community.report.created": "🚨 Репорт",
        "appeal_created": "⚖️ Апелляция",
        "community.appeal.created": "⚖️ Апелляция",
        "guild_recruitment_created": "🛡 Набор в гильдию",
        "community.guild_recruitment.created": "🛡 Набор в гильдию",
        "identity_linked": "🔗 Привязка аккаунта",
        "identity_unlinked": "⛓️‍💥 Отвязка аккаунта",
    }
    return labels.get(key, "NeverMine")


def render_discord_event(envelope: dict[str, Any]) -> str:
    event_type = str(envelope.get("event_type") or "unknown")
    payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
    semantic_kind = str(payload.get("semantic_kind") or "")
    label = _event_label(event_type, semantic_kind)

    title = str(payload.get("title") or payload.get("name") or label).strip()
    body = str(
        payload.get("text")
        or payload.get("body")
        or payload.get("description")
        or payload.get("details")
        or payload.get("comment")
        or ""
    ).strip()
    url = str(payload.get("url") or payload.get("jump_url") or payload.get("discord_url") or "").strip()

    lines = [f"<b>{html.escape(label)}</b>"]
    if title and title != label:
        lines.append(f"<b>{html.escape(title[:256])}</b>")
    if body:
        lines.extend(["", html.escape(body[:3200])])
    if url:
        lines.extend(["", html.escape(url[:900])])

    source = str(envelope.get("source") or "discord").strip()
    event_id = str(envelope.get("event_id") or "").strip()
    footer = f"source={source}"
    if event_id:
        footer += f" event={event_id[:64]}"
    lines.extend(["", f"<code>{html.escape(footer)}</code>"])
    return "\n".join(lines)[:4096]


def _event_tag(envelope: dict[str, Any]) -> str:
    payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else {}
    tag = str(payload.get("tag") or payload.get("semantic_kind") or "").strip()
    if tag:
        return tag
    event_type = str(envelope.get("event_type") or "")
    if "devlog" in event_type:
        return "devlogs"
    if "event" in event_type or "stage" in event_type:
        return "events"
    if "guild" in event_type:
        return "guilds"
    if "support" in event_type or "bug" in event_type or "suggestion" in event_type:
        return "support"
    return "news"


def _target_chats(application: Application, envelope: dict[str, Any]) -> list[int]:
    cfg = application.bot_data["config"]
    db = application.bot_data["db"]
    explicit = set(getattr(cfg, "bridge_target_chat_ids", set()) or set())
    allowed = explicit or set(getattr(cfg, "telegram_allowed_chat_ids", set()) or set())
    if not allowed:
        return []

    tags = list(getattr(cfg, "bridge_target_tags", set()) or set())
    tag = _event_tag(envelope)
    if not tags and tag:
        tags = [tag]

    try:
        return db.resolve_target_chats(
            allowed_chat_ids=allowed,
            fallback_chat_id=None,
            target_scope=str(getattr(cfg, "bridge_target_scope", "all") or "all"),
            target_tags=tags,
            feature="announcements",
        )
    except TypeError:
        return sorted(allowed)


def _allowed_event(cfg: Any, event_type: str) -> bool:
    allowed = set(getattr(cfg, "bridge_allowed_event_types", set()) or set())
    if not allowed:
        return True
    return event_type in allowed or "*" in allowed


async def handle_discord_bridge_event(application: Application, envelope: dict[str, Any]) -> dict[str, Any]:
    cfg = application.bot_data["config"]
    event_type = str(envelope.get("event_type") or "").strip()
    if not _allowed_event(cfg, event_type):
        return {"ok": True, "dropped": True, "reason": "event_type_not_allowed"}

    targets = _target_chats(application, envelope)
    if not targets:
        return {"ok": False, "error": "no_bridge_target_chats"}

    text = render_discord_event(envelope)
    sent = 0
    errors: list[str] = []

    for chat_id in targets:
        try:
            await application.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=getattr(cfg, "telegram_parse_mode", "HTML"),
                disable_web_page_preview=False,
            )
            sent += 1
        except Exception as exc:
            LOGGER.warning("failed to deliver Discord bridge event to Telegram chat %s: %s", chat_id, exc)
            errors.append(f"{chat_id}:{type(exc).__name__}")

    db = application.bot_data.get("db")
    if db is not None and hasattr(db, "queue_external_sync_event"):
        try:
            event_id = db.queue_external_sync_event(event_kind=event_type or "discord.bridge.event", destination="telegram-local", payload=envelope)
            if hasattr(db, "mark_external_sync_event"):
                db.mark_external_sync_event(event_id, status="sent" if sent else "retry", error=";".join(errors))
        except Exception:
            LOGGER.debug("failed to record local Telegram bridge delivery", exc_info=True)

    return {
        "ok": sent > 0,
        "sent": sent,
        "targets": len(targets),
        "errors": errors[:5],
    }
