from __future__ import annotations

import html
import json
import shlex
from datetime import datetime, timedelta
from urllib.parse import urlparse

from telegram import Update
from telegram.ext import ContextTypes

from nmbot import __version__
from nmbot.delivery import OutgoingPayload, parse_delivery_tokens, send_payload, send_payloads_bounded
from nmbot.handlers.ops import _ops_help_text
from nmbot.handlers.shared import (
    authorize,
    broadcast_confirmation_keyboard,
    deps,
    format_announcement_text,
    record,
    utc_now_iso,
    paginate_lines,
)
from nmbot.templates import feed_text


def _format_schedule_list(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "Запланированных broadcast задач нет."
    lines = ["<b>Scheduled broadcasts</b>"]
    for row in rows:
        lines.append(
            f"#{row['id']} | <b>{html.escape(str(row['status']))}</b> | <code>{html.escape(str(row['scheduled_for']))}</code> | "
            f"retry=<b>{row.get('retry_count', 0)}</b> | scope=<b>{html.escape(str(row['target_scope']))}</b> "
            f"tags=<code>{html.escape(str(row['target_tags'] or '-'))}</code>"
        )
    return "\n".join(lines)


def _format_dead_letters(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "Dead letters отсутствуют."
    lines = ["<b>Dead letters</b>"]
    for row in rows:
        lines.append(
            f"#{row['id']} | <b>{html.escape(str(row['source_type']))}</b> | status=<b>{html.escape(str(row['status']))}</b> | "
            f"retry=<b>{row.get('retry_count', 0)}</b> | error=<code>{html.escape(str(row.get('error') or ''))}</code>"
        )
    return "\n".join(lines)


def _database_target(cfg, db) -> str:
    backend = getattr(db, 'backend_name', 'sqlite')
    if backend == 'postgresql' and cfg.database_url:
        parsed = urlparse(cfg.database_url)
        host = parsed.hostname or 'localhost'
        port = parsed.port or 5432
        database = (parsed.path or '/').lstrip('/') or '-'
        return f'postgresql://{host}:{port}/{database}'
    return str(cfg.sqlite_path)


def _shell_split(raw: str) -> list[str]:
    try:
        return shlex.split(raw)
    except ValueError:
        return raw.split()


def _parse_schedule_control(raw: str) -> tuple[str | None, list[str]]:
    tokens = _shell_split(raw)
    if not tokens:
        return None, []
    return tokens[0], tokens[1:]

def _format_known_chats(rows) -> str:
    if not rows:
        return "Известных чатов нет."
    lines = ["<b>Known chats</b>"]
    for item in rows:
        lines.append(
            f"<code>{item.chat_id}</code> | <b>{html.escape(item.chat_type or '-')}</b> | {html.escape(item.title or '')} | "
            f"tags=<code>{html.escape(','.join(item.tags) or '-')}</code> | thread=<code>{item.default_thread_id or '-'}</code>"
        )
    return "\n".join(lines)


async def adminstats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command="adminstats", required_role="admin")
    build_manifest = context.application.bot_data.get('build_manifest', {}) or {}
    limiter = context.application.bot_data["rate_limiter"]
    limiter_diag = limiter.diagnostics() if hasattr(limiter, 'diagnostics') else {'backend_mode': getattr(limiter, 'backend_mode', 'sqlite')}
    stats = db.basic_stats()
    top = db.top_commands(7)
    top_text = ", ".join(f"/{name}={count}" for name, count in top) if top else "нет данных"
    health = db.db_health()
    lines = [
        "<b>NMTelegramBot admin stats</b>",
        f"Backend: <b>{html.escape(getattr(db, 'backend_name', 'sqlite'))}</b>",
        f"Database: <code>{html.escape(_database_target(cfg, db))}</code>",
        f"Errors total: <b>{stats['errors']}</b>",
        f"Rate-limit rejections: <b>{limiter.total_rejections}</b>",
        f"Rate-limit backend: <b>{html.escape(str(limiter_diag.get('backend_mode', getattr(limiter, 'backend_mode', 'sqlite'))))}</b>",
        f"Redis configured: <b>{html.escape(str(limiter_diag.get('redis_configured', False)))}</b>",
        f"Redis connected: <b>{html.escape(str(limiter_diag.get('redis_connected', False)))}</b>",
        f"Привязанных аккаунтов: <b>{db.count_linked_accounts()}</b>",
        f"Top commands: <code>{html.escape(top_text)}</code>",
        f"Feed sync total: <b>{html.escape(db.runtime_value('feed_sync_total', '0'))}</b>",
        f"Scheduled sent: <b>{html.escape(db.runtime_value('scheduled_sent_total', '0'))}</b>",
        f"Scheduled failed: <b>{html.escape(db.runtime_value('scheduled_failed_total', '0'))}</b>",
        f"Dead letters: <b>{health['dead_letters']}</b>",
        f"Feed backlog: <b>{health['feed_backlog']}</b>",
        f"Broadcast backlog: <b>{health.get('broadcast_backlog', 0)}</b>",
        f"Active locks: <b>{health['active_locks']}</b>",
        f"Storage mode: <b>{html.escape(db.runtime_value('storage_backend_mode', getattr(db, 'backend_name', 'sqlite')))}</b>",
        f"Rate-limit mode: <b>{html.escape(db.runtime_value('rate_limit_backend_mode', getattr(limiter, 'backend_mode', 'sqlite')))}</b>",
        f"Redis active: <b>{'yes' if limiter_diag.get('redis_connected', False) else 'no'}</b>",
        f"Last status OK: <code>{html.escape(db.runtime_value('last_status_ok_at', '-'))}</code>",
        f"Schema version: <b>{health['schema_version']}</b>",
        f"Build version: <b>{html.escape(str(build_manifest.get('version', '-')))}</b>",
        f"Build timestamp: <code>{html.escape(str(build_manifest.get('build', '-')))}</code>",
    ]
    await update.effective_message.reply_text("\n".join(lines), parse_mode=cfg.telegram_parse_mode)
    record(db, update, "adminstats", True, "shown")


async def diag_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command="diag", required_role="admin")
    health = db.db_health()
    fallback_root = '/tmp/nmtelegrambot'
    build_manifest = context.application.bot_data.get('build_manifest', {}) or {}
    limiter = context.application.bot_data.get('rate_limiter')
    limiter_diag = limiter.diagnostics() if hasattr(limiter, 'diagnostics') else {'backend_mode': getattr(limiter, 'backend_mode', 'sqlite')}
    lines = [
        "<b>NMTelegramBot diag</b>",
        f"Version: <b>{html.escape(__version__)}</b>",
        f"Mode: <b>{html.escape(cfg.bot_mode)}</b>",
        f"Backend: <b>{html.escape(getattr(db, 'backend_name', 'sqlite'))}</b>",
        f"Database: <code>{html.escape(_database_target(cfg, db))}</code>",
        f"Log path: <code>{html.escape(str(cfg.log_file))}</code>",
        f"Каталог шаблонов: <code>{html.escape(str(cfg.templates_dir))}</code>",
        f"Fallback runtime paths: <b>{'yes' if (str(cfg.artifact_root).startswith(fallback_root) or str(cfg.log_file).startswith(fallback_root) or str(cfg.templates_dir).startswith(fallback_root)) else 'no'}</b>",
        f"Webhook prefix: <code>{html.escape(cfg.webhook_path_prefix)}</code>",
        f"Instance ID: <code>{html.escape(cfg.instance_id)}</code>",
        f"Webhook URL: <code>{html.escape((cfg.webhook_url.rstrip('/') + '/' + cfg.webhook_path_prefix + '/' + cfg.telegram_bot_token.split(':', 1)[0]) if cfg.webhook_url else '-')}</code>",
        f"Health endpoint: <code>{html.escape(f'{cfg.health_http_listen}:{cfg.health_http_port}' if cfg.health_http_port else 'disabled')}</code>",
        f"Rate-limit backend: <b>{html.escape(str(limiter_diag.get('backend_mode', getattr(limiter, 'backend_mode', 'sqlite'))))}</b>",
        f"Redis configured: <b>{html.escape(str(limiter_diag.get('redis_configured', False)))}</b>",
        f"Redis connected: <b>{html.escape(str(limiter_diag.get('redis_connected', False)))}</b>",
        f"Last feed sync: <code>{html.escape(db.runtime_value('last_feed_sync_at', '-'))}</code>",
        f"Last feed error: <code>{html.escape(db.runtime_value('last_feed_error', '-'))}</code>",
        f"Last scheduled dispatch: <code>{html.escape(db.runtime_value('last_scheduled_dispatch_at', '-'))}</code>",
        f"Last scheduled error: <code>{html.escape(db.runtime_value('last_scheduled_error', '-'))}</code>",
        f"Last cleanup: <code>{html.escape(db.runtime_value('last_cleanup_at', '-'))}</code>",
        f"Last status OK: <code>{html.escape(db.runtime_value('last_status_ok_at', '-'))}</code>",
        f"Last status error: <code>{html.escape(db.runtime_value('last_status_error', '-'))}</code>",
        f"Schema version: <b>{health['schema_version']}</b>",
        f"Build version: <b>{html.escape(str(build_manifest.get('version', '-')))}</b>",
        f"Build timestamp: <code>{html.escape(str(build_manifest.get('build', '-')))}</code>",
        f"Compatibility: <code>{html.escape(json.dumps(build_manifest.get('compatibility', {}), ensure_ascii=False, sort_keys=True))}</code>",
        f"Secret sources: <code>{html.escape(json.dumps(cfg.secret_sources, ensure_ascii=False, sort_keys=True))}</code>",
        f"Last NMAuth compat: <code>{html.escape(db.runtime_value('last_nm_auth_compat', '-'))}</code>",
        f"Open alerts: <b>{health.get('open_operator_alerts', 0)}</b>",
        f"Dead letters: <b>{health['dead_letters']}</b>",
        f"Security pending: <b>{health.get('security_pending', 0)}</b>",
        f"Feed backlog: <b>{health['feed_backlog']}</b>",
        f"Broadcast backlog: <b>{health.get('broadcast_backlog', 0)}</b>",
        f"Active locks: <b>{health['active_locks']}</b>",
        f"Storage mode: <b>{html.escape(db.runtime_value('storage_backend_mode', getattr(db, 'backend_name', 'sqlite')))}</b>",
        f"Rate-limit mode: <b>{html.escape(db.runtime_value('rate_limit_backend_mode', getattr(limiter, 'backend_mode', 'sqlite')))}</b>",
        f"Redis active: <b>{'yes' if limiter_diag.get('redis_connected', False) else 'no'}</b>",
    ]
    await update.effective_message.reply_text("\n".join(lines), parse_mode=cfg.telegram_parse_mode)
    record(db, update, "diag", True, "shown")


async def health_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db, status_client = deps(context, with_status=True)
    await authorize(update, context, command="health", required_role="admin")
    status = await status_client.fetch_status()
    dbh = db.db_health()
    try:
        me = await context.bot.get_me()
        telegram_ok = True
        bot_name = me.username or me.first_name or "bot"
    except Exception:
        telegram_ok = False
        bot_name = "unknown"
    lines = [
        "<b>NMTelegramBot health</b>",
        f"Database: <code>{html.escape(_database_target(cfg, db))}</code>",
        f"Journal mode: <b>{html.escape(str(dbh['journal_mode']))}</b>",
        f"Schema version: <b>{dbh['schema_version']}</b>",
        f"Status URL configured: <b>{'yes' if status_client.is_configured() else 'no'}</b>",
        f"Feed URL configured: <b>{'yes' if status_client.feed_is_configured() else 'no'}</b>",
        f"Link verify URL configured: <b>{'yes' if status_client.link_verify_is_configured() else 'no'}</b>",
        f"Status reachable: <b>{'yes' if status.ok else 'no'}</b>",
        f"Telegram API reachable: <b>{'yes' if telegram_ok else 'no'}</b>",
        f"Bot identity: <b>{html.escape(bot_name)}</b>",
        f"Dead letters: <b>{dbh['dead_letters']}</b>",
        f"Scheduled backlog: <b>{dbh['scheduled_backlog']}</b>",
        f"Feed backlog: <b>{dbh['feed_backlog']}</b>",
        f"Active locks: <b>{dbh['active_locks']}</b>",
        f"Last feed sync: <code>{html.escape(db.runtime_value('last_feed_sync_at', '-'))}</code>",
        f"Last scheduled dispatch: <code>{html.escape(db.runtime_value('last_scheduled_dispatch_at', '-'))}</code>",
        f"Bot mode: <b>{html.escape(cfg.bot_mode)}</b>",
        f"Log format: <b>{html.escape(cfg.log_format)}</b>",
        f"Version: <b>{html.escape(__version__)}</b>",
    ]
    await update.effective_message.reply_text("\n".join(lines), parse_mode=cfg.telegram_parse_mode)
    record(db, update, "health", True, "ok")


async def announce_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    settings = await authorize(update, context, command="announce", required_role="admin")
    raw = " ".join(context.args).strip()
    options, text = parse_delivery_tokens(raw)
    if not text:
        await update.effective_message.reply_text("Использование: /announce [media=kind:url] [thread=id] -- <текст>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "announce", False, "empty_payload")
        return
    thread_id = options["message_thread_id"] or (settings.default_thread_id if settings else None)
    payload = OutgoingPayload(
        text=format_announcement_text(cfg, update.effective_user.full_name if update.effective_user else "NMTelegramBot", text, tag=",".join(options["tags"])),
        media_kind=options["media_kind"],
        media_ref=options["media_ref"],
        message_thread_id=thread_id,
        disable_notification=bool(options["disable_notification"] or (settings.disable_notifications if settings else False)),
        dry_run=bool(options.get('dry_run')) or (db.runtime_value('delivery:dry_run', '0') == '1'),
    )
    await send_payload(context.bot, chat_id=update.effective_chat.id, payload=payload, parse_mode=cfg.telegram_parse_mode, db=db)
    db.record_admin_action(chat_id=update.effective_chat.id, user_id=update.effective_user.id, action="announce", payload=text)
    push = context.application.bot_data.get('push_community_event')
    if callable(push):
        try:
            await push(
                context.application,
                event_kind='community.announcement.created',
                payload={
                    'title': 'Анонс из Telegram',
                    'text': text,
                    'actor_user_id': str(update.effective_user.id if update.effective_user else ''),
                    'actor_name': update.effective_user.full_name if update.effective_user else 'NMTelegramBot',
                    'source_platform': 'telegram',
                    'external_message_id': str(update.effective_message.message_id if update.effective_message else ''),
                },
            )
            db.record_admin_action(chat_id=update.effective_chat.id, user_id=update.effective_user.id, action="telegram_announce_bridge_push", payload=text[:500])
        except Exception as exc:
            db.record_admin_action(chat_id=update.effective_chat.id, user_id=update.effective_user.id, action="telegram_announce_bridge_push_failed", payload=str(exc))
    record(db, update, "announce", True, "sent_current_chat")


async def broadcast_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    settings = await authorize(update, context, command="broadcast", required_role="admin")
    user_id = update.effective_user.id
    raw = " ".join(context.args).strip()
    if raw.lower() == "confirm":
        await confirm_broadcast(update, context)
        return
    if raw.lower() == "cancel":
        db.clear_pending_broadcast(user_id=user_id)
        await update.effective_message.reply_text("Ожидающий broadcast отменён.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "broadcast", True, "cancelled")
        return
    if not raw:
        await update.effective_message.reply_text(
            "Использование: /broadcast [scope=...] [tags=..] [media=kind:url] [thread=id] [silent=true] -- <текст>",
            parse_mode=cfg.telegram_parse_mode,
        )
        record(db, update, "broadcast", False, "empty_payload")
        return
    options, text = parse_delivery_tokens(raw)
    if not text:
        await update.effective_message.reply_text("Текст broadcast пуст.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "broadcast", False, "empty_text")
        return
    expires_at = (datetime.utcnow() + timedelta(seconds=cfg.broadcast_confirmation_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    payload_text = format_announcement_text(cfg, update.effective_user.full_name if update.effective_user else "NMTelegramBot", text, tag=",".join(options["tags"]))
    thread_id = options["message_thread_id"] or (settings.default_thread_id if settings and options["scope"] == "current" else None)
    delivery_key = f"broadcast:{user_id}:{expires_at}:{hash(raw)}"
    db.save_pending_broadcast(
        user_id=user_id,
        chat_id=update.effective_chat.id,
        message=payload_text,
        expires_at=expires_at,
        target_scope=options["scope"],
        target_tags=options["tags"],
        media_kind=options["media_kind"],
        media_ref=options["media_ref"],
        message_thread_id=thread_id,
        disable_notification=bool(options["disable_notification"] or (settings.disable_notifications if settings else False)),
        delivery_key=delivery_key,
    )
    targets = db.resolve_target_chats(
        allowed_chat_ids=cfg.telegram_allowed_chat_ids,
        fallback_chat_id=update.effective_chat.id,
        target_scope=options["scope"],
        target_tags=options["tags"],
        feature="broadcasts",
    )
    db.record_admin_action(chat_id=update.effective_chat.id, user_id=user_id, action="broadcast_prepare", payload=raw)
    preview = (
        "<b>Broadcast preview</b>\n"
        f"Scope: <b>{html.escape(options['scope'])}</b>\n"
        f"Tags: <code>{html.escape(','.join(options['tags']) or '-')}</code>\n"
        f"Target chats: <b>{len(targets)}</b>\n"
        f"Media: <b>{html.escape(options['media_kind'] or 'text')}</b>\n"
        f"Thread: <code>{thread_id or '-'}</code>\n"
        f"Expires at (UTC): <code>{expires_at}</code>\n\n"
        f"{payload_text}"
    )
    await update.effective_message.reply_text(preview, parse_mode=cfg.telegram_parse_mode, reply_markup=broadcast_confirmation_keyboard())
    record(db, update, "broadcast", True, f"prepared:{options['scope']}:{','.join(options['tags'])}")


async def confirm_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    pending = db.get_pending_broadcast(user_id=update.effective_user.id)
    if not pending:
        await update.effective_message.reply_text("Нет pending broadcast для подтверждения.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "broadcast", False, "confirm_without_pending")
        return
    if pending["expires_at"] <= utc_now_iso():
        db.clear_pending_broadcast(user_id=update.effective_user.id)
        await update.effective_message.reply_text("Ожидающий broadcast истёк. Создайте новый.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "broadcast", False, "pending_expired")
        return
    if pending.get("delivery_key") and not db.claim_idempotency_key(f"delivery:{pending['delivery_key']}", ttl_seconds=600):
        await update.effective_message.reply_text("Этот broadcast уже был подтверждён ранее.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "broadcast", False, "duplicate_confirm")
        return
    targets = db.resolve_target_chats(
        allowed_chat_ids=cfg.telegram_allowed_chat_ids,
        fallback_chat_id=update.effective_chat.id,
        target_scope=str(pending.get("target_scope") or "all"),
        target_tags=[item for item in str(pending.get("target_tags") or "").split(",") if item],
        feature="broadcasts",
    )
    payload = OutgoingPayload(
        text=pending["message"],
        media_kind=str(pending.get("media_kind") or ""),
        media_ref=str(pending.get("media_ref") or ""),
        message_thread_id=pending.get("message_thread_id"),
        disable_notification=bool(pending.get("disable_notification")),
        dry_run=(db.runtime_value('delivery:dry_run', '0') == '1'),
    )
    source_id = str(pending.get('delivery_key') or f"broadcast:{update.effective_user.id}:{update.update_id}")
    deliveries = []
    payload_data = {
        'text': payload.text, 'media_kind': payload.media_kind, 'media_ref': payload.media_ref,
        'message_thread_id': payload.message_thread_id, 'disable_notification': payload.disable_notification,
    }
    for chat_id in targets:
        allowed, reason = db.should_deliver_now(chat_id=chat_id, tag=str(pending.get('target_tags') or '').split(',')[0] if pending.get('target_tags') else '') if hasattr(db, 'should_deliver_now') else (True, '')
        db.ensure_broadcast_delivery(source_type='broadcast', source_id=source_id, chat_id=chat_id, delivery_key=source_id, payload=payload_data)
        if db.broadcast_delivery_is_sent(source_type='broadcast', source_id=source_id, chat_id=chat_id):
            continue
        if not allowed:
            db.mark_broadcast_delivery_failed(source_type='broadcast', source_id=source_id, chat_id=chat_id, error=reason)
            continue
        db.mark_broadcast_delivery_attempt(source_type='broadcast', source_id=source_id, chat_id=chat_id)
        deliveries.append((chat_id, payload))
    results = await send_payloads_bounded(context.bot, deliveries, parse_mode=cfg.telegram_parse_mode, max_concurrency=cfg.delivery_max_concurrency, max_per_minute=cfg.delivery_max_per_minute, paused_until_ts=float(db.runtime_value('delivery:paused_until_ts', '0') or '0'), db=db)
    sent = 0
    failed = 0
    for result in results:
        if result.ok:
            sent += 1
            db.mark_broadcast_delivery_sent(source_type='broadcast', source_id=source_id, chat_id=result.chat_id)
        else:
            failed += 1
            db.mark_broadcast_delivery_failed(source_type='broadcast', source_id=source_id, chat_id=result.chat_id, error=result.error)
            db.record_admin_action(chat_id=result.chat_id, user_id=update.effective_user.id, action="broadcast_send_failed", payload=result.error)
            db.enqueue_dead_letter(
                source_type="broadcast",
                source_id=source_id,
                chat_id=result.chat_id,
                payload=payload_data,
                error=result.error,
                retry_count=1,
            )
    db.clear_pending_broadcast(user_id=update.effective_user.id)
    db.record_admin_action(chat_id=update.effective_chat.id, user_id=update.effective_user.id, action="broadcast_confirm", payload=pending["message"])
    push = context.application.bot_data.get('push_community_event')
    if callable(push):
        try:
            await push(
                context.application,
                event_kind='community.announcement.created',
                payload={
                    'title': 'Broadcast из Telegram',
                    'text': str(pending.get('message') or ''),
                    'actor_user_id': str(update.effective_user.id if update.effective_user else ''),
                    'actor_name': update.effective_user.full_name if update.effective_user else 'NMTelegramBot',
                    'source_platform': 'telegram',
                    'external_message_id': source_id,
                },
            )
            db.record_admin_action(chat_id=update.effective_chat.id, user_id=update.effective_user.id, action="telegram_broadcast_bridge_push", payload=str(pending.get('message') or '')[:500])
        except Exception as exc:
            db.record_admin_action(chat_id=update.effective_chat.id, user_id=update.effective_user.id, action="telegram_broadcast_bridge_push_failed", payload=str(exc))
    await update.effective_message.reply_text(f"Broadcast завершён. Sent: <b>{sent}</b>, failed: <b>{failed}</b>.", parse_mode=cfg.telegram_parse_mode)
    record(db, update, "broadcast", True, f"sent={sent};failed={failed}")


async def schedule_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    settings = await authorize(update, context, command="schedule", required_role="admin")
    raw = " ".join(context.args).strip()
    if not raw or raw == "list":
        await update.effective_message.reply_text(_format_schedule_list(db.list_scheduled_broadcasts()), parse_mode=cfg.telegram_parse_mode)
        record(db, update, "schedule", True, "list")
        return
    if command == "dlq":
        await update.effective_message.reply_text(_format_dead_letters(db.list_dead_letters()), parse_mode=cfg.telegram_parse_mode)
        record(db, update, "schedule", True, "dlq")
        return
    if command == "resolve":
        try:
            job_id = int(control_args[0])
        except (IndexError, ValueError):
            await update.effective_message.reply_text("Использование: /schedule resolve <dead_letter_id>", parse_mode=cfg.telegram_parse_mode)
            record(db, update, "schedule", False, "resolve_parse_error")
            return
        ok = db.resolve_dead_letter(job_id)
        await update.effective_message.reply_text("Dead letter помечен resolved." if ok else "Dead letter не найден.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "schedule", ok, f"resolve:{job_id}")
        return
    if command == "requeue":
        try:
            job_id = int(control_args[0])
        except (IndexError, ValueError):
            await update.effective_message.reply_text("Использование: /schedule requeue <id>", parse_mode=cfg.telegram_parse_mode)
            record(db, update, "schedule", False, "requeue_parse_error")
            return
        ok = db.requeue_scheduled_broadcast(job_id)
        await update.effective_message.reply_text("Задача requeue выполнена." if ok else "Задача не найдена.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "schedule", ok, f"requeue:{job_id}")
        return
    if command == "cancel":
        try:
            job_id = int(control_args[0])
        except (IndexError, ValueError):
            await update.effective_message.reply_text("Использование: /schedule cancel <id>", parse_mode=cfg.telegram_parse_mode)
            record(db, update, "schedule", False, "cancel_parse_error")
            return
        ok = db.cancel_scheduled_broadcast(job_id)
        await update.effective_message.reply_text("Задача отменена." if ok else "Задача не найдена или уже не pending.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "schedule", ok, f"cancel:{job_id}")
        return
    if command == "replay":
        if not control_args:
            await update.effective_message.reply_text("Использование: /schedule replay <dead_letter_id>|all", parse_mode=cfg.telegram_parse_mode)
            record(db, update, "schedule", False, "replay_parse_error")
            return
        targets = db.list_dead_letters(limit=100 if control_args[0] == 'all' else 1, status='pending') if control_args[0] == 'all' else ([db.get_dead_letter(int(control_args[0]))] if str(control_args[0]).isdigit() else [])
        targets = [item for item in targets if item]
        replayed = 0
        failed = 0
        for job in targets:
            try:
                payload_data = json.loads(job.get('payload_json') or '{}')
                payload = OutgoingPayload(
                    text=str(payload_data.get('text') or ''),
                    media_kind=str(payload_data.get('media_kind') or ''),
                    media_ref=str(payload_data.get('media_ref') or ''),
                    message_thread_id=payload_data.get('message_thread_id'),
                    disable_notification=bool(payload_data.get('disable_notification')),
                )
                await send_payload(context.bot, chat_id=int(job['chat_id']), payload=payload, parse_mode=cfg.telegram_parse_mode, db=db)
                db.resolve_dead_letter(int(job['id']))
                replayed += 1
            except Exception as exc:
                db.touch_dead_letter_retry(int(job['id']), error=str(exc))
                failed += 1
        await update.effective_message.reply_text(f"Replay завершён. replayed=<b>{replayed}</b> failed=<b>{failed}</b>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "schedule", failed == 0, f"replay:{replayed}:{failed}")
        return

    before, sep, message = raw.partition(" -- ")
    if not sep:
        await update.effective_message.reply_text("Использование: /schedule at=YYYY-MM-DDTHH:MM [scope=..] [tags=..] [media=kind:url] [thread=id] -- <текст>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "schedule", False, "missing_separator")
        return
    scheduled_for = ""
    options, _ = parse_delivery_tokens(before + " -- x")
    for token in _shell_split(before):
        if token.startswith("at="):
            scheduled_for = token.split("=", 1)[1].strip()
    if not scheduled_for or not message.strip():
        await update.effective_message.reply_text("Недостаточно данных для schedule.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "schedule", False, "missing_fields")
        return
    try:
        dt = datetime.fromisoformat(scheduled_for)
    except ValueError:
        await update.effective_message.reply_text("Формат at= должен быть YYYY-MM-DDTHH:MM", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "schedule", False, "bad_datetime")
        return
    thread_id = options["message_thread_id"] or (settings.default_thread_id if settings and options["scope"] == "current" else None)
    payload = format_announcement_text(cfg, update.effective_user.full_name if update.effective_user else "NMTelegramBot", message.strip(), tag=",".join(options["tags"]))
    delivery_key = f"schedule:{update.effective_user.id}:{scheduled_for}:{hash(raw)}"
    job_id = db.schedule_broadcast(
        created_by_user_id=update.effective_user.id,
        message=payload,
        target_scope=options["scope"],
        target_tags=options["tags"],
        scheduled_for=dt.strftime("%Y-%m-%d %H:%M:%S"),
        media_kind=options["media_kind"],
        media_ref=options["media_ref"],
        message_thread_id=thread_id,
        disable_notification=bool(options["disable_notification"] or (settings.disable_notifications if settings else False)),
        delivery_key=delivery_key,
    )
    await update.effective_message.reply_text(f"Scheduled broadcast создан: <b>#{job_id}</b>", parse_mode=cfg.telegram_parse_mode)
    record(db, update, "schedule", True, f"create:{job_id}")


async def chatsettings_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    settings = await authorize(update, context, command="chatsettings", required_role="admin")
    raw = " ".join(context.args).strip()
    if settings is None:
        await update.effective_message.reply_text("Chat settings недоступны.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "chatsettings", False, "no_chat")
        return
    if not raw or raw == "show":
        lines = [
            "<b>Chat settings</b>",
            f"chat_id: <code>{settings.chat_id}</code>",
            f"title: <b>{html.escape(settings.title or '')}</b>",
            f"type: <b>{html.escape(settings.chat_type or '')}</b>",
            f"allow_status: <b>{settings.allow_status}</b>",
            f"allow_announcements: <b>{settings.allow_announcements}</b>",
            f"allow_broadcasts: <b>{settings.allow_broadcasts}</b>",
            f"tags: <code>{html.escape(','.join(settings.tags) or '-')}</code>",
            f"default_thread_id: <code>{settings.default_thread_id or '-'}</code>",
            f"disable_notifications: <b>{settings.disable_notifications}</b>",
            f"chat_timezone: <code>{html.escape(getattr(settings, 'chat_timezone', 'Europe/Berlin'))}</code>",
            f"quiet_hours: <code>{getattr(settings, 'quiet_hours_start', -1)}-{getattr(settings, 'quiet_hours_end', -1)}</code>",
            f"feature_flags: <code>{html.escape(json.dumps(getattr(settings, 'feature_flags', {}), ensure_ascii=False, sort_keys=True))}</code>",
        ]
        await update.effective_message.reply_text("\n".join(lines), parse_mode=cfg.telegram_parse_mode)
        record(db, update, "chatsettings", True, "show")
        return
    if raw.startswith("list"):
        chat_type = None
        tag = None
        for token in _shell_split(raw)[1:]:
            if token.startswith("type="):
                chat_type = token.split("=", 1)[1].strip()
            elif token.startswith("tag="):
                tag = token.split("=", 1)[1].strip().lower()
        await update.effective_message.reply_text(_format_known_chats(db.list_chat_settings(chat_type=chat_type, tag=tag)), parse_mode=cfg.telegram_parse_mode)
        record(db, update, "chatsettings", True, "list")
        return
    if raw.startswith("bulk "):
        _, key, value, *filters = _shell_split(raw)
        chat_type = None
        tag = None
        for token in filters:
            if token.startswith("type="):
                chat_type = token.split("=", 1)[1].strip()
            elif token.startswith("tag="):
                tag = token.split("=", 1)[1].strip().lower()
        normalized: object = value
        if key in {"allow_status", "allow_announcements", "allow_broadcasts", "disable_notifications"}:
            normalized = value.lower() in {"1", "true", "yes", "on"}
        elif key == "tags":
            normalized = [item.strip().lower() for item in value.split(",") if item.strip()]
        elif key == "default_thread_id":
            normalized = int(value)
        elif key in {"quiet_hours_start", "quiet_hours_end"}:
            normalized = int(value)
        elif key == 'chat_timezone':
            normalized = value.strip()
        elif key.startswith('feature.'):
            flag_name = key.split('.', 1)[1]
            for item in db.list_chat_settings(chat_type=chat_type, tag=tag):
                db.set_chat_feature_flag(item.chat_id, flag_name, value.lower() in {"1", "true", "yes", "on"})
            affected = len(db.list_chat_settings(chat_type=chat_type, tag=tag))
            await update.effective_message.reply_text(f"Массовое обновление feature flag выполнено. Затронуто чатов: <b>{affected}</b>", parse_mode=cfg.telegram_parse_mode)
            record(db, update, "chatsettings", True, f"bulk:{key}:{affected}")
            return
        affected = db.bulk_update_chat_settings(chat_type=chat_type, tag=tag, updates={key: normalized})
        await update.effective_message.reply_text(f"Массовое обновление выполнено. Затронуто чатов: <b>{affected}</b>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "chatsettings", True, f"bulk:{key}:{affected}")
        return
    if not raw.startswith("set "):
        await update.effective_message.reply_text("Использование: /chatsettings show | list [...] | set <key> <value> | bulk <key> <value> [type=..] [tag=..]", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "chatsettings", False, "bad_usage")
        return
    _, key, *rest = _shell_split(raw)
    value = " ".join(rest).strip()
    normalized: object = value
    if key in {"allow_status", "allow_announcements", "allow_broadcasts", "disable_notifications"}:
        normalized = value.lower() in {"1", "true", "yes", "on"}
    elif key == "tags":
        normalized = [item.strip().lower() for item in value.split(",") if item.strip()]
    elif key == "default_thread_id":
        normalized = int(value)
    elif key in {"quiet_hours_start", "quiet_hours_end"}:
        normalized = int(value)
    elif key == 'chat_timezone':
        normalized = value.strip()
    elif key.startswith('feature.'):
        flag_name = key.split('.', 1)[1]
        db.set_chat_feature_flag(update.effective_chat.id, flag_name, value.lower() in {"1", "true", "yes", "on"})
        new_settings = db.get_chat_settings(update.effective_chat.id)
        await update.effective_message.reply_text(f"Обновлено: <b>{html.escape(key)}</b> = <code>{html.escape(str((new_settings.feature_flags or {}).get(flag_name)))}</code>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "chatsettings", True, f"set:{key}")
        return
    new_settings = db.update_chat_settings(update.effective_chat.id, **{key: normalized})
    await update.effective_message.reply_text(f"Обновлено: <b>{html.escape(key)}</b> = <code>{html.escape(str(getattr(new_settings, key)))}</code>", parse_mode=cfg.telegram_parse_mode)
    record(db, update, "chatsettings", True, f"set:{key}")


async def pull_announcements_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db, status_client = deps(context, with_status=True)
    await authorize(update, context, command="pullannouncements", required_role="admin")
    idem = f"manual-feed:{update.effective_user.id}:{update.update_id}"
    if not db.claim_idempotency_key(idem, ttl_seconds=300):
        await update.effective_message.reply_text("Этот manual feed pull уже выполнялся.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "pullannouncements", False, "duplicate")
        return
    if not status_client.feed_is_configured():
        await update.effective_message.reply_text("ANNOUNCEMENT_FEED_URL не настроен.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "pullannouncements", False, "feed_not_configured")
        return
    try:
        items = await status_client.fetch_announcements()
    except Exception as exc:
        await update.effective_message.reply_text(f"Ошибка при загрузке feed: <code>{html.escape(str(exc))}</code>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "pullannouncements", False, "feed_error")
        return
    new_count = 0
    for item in items:
        targets = db.resolve_target_chats(
            allowed_chat_ids=cfg.telegram_allowed_chat_ids,
            fallback_chat_id=None,
            target_scope="all",
            target_tags=[item.tag] if item.tag else [],
            feature="announcements",
        )
        db.mark_external_announcement_delivered(event_id=item.event_id, tag=item.tag, text=item.text, source_created_at=item.created_at)
        created = db.enqueue_feed_deliveries(event_id=item.event_id, tag=item.tag, text=item.text, source_created_at=item.created_at, chat_ids=targets)
        new_count += created
    db.increment_runtime_counter("feed_sync_total", 1)
    db.set_runtime_value("last_feed_sync_at", utc_now_iso())
    await update.effective_message.reply_text(f"Feed обработан. Новых событий: <b>{new_count}</b>", parse_mode=cfg.telegram_parse_mode)
    record(db, update, "pullannouncements", True, f"new={new_count}")


async def admin_help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='adminhelp', required_role='admin')
    await update.effective_message.reply_text(_ops_help_text('admin'), parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'adminhelp', True, 'shown')
