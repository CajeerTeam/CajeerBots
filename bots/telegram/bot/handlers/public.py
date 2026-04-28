from __future__ import annotations

import html
import json
import time

from telegram import Update
from telegram.ext import ContextTypes

from nmbot.handlers.shared import authorize, deps, format_status_text, get_maintenance_banner, links_keyboard, record, reply_help, root_menu_keyboard
from nmbot.templates import start_text


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    settings = await authorize(update, context, command="start")
    banner = get_maintenance_banner(db)
    text = start_text(cfg)
    if banner:
        text += "\n\n" + banner
    await update.effective_message.reply_text(text, parse_mode=cfg.telegram_parse_mode, reply_markup=root_menu_keyboard())
    record(db, update, "start", True, "shown")


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    settings = await authorize(update, context, command="help")
    banner = get_maintenance_banner(db)
    if banner:
        await update.effective_message.reply_text(banner, parse_mode=cfg.telegram_parse_mode)
    await reply_help(update, context, cfg)
    record(db, update, "help", True, "shown")


async def status_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db, status_client = deps(context, with_status=True)
    settings = await authorize(update, context, command="status", expensive=True)
    shard = context.args[0].strip().lower() if context.args else ''
    status = await status_client.fetch_status(shard=shard)
    maintenance = db.get_maintenance_state()
    await update.effective_message.reply_text(
        format_status_text(cfg, status, maintenance_message=maintenance.get('message') if maintenance.get('active') else ''),
        parse_mode=cfg.telegram_parse_mode,
    )
    record(db, update, "status", status.ok, f"fetched:{shard or 'default'}")


async def online_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db, status_client = deps(context, with_status=True)
    settings = await authorize(update, context, command="online", expensive=True)
    shard = context.args[0].strip().lower() if context.args else ''
    status = await status_client.fetch_status(shard=shard)
    value = status.players_online if status.players_online is not None else "unknown"
    text = f"Онлайн игроков: <b>{html.escape(str(value))}</b>"
    banner = get_maintenance_banner(db)
    if banner:
        text += "\n\n" + banner
    await update.effective_message.reply_text(text, parse_mode=cfg.telegram_parse_mode)
    record(db, update, "online", status.ok, f"players={value};shard={shard or 'default'}")


async def links_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    settings = await authorize(update, context, command="links")
    lines = ["<b>Ссылки NeverMine</b>"]
    for label, url in cfg.links.items():
        if url:
            lines.append(f'• <a href="{html.escape(url)}">{html.escape(label)}</a>')
    if len(lines) == 1:
        lines.append("Ссылки пока не заполнены.")
    banner = get_maintenance_banner(db)
    if banner:
        lines.append("")
        lines.append(banner)
    await update.effective_message.reply_text("\n".join(lines), parse_mode=cfg.telegram_parse_mode, disable_web_page_preview=True, reply_markup=links_keyboard(cfg))
    record(db, update, "links", True, "shown")


async def stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    settings = await authorize(update, context, command="stats")
    stats = db.basic_stats()
    uptime_seconds = int(time.time() - context.application.bot_data["started_at"])
    lines = [
        "<b>NMTelegramBot stats</b>",
        f"Uptime: <b>{uptime_seconds}s</b>",
        f"Interactions total: <b>{stats['total']}</b>",
        f"Interactions 24h: <b>{stats['last_24h']}</b>",
        f"Unique users: <b>{stats['unique_users']}</b>",
        f"Unique chats: <b>{stats['unique_chats']}</b>",
        f"Привязанных аккаунтов: <b>{db.count_linked_accounts()}</b>",
        f"Mode: <b>{html.escape(cfg.bot_mode)}</b>",
    ]
    banner = get_maintenance_banner(db)
    if banner:
        lines.append(banner)
    await update.effective_message.reply_text("\n".join(lines), parse_mode=cfg.telegram_parse_mode)
    record(db, update, "stats", True, "shown")


async def me_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='me')
    user = update.effective_user
    linked = db.get_linked_account(user_id=user.id) if user else None
    prefs = db.get_user_notification_prefs(user.id) if user else {}
    lines = ['<b>Мой профиль</b>']
    lines.append(f'User ID: <code>{user.id}</code>')
    lines.append(f'Role: <b>{html.escape(cfg.role_for_user(user.id))}</b>')
    if linked:
        lines.append(f'Игровой аккаунт: <b>{html.escape(linked.player_name)}</b>')
        if linked.player_uuid:
            lines.append(f'UUID: <code>{html.escape(linked.player_uuid)}</code>')
    else:
        lines.append('Игровой аккаунт: <i>не привязан</i>')
    lines.append(f"Часовой пояс: <code>{html.escape(str(prefs.get('timezone') or cfg.user_pref_default_timezone))}</code>")
    lines.append(f"Теги: <code>{html.escape(','.join(prefs.get('tags') or []) or '-')}</code>")
    lines.append(f"Тихие часы: <code>{prefs.get('quiet_hours_start', -1)}-{prefs.get('quiet_hours_end', -1)}</code>")
    await update.effective_message.reply_text('\n'.join(lines), parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'me', True, 'shown')


async def sessions_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db, status_client = deps(context, with_status=True)
    await authorize(update, context, command='sessions')
    user = update.effective_user
    result = await status_client.list_security_sessions(telegram_user_id=user.id if user else 0)
    payload = json.dumps(result.raw or {}, ensure_ascii=False, indent=2)
    await update.effective_message.reply_text(f'<b>My sessions</b>\nOK: <b>{result.ok}</b>\n<pre>{html.escape(payload[:3500])}</pre>', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'sessions', result.ok, 'shown')


async def twofa_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db, status_client = deps(context, with_status=True)
    await authorize(update, context, command='2fa')
    user = update.effective_user
    result = await status_client.get_security_status(telegram_user_id=user.id if user else 0)
    payload = json.dumps(result.raw or {}, ensure_ascii=False, indent=2)
    await update.effective_message.reply_text(f'<b>2FA status</b>\nOK: <b>{result.ok}</b>\nMessage: <code>{html.escape(result.message or "-")}</code>\n<pre>{html.escape(payload[:3000])}</pre>', parse_mode=cfg.telegram_parse_mode)
    record(db, update, '2fa', result.ok, 'status')


async def notifications_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='notifications')
    user = update.effective_user
    action = (context.args[0].lower() if context.args else 'show')
    prefs = db.get_user_notification_prefs(user.id if user else 0)
    if action == 'show':
        text = '\n'.join([
            '<b>Настройки уведомлений</b>',
            f"часовой пояс: <code>{html.escape(str(prefs.get('timezone') or cfg.user_pref_default_timezone))}</code>",
            f"tags: <code>{html.escape(','.join(prefs.get('tags') or []) or '-')}</code>",
            f"security_enabled: <b>{bool(prefs.get('security_enabled', True))}</b>",
            f"status_enabled: <b>{bool(prefs.get('status_enabled', True))}</b>",
            f"events_enabled: <b>{bool(prefs.get('events_enabled', True))}</b>",
            f"maintenance_enabled: <b>{bool(prefs.get('maintenance_enabled', True))}</b>",
        ])
        await update.effective_message.reply_text(text, parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'notifications', True, 'show')
        return
    if action == 'set' and len(context.args) >= 3:
        key = context.args[1].lower()
        value = ' '.join(context.args[2:]).strip()
        updates = {}
        if key == 'tags':
            updates['tags'] = [item.strip().lower() for item in value.split(',') if item.strip()]
        elif key in {'security_enabled','status_enabled','events_enabled','maintenance_enabled'}:
            updates[key] = value.lower() in {'1','true','yes','on'}
        elif key in {'timezone'}:
            updates[key] = value
        else:
            await update.effective_message.reply_text('Использование: /notifications set tags|security_enabled|status_enabled|events_enabled|maintenance_enabled|timezone <value>', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'notifications', False, 'bad_key')
            return
        db.update_user_notification_prefs(user.id if user else 0, **updates)
        await update.effective_message.reply_text('Настройки уведомлений обновлены.', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'notifications', True, 'set')
        return
    await update.effective_message.reply_text('Использование: /notifications show | set <key> <value>', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'notifications', False, 'bad_action')


async def quiethours_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='quiethours')
    user = update.effective_user
    prefs = db.get_user_notification_prefs(user.id if user else 0)
    if not context.args or context.args[0].lower() == 'show':
        await update.effective_message.reply_text(
            f"Quiet hours: <code>{prefs.get('quiet_hours_start', -1)}-{prefs.get('quiet_hours_end', -1)}</code>\nTimezone: <code>{html.escape(str(prefs.get('timezone') or cfg.user_pref_default_timezone))}</code>",
            parse_mode=cfg.telegram_parse_mode,
        )
        record(db, update, 'quiethours', True, 'show')
        return
    if context.args[0].lower() == 'set' and len(context.args) >= 3:
        try:
            start = int(context.args[1]); end = int(context.args[2])
        except ValueError:
            await update.effective_message.reply_text('Использование: /quiethours set <start> <end> [timezone]', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'quiethours', False, 'bad_hours')
            return
        timezone = context.args[3] if len(context.args) >= 4 else (prefs.get('timezone') or cfg.user_pref_default_timezone)
        db.update_user_notification_prefs(user.id if user else 0, quiet_hours_start=start, quiet_hours_end=end, timezone=timezone)
        await update.effective_message.reply_text('Тихие часы обновлены.', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'quiethours', True, 'set')
        return
    await update.effective_message.reply_text('Использование: /quiethours show | set <start> <end> [timezone]', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'quiethours', False, 'bad_action')
