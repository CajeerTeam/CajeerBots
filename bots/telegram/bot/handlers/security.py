from __future__ import annotations

import html
import json

from telegram import Update
from telegram.ext import ContextTypes

from nmbot.handlers.shared import authorize, deps, record
from nmbot.policy import policy_for


def _security_help_text(*, is_admin: bool) -> str:
    base = 'Security help: /security status | recover <PlayerName> | sessions | revoke <session_id> | revoke-all | trusted-clear'
    if is_admin:
        return base + ' | approve <challenge_id> | deny <challenge_id> | pending'
    return base


async def security_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db, status_client = deps(context, with_status=True)
    action = (context.args[0].lower() if context.args else 'status')
    is_admin = cfg.has_role(update.effective_user.id if update.effective_user else None, 'admin')
    if action in {'help', '--help', '-h'}:
        await update.effective_message.reply_text(_security_help_text(is_admin=is_admin), parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'security', True, 'help')
        return
    required_role = 'admin' if action in {'approve','deny','pending'} else 'user'
    await authorize(update, context, command='security', required_role=required_role)
    user_id = update.effective_user.id if update.effective_user else 0
    if action == 'status':
        result = await status_client.get_security_status(telegram_user_id=user_id)
        payload = json.dumps(result.raw or {}, ensure_ascii=False, indent=2)
        await update.effective_message.reply_text(
            f"<b>Security status</b>\nConfigured: <b>{status_client.security_is_configured()}</b>\nOK: <b>{result.ok}</b>\nMessage: <code>{html.escape(result.message or '-')}</code>\n<pre>{html.escape(payload[:3500])}</pre>",
            parse_mode=cfg.telegram_parse_mode,
        )
        record(db, update, 'security', result.ok, 'status')
        return

    if action in {'approve', 'deny'}:
        if len(context.args) < 2:
            await update.effective_message.reply_text('Использование: /security approve|deny <challenge_id>', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'security', False, 'missing_challenge')
            return
        challenge_id = context.args[1]
        result = await status_client.act_2fa_challenge(challenge_id=challenge_id, action=action, actor_user_id=user_id)
        if hasattr(db, 'mark_security_challenge_notice'):
            db.mark_security_challenge_notice(challenge_id, status='approved' if action == 'approve' else 'denied', actor_user_id=user_id)
        await update.effective_message.reply_text(f"Security action: <b>{result.ok}</b>\n<code>{html.escape(result.message or '-')}</code>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'security', result.ok, f'{action}:{challenge_id}')
        return

    if action == 'recover':
        if len(context.args) < 2:
            await update.effective_message.reply_text('Использование: /security recover <PlayerName>', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'security', False, 'missing_player')
            return
        result = await status_client.request_password_recovery(telegram_user_id=user_id, player_name=context.args[1])
        await update.effective_message.reply_text(f"Recovery request: <b>{result.ok}</b>\n<code>{html.escape(result.message or '-')}</code>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'security', result.ok, f"recover:{context.args[1]}")
        return

    if action == 'sessions':
        result = await status_client.list_security_sessions(telegram_user_id=user_id)
        payload = json.dumps(result.raw or {}, ensure_ascii=False, indent=2)
        await update.effective_message.reply_text(f"<b>Security sessions</b>\nOK: <b>{result.ok}</b>\n<pre>{html.escape(payload[:3500])}</pre>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'security', result.ok, 'sessions')
        return

    if action in {'revoke', 'terminate'}:
        if len(context.args) < 2:
            await update.effective_message.reply_text('Использование: /security revoke <session_id>', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'security', False, 'missing_session')
            return
        result = await status_client.act_security_session(telegram_user_id=user_id, action='revoke', session_id=context.args[1])
        await update.effective_message.reply_text(f"Session revoke: <b>{result.ok}</b>\n<code>{html.escape(result.message or '-')}</code>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'security', result.ok, f"revoke:{context.args[1]}")
        return

    if action in {'revoke-all', 'logout-all'}:
        policy = policy_for('security.revoke_all')
        if policy.get('second_approval') and cfg.role_for_user(user_id) != 'owner' and hasattr(db, 'create_approval_request'):
            request_id = db.create_approval_request(kind='security_session_action', payload={'telegram_user_id': user_id, 'action': 'revoke_all', 'scope': 'all'}, requested_by=user_id, requested_by_name=update.effective_user.full_name if update.effective_user else str(user_id), required_role='owner')
            await update.effective_message.reply_text(f'Создан approval request: <code>{request_id}</code>', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'security', True, f'approval:revoke_all:{request_id}')
            return
        result = await status_client.act_security_session(telegram_user_id=user_id, action='revoke_all', scope='all')
        await update.effective_message.reply_text(f"Revoke all: <b>{result.ok}</b>\n<code>{html.escape(result.message or '-')}</code>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'security', result.ok, 'revoke_all')
        return

    if action == 'trusted-clear':
        policy = policy_for('security.trusted_clear')
        if policy.get('second_approval') and cfg.role_for_user(user_id) != 'owner' and hasattr(db, 'create_approval_request'):
            request_id = db.create_approval_request(kind='security_session_action', payload={'telegram_user_id': user_id, 'action': 'trusted_clear', 'scope': 'trusted'}, requested_by=user_id, requested_by_name=update.effective_user.full_name if update.effective_user else str(user_id), required_role='owner')
            await update.effective_message.reply_text(f'Создан approval request: <code>{request_id}</code>', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'security', True, f'approval:trusted_clear:{request_id}')
            return
        result = await status_client.act_security_session(telegram_user_id=user_id, action='trusted_clear', scope='trusted')
        await update.effective_message.reply_text(f"Trusted sessions cleared: <b>{result.ok}</b>\n<code>{html.escape(result.message or '-')}</code>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'security', result.ok, 'trusted_clear')
        return

    if action == 'pending':
        rows = db.list_security_challenge_notices(status='pending', limit=20) if hasattr(db, 'list_security_challenge_notices') else []
        if not rows:
            await update.effective_message.reply_text('Ожидающих security challenge нет.', parse_mode=cfg.telegram_parse_mode)
        else:
            lines = ['<b>Ожидающие security challenges</b>']
            for row in rows:
                lines.append(f"<code>{html.escape(str(row.get('challenge_id')))}</code> | status=<b>{html.escape(str(row.get('status')))}</b> | action=<code>{html.escape(str(row.get('action')))}</code>")
            await update.effective_message.reply_text('\n'.join(lines), parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'security', True, 'pending')
        return

    await update.effective_message.reply_text(
        _security_help_text(is_admin=is_admin).replace('Security help: ', 'Использование: '),
        parse_mode=cfg.telegram_parse_mode,
    )
    record(db, update, 'security', False, 'bad_action')
