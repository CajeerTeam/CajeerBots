from __future__ import annotations

import os

import html
import json
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from nmbot.delivery import parse_delivery_tokens
from nmbot.handlers.shared import authorize, deps, record, paginate_lines
from nmbot.health_http import render_metrics
from nmbot.templates import preview_template, validate_templates
from nmbot.main_support import execute_approval_request
from nmbot.policy import policy_for

_ALLOWED_RBAC_ROLES = {'user', 'mod', 'admin', 'owner'}

def _ops_help_text(group: str) -> str:
    mapping = {
        'ops': 'Операционные команды: /opshelp или /ops. Ключевые группы: /adminhelp, /deliveryhelp, /securityhelp. Также доступны /maintenance, /template, /rbac, /webhook, /metrics, /onboarding, /subscribe, /unsubscribe, /alerts, /approval, /adminsite, /timezone, /mode, /incident',
        'delivery': 'Delivery help: /deliveryhelp или /delivery status | pause | resume | dry-run on|off | dry-run-targets <scope> [tags=a,b] [shards=x,y]',
        'admin': 'Admin help: /adminhelp или /admin, /health, /diag, /adminstats, /announce, /broadcast, /schedule, /chatsettings, /pullannouncements',
        'security': 'Security help: /securityhelp или /security help | status | poll | approve <id> | deny <id> | sessions <player> | revoke <session_id> | revoke-all <player> | trusted-clear <player>',
    }
    return mapping.get(group, mapping['ops'])


def _shell_split(raw: str) -> list[str]:
    import shlex
    try:
        return shlex.split(raw)
    except ValueError:
        return raw.split()


def _format_rbac(db) -> str:
    rows = db.list_rbac_entries()
    if not rows:
        return 'RBAC overrides отсутствуют.'
    lines = ['<b>RBAC overrides</b>']
    for row in rows:
        lines.append(f"{html.escape(row['kind'])} target=<code>{html.escape(row['target'])}</code> command=<code>{html.escape(row['command'])}</code> role=<b>{html.escape(row['value'])}</b>")
    return '\n'.join(lines)


def _format_onboarding(rows: list[dict]) -> str:
    if not rows:
        return 'Onboarding queue пуста.'
    lines = ['<b>Onboarding queue</b>']
    for row in rows:
        lines.append(f"chat=<code>{row.get('chat_id')}</code> | status=<b>{html.escape(str(row.get('status', '-')))}</b> | type=<code>{html.escape(str(row.get('chat_type', '-')))}</code> | title=<b>{html.escape(str(row.get('title', '-')))}</b> | updated=<code>{html.escape(str(row.get('updated_at', '-')))}</code>")
    return '\n'.join(lines)


async def subscribe_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    settings = await authorize(update, context, command='subscribe', required_role='admin')
    if not context.args:
        await update.effective_message.reply_text('Использование: /subscribe <tag1,tag2>', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'subscribe', False, 'missing_tags')
        return
    tags = sorted({item.strip().lower() for item in ' '.join(context.args).split(',') if item.strip()})
    current = settings.tags if settings else []
    merged = sorted(set(current) | set(tags))
    db.update_chat_settings(update.effective_chat.id, tags=merged)
    await update.effective_message.reply_text(f"Подписки чата обновлены: <code>{html.escape(','.join(merged) or '-')}</code>", parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'subscribe', True, ','.join(merged))


async def unsubscribe_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    settings = await authorize(update, context, command='unsubscribe', required_role='admin')
    current = settings.tags if settings else []
    if not context.args:
        await update.effective_message.reply_text('Использование: /unsubscribe <tag1,tag2|all>', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'unsubscribe', False, 'missing_tags')
        return
    raw = ' '.join(context.args).strip().lower()
    updated = [] if raw == 'all' else [item for item in current if item not in {t.strip().lower() for t in raw.split(',') if t.strip()}]
    db.update_chat_settings(update.effective_chat.id, tags=updated)
    await update.effective_message.reply_text(f"Подписки чата: <code>{html.escape(','.join(updated) or '-')}</code>", parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'unsubscribe', True, ','.join(updated))


async def maintenance_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='maintenance', required_role='admin')
    action = (context.args[0].lower() if context.args else 'status')
    if action == 'status':
        state = db.get_maintenance_state()
        await update.effective_message.reply_text(
            f"Техработы: <b>{'on' if state['active'] else 'off'}</b>\nСообщение: <code>{html.escape(state['message'] or '-')}</code>",
            parse_mode=cfg.telegram_parse_mode,
        )
        record(db, update, 'maintenance', True, 'status')
        return
    if action in {'on', 'enable'}:
        message = ' '.join(context.args[1:]).strip()
        actor_id = update.effective_user.id if update.effective_user else 0
        policy = policy_for('maintenance.on')
        if policy.get('second_approval') and cfg.role_for_user(actor_id) != 'owner' and hasattr(db, 'create_approval_request'):
            request_id = db.create_approval_request(kind='maintenance', payload={'active': True, 'message': message}, requested_by=actor_id, requested_by_name=update.effective_user.full_name if update.effective_user else str(actor_id), required_role='owner')
            await update.effective_message.reply_text(f'Создан approval request: <code>{request_id}</code>', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'maintenance', True, f'approval:on:{request_id}')
            return
        db.set_maintenance_state(active=True, message=message, updated_by=str(actor_id))
        await update.effective_message.reply_text('Maintenance mode включён.', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'maintenance', True, 'on')
        return
    if action in {'off', 'disable'}:
        actor_id = update.effective_user.id if update.effective_user else 0
        policy = policy_for('maintenance.off')
        if policy.get('second_approval') and cfg.role_for_user(actor_id) != 'owner' and hasattr(db, 'create_approval_request'):
            request_id = db.create_approval_request(kind='maintenance', payload={'active': False, 'message': ''}, requested_by=actor_id, requested_by_name=update.effective_user.full_name if update.effective_user else str(actor_id), required_role='owner')
            await update.effective_message.reply_text(f'Создан approval request: <code>{request_id}</code>', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'maintenance', True, f'approval:off:{request_id}')
            return
        db.set_maintenance_state(active=False, message='', updated_by=str(actor_id))
        await update.effective_message.reply_text('Maintenance mode выключен.', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'maintenance', True, 'off')
        return
    await update.effective_message.reply_text('Использование: /maintenance on <message> | off | status', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'maintenance', False, 'bad_action')


async def template_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='template', required_role='admin')
    action = (context.args[0].lower() if context.args else 'validate')
    if action == 'validate':
        errors = validate_templates(cfg)
        text = 'Шаблоны корректны' if not errors else 'Ошибки валидации шаблонов:\n' + '\n'.join(errors)
        await update.effective_message.reply_text(text, parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'template', not errors, 'validate')
        return
    if action == 'preview':
        raw = ' '.join(context.args[1:]).strip()
        if not raw:
            await update.effective_message.reply_text('Использование: /template preview <start|feed|announcement|maintenance> [tag=x] -- <text>', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'template', False, 'preview_missing')
            return
        _, text = parse_delivery_tokens(raw)
        tokens = _shell_split(raw)
        name = tokens[0]
        tag = ''
        for token in tokens[1:]:
            if token.startswith('tag='):
                tag = token.split('=', 1)[1].strip()
        preview = preview_template(cfg, name, text=text or 'Preview text', author=update.effective_user.full_name if update.effective_user else 'NMTelegramBot', tag=tag)
        await update.effective_message.reply_text(preview, parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'template', True, f'preview:{name}:ru')
        return
    await update.effective_message.reply_text('Использование: /template preview|validate', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'template', False, 'bad_action')


async def rbac_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='rbac', required_role='owner')
    if not context.args or context.args[0].lower() == 'show':
        await update.effective_message.reply_text(_format_rbac(db), parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'rbac', True, 'show')
        return
    action = context.args[0].lower()
    if action == 'set' and len(context.args) >= 4:
        scope = context.args[1].lower()
        if scope == 'user' and len(context.args) >= 4:
            user_id = int(context.args[2]); role = context.args[3].lower()
            if role not in _ALLOWED_RBAC_ROLES:
                raise ValueError('bad role')
            db.set_user_role_override(user_id, role)
        elif scope == 'global' and len(context.args) >= 4:
            command = context.args[2].lower(); role = context.args[3].lower()
            if role not in _ALLOWED_RBAC_ROLES:
                raise ValueError('bad role')
            db.set_command_role_override(scope='global', command=command, role=role)
        elif scope == 'chat' and len(context.args) >= 5:
            chat_id = int(context.args[2]); command = context.args[3].lower(); role = context.args[4].lower()
            if role not in _ALLOWED_RBAC_ROLES:
                raise ValueError('bad role')
            db.set_command_role_override(scope='chat', chat_id=chat_id, command=command, role=role)
        else:
            await update.effective_message.reply_text('Использование: /rbac set user <user_id> <role> | global <command> <role> | chat <chat_id> <command> <role>', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'rbac', False, 'bad_set')
            return
        await update.effective_message.reply_text('RBAC override сохранён.', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'rbac', True, 'set')
        return
    if action == 'reset' and len(context.args) >= 3:
        scope = context.args[1].lower()
        if scope == 'user':
            db.clear_user_role_override(int(context.args[2]))
        elif scope == 'global':
            db.clear_command_role_override(scope='global', command=context.args[2].lower())
        elif scope == 'chat' and len(context.args) >= 4:
            db.clear_command_role_override(scope='chat', chat_id=int(context.args[2]), command=context.args[3].lower())
        else:
            await update.effective_message.reply_text('Использование: /rbac reset user <user_id> | global <command> | chat <chat_id> <command>', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'rbac', False, 'bad_reset')
            return
        await update.effective_message.reply_text('RBAC override удалён.', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'rbac', True, 'reset')
        return
    await update.effective_message.reply_text('Использование: /rbac show | set ... | reset ...', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'rbac', False, 'bad_action')


async def webhook_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='webhook', required_role='admin')
    action = context.args[0].lower() if context.args else 'status'
    token_part = cfg.telegram_bot_token.split(':', 1)[0]
    url_path = f"{cfg.webhook_path_prefix}/{token_part}"
    target_url = cfg.webhook_url.rstrip('/') + '/' + url_path if cfg.webhook_url else ''
    if action == 'status':
        info = await context.bot.get_webhook_info()
        mismatch = bool(target_url and info.url != target_url)
        await update.effective_message.reply_text(
            f"Configured target: <code>{html.escape(target_url or '-')}</code>\nCurrent webhook: <code>{html.escape(info.url or '-')}</code>\nPending updates: <b>{info.pending_update_count}</b>\nMismatch: <b>{mismatch}</b>",
            parse_mode=cfg.telegram_parse_mode,
        )
        record(db, update, 'webhook', True, 'status')
        return
    if action in {'set', 'refresh', 'reconcile'}:
        if not target_url:
            await update.effective_message.reply_text('WEBHOOK_URL не задан.', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'webhook', False, 'missing_target')
            return
        ok = await context.bot.set_webhook(url=target_url, secret_token=cfg.webhook_secret_token or None)
        await update.effective_message.reply_text(f"Webhook установлен: <b>{ok}</b>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'webhook', bool(ok), action)
        return
    if action == 'delete':
        ok = await context.bot.delete_webhook(drop_pending_updates=False)
        await update.effective_message.reply_text(f"Webhook удалён: <b>{ok}</b>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'webhook', bool(ok), 'delete')
        return
    await update.effective_message.reply_text('Использование: /webhook status|set|refresh|reconcile|delete', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'webhook', False, 'bad_action')


async def metrics_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='metrics', required_role='admin')
    await update.effective_message.reply_text(f'<pre>{html.escape(render_metrics(context.application))}</pre>', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'metrics', True, 'shown')


def _wizard_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('Русский', callback_data=f'onboarding:lang:{chat_id}:ru')],
        [InlineKeyboardButton('news', callback_data=f'onboarding:tag:{chat_id}:news'), InlineKeyboardButton('events', callback_data=f'onboarding:tag:{chat_id}:events')],
        [InlineKeyboardButton('maintenance', callback_data=f'onboarding:tag:{chat_id}:maintenance'), InlineKeyboardButton('Done', callback_data=f'onboarding:done:{chat_id}')],
    ])


async def onboarding_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='onboarding', required_role='admin')
    action = context.args[0].lower() if context.args else 'list'
    if action == 'list':
        rows = db.list_onboarding()
        await update.effective_message.reply_text(_format_onboarding(rows), parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'onboarding', True, 'list')
        return
    if action == 'wizard':
        chat_id = int(context.args[1]) if len(context.args) >= 2 else update.effective_chat.id
        db.set_json_state(f'onboarding:wizard:{chat_id}', {'chat_id': chat_id, 'tags': [], 'started_by': update.effective_user.id if update.effective_user else None})
        await update.effective_message.reply_text('Onboarding wizard started.', parse_mode=cfg.telegram_parse_mode, reply_markup=_wizard_keyboard(chat_id))
        record(db, update, 'onboarding', True, f'wizard:{chat_id}')
        return
    if action == 'status' and len(context.args) >= 2:
        row = db.get_onboarding_status(int(context.args[1]))
        await update.effective_message.reply_text(json.dumps(row or {}, ensure_ascii=False, indent=2), parse_mode=None)
        record(db, update, 'onboarding', True, 'status')
        return
    if action in {'approve', 'reject'} and len(context.args) >= 2:
        chat_id = int(context.args[1])
        settings = db.get_chat_settings(chat_id)
        db.set_onboarding_status(chat_id=chat_id, status='approved' if action == 'approve' else 'rejected', title=settings.title if settings else '', chat_type=settings.chat_type if settings else '', updated_by=str(update.effective_user.id if update.effective_user else ''))
        await update.effective_message.reply_text(f'Onboarding {action}: <code>{chat_id}</code>', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'onboarding', True, action)
        return
    await update.effective_message.reply_text('Использование: /onboarding list | wizard [chat_id] | status <chat_id> | approve <chat_id> | reject <chat_id>', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'onboarding', False, 'bad_action')


async def delivery_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='delivery', required_role='admin')
    action = context.args[0].lower() if context.args else 'status'
    if action == 'status':
        paused_until = float(db.runtime_value('delivery:paused_until_ts', '0') or '0')
        paused = paused_until > time.time()
        dry_run = db.runtime_value('delivery:dry_run', '0') == '1'
        await update.effective_message.reply_text(
            f"Delivery paused: <b>{paused}</b>\nPaused until: <code>{paused_until if paused else '-'}</code>\nConcurrency: <b>{cfg.delivery_max_concurrency}</b>\nMax/min: <b>{cfg.delivery_max_per_minute}</b>\nDry-run: <b>{dry_run}</b>",
            parse_mode=cfg.telegram_parse_mode,
        )
        record(db, update, 'delivery', True, 'status')
        return
    if action == 'pause':
        seconds = int(context.args[1]) if len(context.args) > 1 else 300
        until = time.time() + max(1, seconds)
        db.set_runtime_value('delivery:paused_until_ts', str(until))
        await update.effective_message.reply_text(f'Delivery paused for {seconds}s.', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'delivery', True, 'pause')
        return
    if action == 'resume':
        db.set_runtime_value('delivery:paused_until_ts', '0')
        await update.effective_message.reply_text('Delivery resumed.', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'delivery', True, 'resume')
        return
    if action == 'dry-run' and len(context.args) >= 2:
        enabled = context.args[1].lower() in {'1', 'true', 'yes', 'on', 'enable'}
        db.set_runtime_value('delivery:dry_run', '1' if enabled else '0')
        await update.effective_message.reply_text(f'Delivery dry-run: <b>{enabled}</b>', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'delivery', True, f'dry-run:{enabled}')
        return
    await update.effective_message.reply_text('Использование: /delivery status | pause <seconds> | resume | dry-run <on|off>', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'delivery', False, 'bad_action')


def _format_alerts(rows: list[dict], *, page: int = 1) -> str:
    body = []
    for row in rows:
        body.append(f"#{row.get('id')} | <b>{html.escape(str(row.get('status')))}</b> | <code>{html.escape(str(row.get('kind')))}</code> | sev=<b>{html.escape(str(row.get('severity')))}</b> | count=<b>{row.get('count')}</b> | <code>{html.escape(str(row.get('summary') or '-'))}</code>")
    return paginate_lines(body, page=page, per_page=10, title='<b>Operator alerts</b>') if body else 'Operator alerts отсутствуют.'


async def alerts_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='alerts', required_role='admin')
    action = context.args[0].lower() if context.args else 'list'
    if action == 'list':
        page = 1
        for arg in context.args[1:]:
            if arg.startswith('page='):
                try: page = int(arg.split('=',1)[1])
                except Exception: pass
        rows = db.list_operator_alerts(limit=100) if hasattr(db, 'list_operator_alerts') else []
        await update.effective_message.reply_text(_format_alerts(rows, page=page), parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'alerts', True, f'list:{page}')
        return
    if len(context.args) < 2:
        await update.effective_message.reply_text('Использование: /alerts list [page=N] | ack <id> | mute <id> <minutes> | resolve <id>', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'alerts', False, 'missing_id')
        return
    try:
        alert_id = int(context.args[1])
    except ValueError:
        await update.effective_message.reply_text('id должен быть integer', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'alerts', False, 'bad_id')
        return
    ok = False
    if action == 'ack':
        ok = db.ack_operator_alert(alert_id, update.effective_user.id)
    elif action == 'mute':
        minutes = int(context.args[2]) if len(context.args) >= 3 else 60
        ok = db.mute_operator_alert(alert_id, minutes=minutes, user_id=update.effective_user.id)
    elif action == 'resolve':
        ok = db.resolve_operator_alert(alert_id, update.effective_user.id)
    else:
        await update.effective_message.reply_text('Использование: /alerts list [page=N] | ack <id> | mute <id> <minutes> | resolve <id>', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'alerts', False, 'bad_action')
        return
    await update.effective_message.reply_text('OK' if ok else 'Не найдено.', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'alerts', ok, action)


async def timezone_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='timezone', required_role='admin')
    settings = db.get_chat_settings(update.effective_chat.id) if update.effective_chat else None
    if not context.args or context.args[0].lower() == 'show':
        value = getattr(settings, 'chat_timezone', 'Europe/Berlin') if settings else 'Europe/Berlin'
        await update.effective_message.reply_text(f'Часовой пояс чата: <code>{html.escape(str(value))}</code>', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'timezone', True, 'show')
        return
    if context.args[0].lower() == 'set' and len(context.args) >= 2:
        tz = context.args[1].strip()
        db.update_chat_settings(update.effective_chat.id, chat_timezone=tz)
        await update.effective_message.reply_text(f'Часовой пояс чата обновлён: <code>{html.escape(tz)}</code>', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'timezone', True, 'set')
        return
    await update.effective_message.reply_text('Использование: /timezone show | set <Europe/Berlin>', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'timezone', False, 'bad_action')


async def mode_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='mode', required_role='owner')
    action = context.args[0].lower() if context.args else 'status'
    if action == 'status':
        requested = db.runtime_value('requested_bot_mode', cfg.bot_mode)
        await update.effective_message.reply_text(f'Current mode: <b>{html.escape(cfg.bot_mode)}</b>\nRequested next mode: <b>{html.escape(requested)}</b>', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'mode', True, 'status')
        return
    if action == 'prepare' and len(context.args) >= 2:
        target = context.args[1].strip().lower()
        if target not in {'polling','webhook'}:
            await update.effective_message.reply_text('target должен быть polling или webhook', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'mode', False, 'bad_target')
            return
        db.set_runtime_value('requested_bot_mode', target)
        text = f'Prepared next mode: <b>{html.escape(target)}</b>. Реальное переключение произойдёт после restart.'
        if target == 'webhook' and cfg.webhook_url:
            try:
                token_part = cfg.telegram_bot_token.split(':', 1)[0]
                target_url = cfg.webhook_url.rstrip('/') + '/' + f"{cfg.webhook_path_prefix}/{token_part}"
                await context.bot.set_webhook(url=target_url, secret_token=cfg.webhook_secret_token or None)
                text += '\nWebhook синхронизирован для следующего рестарта.'
            except Exception as exc:
                text += f'\nОшибка синхронизации webhook: <code>{html.escape(str(exc))}</code>'
        elif target == 'polling':
            try:
                await context.bot.delete_webhook(drop_pending_updates=False)
                text += '\nWebhook удалён для запуска в polling режиме.'
            except Exception as exc:
                text += f'\nОшибка удаления webhook: <code>{html.escape(str(exc))}</code>'
        await update.effective_message.reply_text(text, parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'mode', True, f'prepare:{target}')
        return
    await update.effective_message.reply_text('Использование: /mode status | prepare <polling|webhook>', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'mode', False, 'bad_action')


async def incident_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='incident', required_role='admin')
    action = context.args[0].lower() if context.args else 'snapshot'
    if action != 'snapshot':
        await update.effective_message.reply_text('Использование: /incident snapshot', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'incident', False, 'bad_action')
        return
    payload = db.collect_incident_snapshot() if hasattr(db, 'collect_incident_snapshot') else {'db_health': db.db_health()}
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    await update.effective_message.reply_text(f'<pre>{html.escape(text[:3900])}</pre>', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'incident', True, 'snapshot')


async def approval_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='approval', required_role='admin')
    action = (context.args[0].lower() if context.args else 'list')
    if action == 'list':
        rows = db.list_approval_requests(status='pending', limit=20) if hasattr(db, 'list_approval_requests') else []
        if not rows:
            text = 'Нет ожидающих approval-запросов.'
        else:
            lines = ['<b>Ожидающие approval-запросы</b>']
            for row in rows:
                lines.append(f"<code>{row['id']}</code> | <b>{html.escape(str(row['kind']))}</b> | by=<code>{row['requested_by']}</code> | status=<b>{html.escape(str(row['status']))}</b>")
            text = '\n'.join(lines)
        await update.effective_message.reply_text(text, parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'approval', True, 'list')
        return
    if len(context.args) < 2:
        await update.effective_message.reply_text('Использование: /approval list | approve <id> | reject <id>', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'approval', False, 'missing_id')
        return
    try:
        request_id = int(context.args[1])
    except ValueError:
        await update.effective_message.reply_text('id должен быть integer', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'approval', False, 'bad_id')
        return
    user_id = update.effective_user.id if update.effective_user else 0
    row = db.get_approval_request(request_id) if hasattr(db, 'get_approval_request') else None
    if not row:
        await update.effective_message.reply_text('Approval request не найден.', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'approval', False, 'not_found')
        return
    if action == 'approve':
        ok, result = await execute_approval_request(context.application, row, actor_user_id=user_id, actor_name=update.effective_user.full_name if update.effective_user else str(user_id))
        db.resolve_approval_request(request_id, status='approved' if ok else 'failed', acted_by=user_id, result_json=result if isinstance(result, dict) else {'message': str(result)})
        await update.effective_message.reply_text(f"Approval applied: <b>{ok}</b>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'approval', ok, f'approve:{request_id}')
        return
    if action == 'reject':
        db.resolve_approval_request(request_id, status='rejected', acted_by=user_id, result_json={'message': 'rejected'})
        await update.effective_message.reply_text('Approval request отклонён.', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'approval', True, f'reject:{request_id}')
        return
    await update.effective_message.reply_text('Использование: /approval list | approve <id> | reject <id>', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'approval', False, 'bad_action')


async def adminsite_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='adminsite', required_role='admin')
    action = (context.args[0].lower() if context.args else 'show')
    site_url = os.getenv('EXTERNAL_ADMIN_SITE_URL', '').strip()
    api_url = os.getenv('EXTERNAL_ADMIN_API_URL', '').strip()
    if action == 'show':
        lines = ['<b>Внешняя admin control surface</b>', f"site: <code>{html.escape(site_url or '-')}</code>", f"api: <code>{html.escape(api_url or '-')}</code>"]
        await update.effective_message.reply_text('\n'.join(lines), parse_mode=cfg.telegram_parse_mode, disable_web_page_preview=True)
        record(db, update, 'adminsite', True, 'show')
        return
    if action == 'push':
        push = context.application.bot_data.get('push_external_admin_snapshot')
        if not push:
            await update.effective_message.reply_text('Внешняя admin sync не настроена.', parse_mode=cfg.telegram_parse_mode)
            record(db, update, 'adminsite', False, 'not_configured')
            return
        ok = await push(reason='manual', actor_user_id=update.effective_user.id if update.effective_user else 0)
        await update.effective_message.reply_text(f'External admin snapshot pushed: <b>{ok}</b>', parse_mode=cfg.telegram_parse_mode)
        record(db, update, 'adminsite', bool(ok), 'push')
        return
    await update.effective_message.reply_text('Использование: /adminsite show | push', parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'adminsite', False, 'bad_action')


async def ops_help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='opshelp', required_role='admin')
    text = '\n\n'.join([
        '<b>Ops</b>\n' + _ops_help_text('ops'),
        '<b>Admin</b>\n' + _ops_help_text('admin'),
        '<b>Delivery</b>\n' + _ops_help_text('delivery'),
        '<b>Security</b>\n' + _ops_help_text('security'),
    ])
    await update.effective_message.reply_text(text, parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'opshelp', True, 'shown')


async def delivery_help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='deliveryhelp', required_role='admin')
    await update.effective_message.reply_text(_ops_help_text('delivery'), parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'deliveryhelp', True, 'shown')


async def security_help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db = deps(context)
    await authorize(update, context, command='securityhelp', required_role='admin')
    await update.effective_message.reply_text(_ops_help_text('security'), parse_mode=cfg.telegram_parse_mode)
    record(db, update, 'securityhelp', True, 'shown')
