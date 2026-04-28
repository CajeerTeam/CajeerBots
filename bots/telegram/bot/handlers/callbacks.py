from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes

from nmbot.handlers.admin import confirm_broadcast
from nmbot.handlers.public import help_handler, links_handler, online_handler, status_handler


async def menu_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None or not query.data:
        return
    db = context.application.bot_data['db']
    cfg = context.application.bot_data['config']
    if not db.claim_idempotency_key(f'callback:{query.id}', ttl_seconds=300):
        await query.answer('Уже обработано', show_alert=False)
        return
    await query.answer()
    data = query.data
    if data.startswith('menu:'):
        action = data.split(':', 1)[1]
        if action == 'help':
            await help_handler(update, context)
        elif action == 'status':
            await status_handler(update, context)
        elif action == 'online':
            await online_handler(update, context)
        elif action == 'links':
            await links_handler(update, context)
        return
    if data == 'broadcast:confirm':
        await confirm_broadcast(update, context)
        return
    if data == 'broadcast:cancel':
        user_id = update.effective_user.id if update.effective_user else None
        if user_id is not None:
            db.clear_pending_broadcast(user_id=user_id)
        if update.effective_message:
            await update.effective_message.reply_text('Ожидающий broadcast отменён.', parse_mode=cfg.telegram_parse_mode)
        return
    if data.startswith('security:'):
        # security:approve:<challenge_id> / security:deny:<challenge_id>
        parts = data.split(':', 2)
        if len(parts) == 3:
            action, challenge_id = parts[1], parts[2]
            status_client = context.application.bot_data['status_client']
            actor_user_id = update.effective_user.id if update.effective_user else 0
            result = await status_client.act_2fa_challenge(challenge_id=challenge_id, action=action, actor_user_id=actor_user_id)
            if hasattr(db, 'mark_security_challenge_notice'):
                db.mark_security_challenge_notice(challenge_id, status='approved' if action == 'approve' else 'denied', actor_user_id=actor_user_id)
            if update.effective_message:
                await update.effective_message.reply_text(f"Security action: <b>{result.ok}</b>\n<code>{result.message or '-'}</code>", parse_mode=cfg.telegram_parse_mode)
        return
    if data.startswith('onboarding:'):
        parts = data.split(':')
        if len(parts) >= 4:
            action, chat_id, value = parts[1], int(parts[2]), parts[3]
            state = db.get_json_state(f'onboarding:wizard:{chat_id}', default={'chat_id': chat_id, 'tags': []}) or {'chat_id': chat_id, 'tags': []}
            if action == 'lang':
                pass
            elif action == 'tag':
                tags = set(state.get('tags') or [])
                tags.add(value)
                state['tags'] = sorted(tags)
            db.set_json_state(f'onboarding:wizard:{chat_id}', state)
            await query.edit_message_text(f"Wizard chat={chat_id}\ntags={','.join(state.get('tags', [])) or '-'}", reply_markup=query.message.reply_markup)
            return
        if len(parts) == 3 and parts[1] == 'done':
            chat_id = int(parts[2])
            state = db.get_json_state(f'onboarding:wizard:{chat_id}', default={}) or {}
            if state:
                db.update_chat_settings(chat_id, tags=state.get('tags', []))
                db.set_onboarding_status(chat_id=chat_id, status='approved', title='', chat_type='', updated_by=str(update.effective_user.id if update.effective_user else ''))
                await query.edit_message_text(f"Onboarding applied for chat {chat_id}.", parse_mode=cfg.telegram_parse_mode)
            return
