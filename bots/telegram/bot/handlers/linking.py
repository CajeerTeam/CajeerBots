from __future__ import annotations

import html
import json
import secrets
from datetime import datetime, timedelta

from telegram import Update
from telegram.ext import ContextTypes

from nmbot.handlers.shared import authorize, deps, record, paginate_lines


def _format_history(rows: list[dict], *, page: int = 1) -> str:
    if not rows:
        return "Link history пуста."
    body = []
    for row in rows:
        body.append(
            f"<code>{html.escape(str(row['created_at']))}</code> | <b>{html.escape(str(row['event']))}</b> | "
            f"user=<code>{row.get('user_id') or '-'}</code> | player=<b>{html.escape(str(row.get('player_name') or '-'))}</b> | code=<code>{html.escape(str(row.get('code') or '-'))}</code>"
        )
    return paginate_lines(body, page=page, per_page=10, title='<b>Link history</b>')


async def link_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg, db, status_client = deps(context, with_status=True)
    await authorize(update, context, command="link")
    user = update.effective_user
    linked = db.get_linked_account(user_id=user.id)
    raw_args = list(context.args)
    action = raw_args[0].lower() if raw_args else ""

    if not raw_args or action == "status":
        pending = db.get_pending_link_by_user(user_id=user.id)
        lines = ["<b>Link status</b>"]
        if linked:
            lines.append(f"Игровой аккаунт: <b>{html.escape(linked.player_name)}</b>")
            if linked.player_uuid:
                lines.append(f"UUID: <code>{html.escape(linked.player_uuid)}</code>")
            lines.append(f"Связан с: <code>{html.escape(linked.linked_at)}</code>")
        else:
            lines.append("Аккаунт пока не привязан.")
        if pending:
            lines.append(f"Ожидающий код: <code>{html.escape(pending.code)}</code>")
            lines.append(f"Requested player: <b>{html.escape(pending.player_name)}</b>")
            lines.append(f"Expires at (UTC): <code>{html.escape(pending.expires_at)}</code>")
            if pending.verified_at:
                lines.append(f"Verified at: <code>{html.escape(pending.verified_at)}</code>")
        lines.append("Использование: /link request <PlayerName> | /link unlink")
        if cfg.has_role(user.id, "admin"):
            lines.append("Admin: /link pending | history [limit] | approve <CODE> [player_uuid] | reject <CODE> | revoke <user_id> | cleanup")
        await update.effective_message.reply_text("\n".join(lines), parse_mode=cfg.telegram_parse_mode)
        record(db, update, "link", True, "status")
        return

    if action == "pending":
        await authorize(update, context, command="link", required_role="admin")
        rows = db.list_pending_links(limit=20)
        if not rows:
            text = "Ожидающих привязок нет."
        else:
            text = "<b>Ожидающие привязки</b>\n" + "\n".join(
                f"<code>{html.escape(item.code)}</code> | user=<code>{item.user_id}</code> | player=<b>{html.escape(item.player_name)}</b> | expires=<code>{html.escape(item.expires_at)}</code>"
                for item in rows
            )
        await update.effective_message.reply_text(text, parse_mode=cfg.telegram_parse_mode)
        record(db, update, "link", True, "pending")
        return

    if action == "history":
        await authorize(update, context, command="link", required_role="admin")
        limit = 50
        page = 1
        for arg in raw_args[1:]:
            if arg.startswith('page='):
                try:
                    page = max(1, int(arg.split('=',1)[1]))
                except ValueError:
                    page = 1
            else:
                try:
                    limit = max(1, min(200, int(arg)))
                except ValueError:
                    pass
        await update.effective_message.reply_text(_format_history(db.list_link_events(limit=limit), page=page), parse_mode=cfg.telegram_parse_mode)
        record(db, update, "link", True, f"history:{page}")
        return

    if action == "cleanup":
        await authorize(update, context, command="link", required_role="admin")
        counters = db.cleanup(
            interaction_retention_days=cfg.interaction_retention_days,
            admin_action_retention_days=cfg.admin_action_retention_days,
            runtime_state_retention_days=cfg.runtime_state_retention_days,
            dead_letter_retention_days=cfg.dead_letter_retention_days,
        )
        await update.effective_message.reply_text(f"Cleanup выполнен. expired pending links: <b>{counters.get('pending_links', 0)}</b>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "link", True, "cleanup")
        return

    if action == "unlink":
        changed = db.unlink_account(user_id=user.id, admin_user_id=user.id)
        await update.effective_message.reply_text("Привязка удалена." if changed else "Аккаунт не был привязан.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "link", changed, "unlink")
        return

    if action == "revoke":
        await authorize(update, context, command="link", required_role="admin")
        if len(raw_args) < 2:
            await update.effective_message.reply_text("Использование: /link revoke <user_id>", parse_mode=cfg.telegram_parse_mode)
            record(db, update, "link", False, "revoke_missing_user")
            return
        try:
            target_user_id = int(raw_args[1])
        except ValueError:
            await update.effective_message.reply_text("user_id должен быть integer", parse_mode=cfg.telegram_parse_mode)
            record(db, update, "link", False, "revoke_bad_user")
            return
        changed = db.unlink_account(user_id=target_user_id, admin_user_id=user.id)
        await update.effective_message.reply_text("Привязка отозвана." if changed else "Аккаунт не был привязан.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "link", changed, "revoke")
        return

    if action == "reject":
        await authorize(update, context, command="link", required_role="admin")
        if len(raw_args) < 2:
            await update.effective_message.reply_text("Использование: /link reject <CODE>", parse_mode=cfg.telegram_parse_mode)
            record(db, update, "link", False, "reject_missing_code")
            return
        ok = db.reject_pending_link(code=raw_args[1].upper(), admin_user_id=user.id)
        await update.effective_message.reply_text("Запрос отклонён." if ok else "Код не найден.", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "link", ok, "reject")
        return

    if action == "approve":
        await authorize(update, context, command="link", required_role="admin")
        if len(raw_args) < 2:
            await update.effective_message.reply_text("Использование: /link approve <CODE> [player_uuid]", parse_mode=cfg.telegram_parse_mode)
            record(db, update, "link", False, "approve_missing_code")
            return
        code = raw_args[1].upper()
        pending = db.get_pending_link_by_code(code=code)
        if pending is None:
            await update.effective_message.reply_text("Код не найден или уже использован.", parse_mode=cfg.telegram_parse_mode)
            record(db, update, "link", False, "approve_not_found")
            return
        player_uuid = raw_args[2] if len(raw_args) > 2 else None
        if status_client.link_verify_is_configured() and cfg.link_verify_auto_approve:
            try:
                verified = await status_client.verify_link_code(code=code, telegram_user_id=pending.user_id)
            except Exception as exc:
                await update.effective_message.reply_text(f"Ошибка server verification: <code>{html.escape(str(exc))}</code>", parse_mode=cfg.telegram_parse_mode)
                record(db, update, "link", False, "verify_error")
                return
            if not verified.ok:
                await update.effective_message.reply_text("Server verification вернул отказ.", parse_mode=cfg.telegram_parse_mode)
                record(db, update, "link", False, "verify_rejected")
                return
            payload = json.dumps(verified.raw or {}, ensure_ascii=False)
            db.mark_pending_link_verified(code=code, payload=payload)
            if verified.player_uuid:
                player_uuid = verified.player_uuid
        linked_account = db.link_account(code=code, player_uuid=player_uuid)
        if linked_account is None:
            await update.effective_message.reply_text("Код не найден или уже использован.", parse_mode=cfg.telegram_parse_mode)
            record(db, update, "link", False, "approve_not_found_postcheck")
            return
        db.record_admin_action(chat_id=update.effective_chat.id, user_id=user.id, action="link_approve", payload=code)
        db.record_link_event(
            event="approve_admin",
            code=code,
            user_id=linked_account.user_id,
            admin_user_id=user.id,
            player_name=linked_account.player_name,
            player_uuid=linked_account.player_uuid,
            details="approved via telegram admin",
        )
        await update.effective_message.reply_text(f"Привязка подтверждена: <b>{html.escape(linked_account.player_name)}</b>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "link", True, "approve")
        return

    if action == "request":
        raw_args = raw_args[1:]
    player_name = " ".join(raw_args).strip()
    if not player_name:
        await update.effective_message.reply_text("Использование: /link request <PlayerName>", parse_mode=cfg.telegram_parse_mode)
        record(db, update, "link", False, "missing_player_name")
        return
    code = secrets.token_hex(4).upper()
    expires_at = (datetime.utcnow() + timedelta(seconds=cfg.link_code_ttl_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    db.create_pending_link(
        code=code,
        user_id=user.id,
        chat_id=update.effective_chat.id if update.effective_chat else None,
        username=user.username,
        player_name=player_name,
        expires_at=expires_at,
    )
    text = (
        "<b>Link request created</b>\n"
        f"Player: <b>{html.escape(player_name)}</b>\n"
        f"One-time code: <code>{code}</code>\n"
        f"Expires at (UTC): <code>{expires_at}</code>\n\n"
        "Передайте код в NeverMine / community-core для подтверждения.\n"
    )
    if status_client.link_verify_is_configured():
        text += "Включена server verification: после подтверждения код будет проверен через API.\n"
    text += "Администратор может завершить привязку командой <code>/link approve &lt;CODE&gt; [player_uuid]</code>."
    await update.effective_message.reply_text(text, parse_mode=cfg.telegram_parse_mode)
    record(db, update, "link", True, f"request:{player_name}")
