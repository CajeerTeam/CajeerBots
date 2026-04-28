from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from nmbot.handlers.shared import record
from nmbot.services.access import PermissionDeniedError, RateLimitExceededError

logger = logging.getLogger(__name__)


async def permission_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.application.bot_data["config"]
    db = context.application.bot_data["db"]
    error = context.error
    if not isinstance(error, PermissionDeniedError):
        return
    reason = str(error)
    message = {
        "chat_not_allowed": "Этот чат не входит в список разрешённых.",
        "private_only": "Бот сейчас работает только в private chat режиме.",
        "groups_only": "Бот сейчас работает только в group mode.",
        "no_chat_or_user": "Не удалось определить chat/user контекст.",
        "chat_status_disabled": "В этом чате статус-команды отключены.",
        "chat_broadcasts_disabled": "В этом чате broadcast-команды отключены.",
        "role_required:mod": "Эта команда доступна только модераторам и выше.",
        "role_required:admin": "Эта команда доступна только администраторам и выше.",
        "role_required:owner": "Эта команда доступна только владельцам бота.",
    }.get(reason, "Доступ запрещён.")
    if isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text(message, parse_mode=cfg.telegram_parse_mode)
    record(db, update if isinstance(update, Update) else None, "permission_denied", False, reason)


async def rate_limit_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    cfg = context.application.bot_data["config"]
    db = context.application.bot_data["db"]
    error = context.error
    if not isinstance(error, RateLimitExceededError):
        return
    if isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text(
            f"Слишком часто. Повторите через ~{error.retry_after:.1f} сек.",
            parse_mode=cfg.telegram_parse_mode,
        )
    record(db, update if isinstance(update, Update) else None, "rate_limit", False, f"retry_after={error.retry_after:.2f}")


async def generic_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if isinstance(context.error, (PermissionDeniedError, RateLimitExceededError)):
        return
    cfg = context.application.bot_data["config"]
    db = context.application.bot_data["db"]
    logger.exception("Unhandled bot error", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        await update.effective_message.reply_text(
            "Внутренняя ошибка NMTelegramBot. Проверьте логи.",
            parse_mode=cfg.telegram_parse_mode,
        )
    record(db, update if isinstance(update, Update) else None, "exception", False, repr(context.error))
