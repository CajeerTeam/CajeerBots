from __future__ import annotations

import logging
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from nmbot.config import BotConfig
from nmbot.database import ChatSettings, Database, InteractionRecord
from nmbot.services.access import enforce_rate_limit, guard_access
from nmbot.services.server_api import ServerStatusClient
from nmbot.templates import announcement_text, maintenance_text, status_text

logger = logging.getLogger(__name__)


def deps(context: ContextTypes.DEFAULT_TYPE, *, with_status: bool = False):
    cfg: BotConfig = context.application.bot_data["config"]
    db: Database = context.application.bot_data["db"]
    if with_status:
        status_client: ServerStatusClient = context.application.bot_data["status_client"]
        return cfg, db, status_client
    return cfg, db


async def authorize(update: Update, context: ContextTypes.DEFAULT_TYPE, *, command: str, required_role: str = "user", expensive: bool = False) -> ChatSettings | None:
    cfg: BotConfig = context.application.bot_data["config"]
    db: Database = context.application.bot_data["db"]
    chat = update.effective_chat
    user = update.effective_user
    if chat is not None:
        db.touch_chat(chat_id=chat.id, title=get_chat_title(chat), chat_type=chat.type)
        if chat.type != 'private' and db.get_onboarding_status(chat.id) is None:
            db.set_onboarding_status(chat_id=chat.id, status='pending', title=get_chat_title(chat), chat_type=chat.type, updated_by=str(user.id if user else 'system'))
    guard_access(cfg, db, chat=chat, user_id=user.id if user else None, required_role=required_role, command=command)
    enforce_rate_limit(context, user_id=user.id, chat_id=chat.id, command=command, cooldown_seconds=(cfg.status_command_cooldown_seconds if expensive else cfg.command_cooldown_seconds))
    logger.info("Authorized command /%s from user_id=%s chat_id=%s role=%s", command, user.id, chat.id, cfg.role_for_user(user.id))
    return db.get_chat_settings(chat.id) if chat else None


def get_chat_title(chat) -> str:
    return getattr(chat, "title", None) or getattr(chat, "full_name", None) or str(chat.id)


def get_maintenance_banner(db: Database) -> str:
    state = db.get_maintenance_state()
    if not state.get('active'):
        return ''
    message = str(state.get('message') or '')
    try:
        cfg = None
    except Exception:
        cfg = None
    title = '⚠️ Техработы'
    return f"{title}\n{message}".strip()


async def reply_help(update: Update, context: ContextTypes.DEFAULT_TYPE, cfg: BotConfig) -> None:
    user_id = update.effective_user.id if update.effective_user else None
    lines = [
        "<b>Команды</b>",
        "/start — стартовое сообщение",
        "/help — список команд",
        "/status — статус NeverMine",
        "/online — онлайн игроков",
        "/links — ссылки проекта",
        "/stats — базовая статистика бота",
        "/link — статус привязки",
        "/link request &lt;PlayerName&gt; — запросить привязку",
        "/link unlink — отвязать аккаунт",
        "/me — linked account и пользовательские настройки",
        "/sessions — мои security sessions",
        "/2fa status — статус 2FA/security",
        "/security status|recover|sessions|revoke|revoke-all|trusted-clear",
        "/notifications show|set ... — пользовательские уведомления",
        "Русский язык используется всегда",
        "/quiethours show|set &lt;start&gt; &lt;end&gt; [timezone]",
    ]
    if cfg.has_role(user_id, "admin"):
        lines.extend([
            "",
            "<b>Админ-команды</b>",
            "/health — deep health check",
            "/adminstats — расширенная статистика",
            "/announce [media=kind:url] [thread=id] -- &lt;text&gt;",
            "/broadcast [scope=all|current|private|groups] [tags=a,b] [media=kind:url] [thread=id] -- &lt;text&gt;",
            "/schedule list | cancel &lt;id&gt; | requeue &lt;id&gt; | dlq | resolve &lt;id&gt; | at=YYYY-MM-DDTHH:MM [...] -- &lt;text&gt;",
            "/chatsettings show | list | set | bulk",
            "/pullannouncements — ручной импорт внешнего feed",
            "/maintenance on|off|status [message]",
            "/template preview|validate",
            "/rbac show|set|reset",
            "/webhook status|refresh|delete|set|reconcile",
            "/security status|approve|deny|recover|sessions|revoke|revoke-all|trusted-clear|pending",
            "/subscribe &lt;tag1,tag2&gt; — подписка на теги чата",
            "/unsubscribe &lt;tag1,tag2|all&gt; — убрать подписку на теги",
            "/onboarding list|wizard|approve|reject|status",
            "/metrics — runtime metrics",
            "/delivery status|pause|resume|dry-run",
            "/alerts list|ack|mute|resolve",
            "/approval list|approve|reject",
            "/adminsite show|push",
            "/timezone show|set &lt;Europe/Berlin&gt;",
            "/mode status|prepare &lt;polling|webhook&gt;",
            "/incident snapshot",
            "/link approve|reject|pending|history|revoke|cleanup",
        ])
    await update.effective_message.reply_text("\n".join(lines), parse_mode=cfg.telegram_parse_mode)


def root_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Статус", callback_data="menu:status"), InlineKeyboardButton("Онлайн", callback_data="menu:online")],
        [InlineKeyboardButton("Ссылки", callback_data="menu:links"), InlineKeyboardButton("Помощь", callback_data="menu:help")],
    ])


def links_keyboard(cfg: BotConfig) -> InlineKeyboardMarkup | None:
    buttons = []
    for label, url in cfg.links.items():
        if url:
            buttons.append([InlineKeyboardButton(label, url=url)])
    return InlineKeyboardMarkup(buttons) if buttons else None


def broadcast_confirmation_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("Подтвердить", callback_data="broadcast:confirm"), InlineKeyboardButton("Отменить", callback_data="broadcast:cancel")]])


def format_status_text(cfg: BotConfig, status, *, maintenance_message: str = '') -> str:
    return status_text(cfg, status, maintenance_message=maintenance_message)


def format_announcement_text(cfg: BotConfig, author: str, text: str, *, tag: str = "") -> str:
    return announcement_text(cfg, author=author, text=text, tag=tag)


def utc_now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def record(db: Database, update: Update | None, command: str, ok: bool, details: str) -> None:
    chat_id = update.effective_chat.id if update and update.effective_chat else None
    user = update.effective_user if update else None
    db.record_interaction(InteractionRecord(chat_id=chat_id, user_id=user.id if user else None, username=user.username if user else None, command=command, ok=ok, details=details))


def paginate_lines(lines: list[str], *, page: int = 1, per_page: int = 20, title: str = '') -> str:
    page = max(1, page)
    per_page = max(1, per_page)
    if not lines:
        return title or ''
    start = (page - 1) * per_page
    end = start + per_page
    body = lines[start:end]
    pages = (len(lines) + per_page - 1) // per_page
    prefix = [title] if title else []
    prefix.extend(body)
    prefix.append(f'<i>page {page}/{pages}</i>')
    return '\n'.join(prefix)
