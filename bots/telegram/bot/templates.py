from __future__ import annotations

import html
import string
from datetime import datetime
from zoneinfo import ZoneInfo

from nmbot.config import BotConfig

_DEFAULT_TEMPLATES = {
    "start": """<b>NeverMine Telegram</b>
Добро пожаловать в Telegram runtime проекта NeverMine.

Используйте кнопки ниже или /help для списка команд.""",
    "feed": """<b>NeverMine Feed</b>
{tag_block}{text}""",
    "announcement": """<b>NeverMine Announcement</b>
От: <b>{author}</b>
{tag_block}{text}""",
    "maintenance": "⚠️ Техработы\n{message}",
}
_ALLOWED_TEMPLATE_FIELDS = {
    'start': set(),
    'feed': {'text', 'tag_block'},
    'announcement': {'author', 'text', 'tag_block'},
    'maintenance': {'message'},
}
_REQUIRED_TEMPLATE_FIELDS = {
    'feed': {'text'},
    'announcement': {'author', 'text'},
    'maintenance': {'message'},
}


def _load_template(cfg: BotConfig, name: str) -> str:
    candidate = cfg.templates_dir / f"{name}.txt"
    if candidate.exists():
        return candidate.read_text(encoding="utf-8")
    return _DEFAULT_TEMPLATES[name]


def _template_fields(template: str) -> set[str]:
    fields: set[str] = set()
    for _, field_name, _, _ in string.Formatter().parse(template):
        if field_name:
            fields.add(field_name)
    return fields


def _validate_template_contract(cfg: BotConfig, name: str, template: str) -> list[str]:
    errors: list[str] = []
    found = _template_fields(template)
    allowed = _ALLOWED_TEMPLATE_FIELDS.get(name, set())
    required = _REQUIRED_TEMPLATE_FIELDS.get(name, set())
    unknown = sorted(found - allowed)
    missing = sorted(required - found)
    if unknown:
        errors.append(f"{name}: unknown placeholders: {', '.join(unknown)}")
    if missing:
        errors.append(f"{name}: missing placeholders: {', '.join(missing)}")
    if cfg.template_strict_mode and errors:
        raise ValueError('; '.join(errors))
    return errors


def start_text(cfg: BotConfig) -> str:
    template = _load_template(cfg, 'start')
    _validate_template_contract(cfg, 'start', template)
    return template


def announcement_text(cfg: BotConfig, *, author: str, text: str, tag: str = '') -> str:
    template = _load_template(cfg, 'announcement')
    _validate_template_contract(cfg, 'announcement', template)
    tag_block = f"Тег: <b>{html.escape(tag)}</b>\n" if tag else ''
    return template.format(author=html.escape(author), text=html.escape(text), tag_block=tag_block)


def feed_text(cfg: BotConfig, *, text: str, tag: str = '') -> str:
    template = _load_template(cfg, 'feed')
    _validate_template_contract(cfg, 'feed', template)
    tag_block = f"Тег: <b>{html.escape(tag)}</b>\n" if tag else ''
    return template.format(text=html.escape(text), tag_block=tag_block)


def maintenance_text(cfg: BotConfig, *, message: str) -> str:
    template = _load_template(cfg, 'maintenance')
    _validate_template_contract(cfg, 'maintenance', template)
    return template.format(message=html.escape(message))


def status_text(cfg: BotConfig, status, *, maintenance_message: str = '') -> str:
    now = datetime.now(ZoneInfo(cfg.bot_timezone)).strftime('%Y-%m-%d %H:%M:%S')
    lines = [
        '<b>Статус NeverMine</b>',
        f"Сервис: <b>{html.escape(status.server_name)}</b>",
        f"Состояние: <b>{'online' if status.online else 'offline'}</b>",
    ]
    if maintenance_message:
        lines.append(f"Техработы: <b>{html.escape(maintenance_message)}</b>")
    if status.players_online is not None:
        if status.max_players is not None:
            lines.append(f"Игроки: <b>{status.players_online}/{status.max_players}</b>")
        else:
            lines.append(f"Игроки online: <b>{status.players_online}</b>")
    if status.version:
        lines.append(f"Версия: <b>{html.escape(status.version)}</b>")
    if status.motd:
        lines.append(f"MOTD: <code>{html.escape(status.motd)}</code>")
    if status.latency_ms is not None:
        lines.append(f"Latency: <b>{status.latency_ms} ms</b>")
    lines.append(f"Проверено: <code>{now}</code>")
    return "\n".join(lines)


def validate_templates(cfg: BotConfig) -> list[str]:
    errors: list[str] = []
    for name in ('start', 'announcement', 'feed', 'maintenance'):
        try:
            template = _load_template(cfg, name)
            errors.extend(_validate_template_contract(cfg, name, template))
        except Exception as exc:
            errors.append(f"ru:{name}: {exc}")
    try:
        start_text(cfg)
        announcement_text(cfg, author='Bot', text='test', tag='news')
        feed_text(cfg, text='test', tag='news')
        maintenance_text(cfg, message='maintenance')
    except Exception as exc:
        errors.append(f"ru: render error: {exc}")
    return errors


def preview_template(cfg: BotConfig, name: str, *, text: str = '', author: str = 'NMTelegramBot', tag: str = '') -> str:
    if name == 'start':
        return start_text(cfg)
    if name == 'feed':
        return feed_text(cfg, text=text or 'Preview text', tag=tag)
    if name == 'announcement':
        return announcement_text(cfg, author=author, text=text or 'Preview text', tag=tag)
    if name == 'maintenance':
        return maintenance_text(cfg, message=text or 'Maintenance in progress')
    raise ValueError(f'Unknown template: {name}')
