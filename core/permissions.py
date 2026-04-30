from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Permission:
    key: str
    description: str


CORE_PERMISSIONS = [
    Permission("bots.runtime.run", "Запуск runtime Cajeer Bots"),
    Permission("bots.runtime.restart", "Перезапуск адаптеров ботов"),
    Permission("bots.modules.configure", "Включение и отключение модулей"),
    Permission("bots.plugins.configure", "Включение и отключение плагинов"),
    Permission("bots.events.read", "Чтение событий платформы"),
    Permission("bots.events.retry", "Повторная обработка ошибочных событий"),
    Permission("bots.logs.read", "Чтение журналов"),
    Permission("bots.announce.create", "Создание объявления"),
    Permission("bots.announce.publish", "Публикация объявления"),
    Permission("bots.announce.manage", "Управление объявлениями"),
    Permission("bots.support.create", "Создание обращения поддержки"),
    Permission("bots.support.reply", "Ответы на обращения поддержки"),
    Permission("bots.support.assign", "Назначение обращений поддержки"),
    Permission("bots.support.manage", "Управление обращениями поддержки"),
    Permission("bots.moderation.warn", "Модерация: предупреждение"),
    Permission("bots.moderation.mute", "Модерация: mute"),
    Permission("bots.moderation.ban", "Модерация: ban"),
    Permission("bots.moderation.kick", "Модерация: kick"),
    Permission("bots.moderation.manage", "Полное управление модерацией"),
]

PERMISSIONS = {item.key for item in CORE_PERMISSIONS}


def has_permission(grants: set[str], permission: str) -> bool:
    return "*" in grants or permission in grants


def grants_from_event(event: Any) -> set[str]:
    payload = getattr(event, "payload", {}) or {}
    if not isinstance(payload, dict):
        return set()
    raw = payload.get("rbac_grants") or payload.get("permissions") or payload.get("grants") or []
    if isinstance(raw, str):
        return {item.strip() for item in raw.split(",") if item.strip()}
    if isinstance(raw, (list, tuple, set)):
        return {str(item).strip() for item in raw if str(item).strip()}
    return set()
