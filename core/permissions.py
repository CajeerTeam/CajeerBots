from dataclasses import dataclass


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
    Permission("bots.announce.create", "Создание объявлений"),
    Permission("bots.support.reply", "Ответы на обращения поддержки"),
    Permission("bots.moderation.manage", "Управление модерацией"),
]


def has_permission(grants: set[str], permission: str) -> bool:
    return "*" in grants or permission in grants


def grants_from_event(event) -> set[str]:  # type: ignore[no-untyped-def]
    payload = getattr(event, "payload", {}) or {}
    raw = payload.get("permissions") or payload.get("grants") or []
    if isinstance(raw, str):
        return {item.strip() for item in raw.split(",") if item.strip()}
    if isinstance(raw, (list, tuple, set)):
        return {str(item).strip() for item in raw if str(item).strip()}
    return set()
