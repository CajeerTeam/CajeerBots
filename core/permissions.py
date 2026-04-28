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
]


def has_permission(grants: set[str], permission: str) -> bool:
    return "*" in grants or permission in grants
