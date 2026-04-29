from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Awaitable, Callable

Handler = Callable[[Any], Awaitable[dict[str, object]]]


@dataclass(frozen=True)
class CommandDefinition:
    name: str
    description: str
    module_id: str | None = None
    permission: str | None = None
    aliases: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class CommandRegistry:
    def __init__(self) -> None:
        self._commands: dict[str, CommandDefinition] = {}
        self._handlers: dict[str, Handler] = {}

    def register(self, command: CommandDefinition, handler: Handler | None = None) -> None:
        if command.name in self._commands:
            raise ValueError(f"команда уже зарегистрирована: {command.name}")
        self._commands[command.name] = command
        for alias in command.aliases:
            if alias in self._commands:
                raise ValueError(f"псевдоним команды уже зарегистрирован: {alias}")
            self._commands[alias] = command
        if handler is not None:
            self._handlers[command.name] = handler

    def list(self) -> list[CommandDefinition]:
        unique: dict[str, CommandDefinition] = {}
        for command in self._commands.values():
            unique[command.name] = command
        return sorted(unique.values(), key=lambda item: item.name)

    async def dispatch(self, command_name: str, event: Any) -> dict[str, object]:
        command = self._commands.get(command_name)
        if command is None:
            return {"ok": False, "message": f"Команда не найдена: {command_name}", "error": f"команда не найдена: {command_name}"}
        handler = self._handlers.get(command.name)
        if handler is None:
            return {"ok": False, "message": f"Для команды {command.name} не назначен обработчик", "error": f"для команды {command.name} не назначен обработчик"}
        return await handler(event)


def build_default_commands(runtime: Any | None = None) -> CommandRegistry:
    registry = CommandRegistry()

    async def help_handler(event: Any) -> dict[str, object]:
        names = ", ".join(command.name for command in registry.list())
        return {
            "ok": True,
            "message": f"Доступные команды: {names}",
            "commands": [command.to_dict() for command in registry.list()],
        }

    async def status_handler(event: Any) -> dict[str, object]:
        if runtime is None:
            return {"ok": True, "status": "каркас платформы доступен", "message": "Cajeer Bots: каркас платформы доступен."}
        ready = runtime.readiness_snapshot()
        status_text = "готов" if ready.get("ok") else "требует внимания"
        problems = ready.get("problems") or []
        message = f"Cajeer Bots {runtime.version}: {status_text}."
        if problems:
            message += " Проблемы: " + "; ".join(str(item) for item in problems[:5])
        return {
            "ok": True,
            "status": "работает",
            "message": message,
            "version": runtime.version,
            "readiness": ready,
            "adapters": [item.to_dict() for item in runtime.adapter_health_snapshot()],
        }

    async def support_handler(event: Any) -> dict[str, object]:
        return {"ok": True, "message": "Модуль поддержки подключён. Опишите обращение следующим сообщением."}

    async def announce_handler(event: Any) -> dict[str, object]:
        return {"ok": True, "message": "Команда объявлений принята. Доставка выполняется модулем announcements."}

    async def moderation_handler(event: Any) -> dict[str, object]:
        return {"ok": True, "message": "Инструменты модерации доступны через модуль moderation."}

    registry.register(CommandDefinition("help", "Показать список доступных команд.", aliases=("помощь",)), help_handler)
    registry.register(CommandDefinition("status", "Показать состояние платформы.", aliases=("статус",)), status_handler)
    registry.register(CommandDefinition("support", "Создать или просмотреть обращение в поддержку.", module_id="support"), support_handler)
    registry.register(CommandDefinition("announce", "Создать объявление для каналов доставки.", module_id="announcements"), announce_handler)
    registry.register(CommandDefinition("moderation", "Открыть инструменты модерации.", module_id="moderation"), moderation_handler)
    return registry
