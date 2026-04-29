from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


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
        self._handlers: dict[str, Any] = {}

    def register(self, command: CommandDefinition, handler: Any | None = None) -> None:
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
            return {"ok": False, "error": f"команда не найдена: {command_name}"}
        handler = self._handlers.get(command.name)
        if handler is None:
            return {"ok": False, "error": f"для команды {command.name} не назначен обработчик"}
        return await handler(event)


def build_default_commands() -> CommandRegistry:
    registry = CommandRegistry()
    registry.register(CommandDefinition("help", "Показать список доступных команд.", aliases=("помощь",)))
    registry.register(CommandDefinition("status", "Показать состояние платформы.", aliases=("статус",)))
    registry.register(CommandDefinition("support", "Создать или просмотреть обращение в поддержку.", module_id="support"))
    registry.register(CommandDefinition("announce", "Создать объявление для каналов доставки.", module_id="announcements"))
    registry.register(CommandDefinition("moderation", "Открыть инструменты модерации.", module_id="moderation"))
    return registry
