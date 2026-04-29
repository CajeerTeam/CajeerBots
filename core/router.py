from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field

from core.commands import CommandRegistry, build_default_commands
from core.events import CajeerEvent

logger = logging.getLogger(__name__)


@dataclass
class RouteResult:
    handled: bool
    handler: str
    details: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class EventRouter:
    def __init__(self, commands: CommandRegistry | None = None) -> None:
        self.commands = commands or build_default_commands()

    async def route(self, event: CajeerEvent) -> RouteResult:
        if event.type == "command.received":
            command_name = str(event.payload.get("command", "")).strip().lstrip("/")
            if not command_name:
                return RouteResult(False, "commands", {"error": "команда не указана"})
            result = await self.commands.dispatch(command_name, event)
            return RouteResult(bool(result.get("ok")), "commands", result)

        if event.type.startswith("adapter."):
            logger.info("служебное событие адаптера: %s", event.type)
            return RouteResult(True, "system.adapter", {"type": event.type})

        if event.type.startswith("message."):
            return RouteResult(False, "message", {"reason": "для сообщения не назначен модуль-обработчик"})

        if event.type.startswith("plugin."):
            return RouteResult(False, "plugin", {"reason": "для события плагина не назначен обработчик"})

        return RouteResult(False, "unknown", {"type": event.type})
