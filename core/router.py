from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any

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
    def __init__(self, commands: CommandRegistry | None = None, idempotency: Any | None = None) -> None:
        self.commands = commands or build_default_commands()
        self.idempotency = idempotency
        self.history: list[RouteResult] = []

    async def route(self, event: CajeerEvent) -> RouteResult:
        if self.idempotency is not None and self.idempotency.seen(event.event_id):
            return self._remember(RouteResult(True, "idempotency", {"skipped": True, "event_id": event.event_id}))

        if event.type == "command.received":
            command_name = str(event.payload.get("command", "")).strip().lstrip("/")
            if not command_name:
                result = RouteResult(False, "commands", {"error": "команда не указана"})
            else:
                details = await self.commands.dispatch(command_name, event)
                result = RouteResult(bool(details.get("ok")), "commands", details)
            return self._remember(result)

        if event.type.startswith("adapter."):
            logger.info("служебное событие адаптера: %s", event.type)
            return self._remember(RouteResult(True, "system.adapter", {"type": event.type}))

        if event.type.startswith("message."):
            return self._remember(RouteResult(False, "message", {"reason": "для сообщения не назначен модуль-обработчик"}))

        if event.type.startswith("plugin."):
            return self._remember(RouteResult(False, "plugin", {"reason": "для события плагина не назначен обработчик"}))

        return self._remember(RouteResult(False, "unknown", {"type": event.type}))

    def _remember(self, result: RouteResult) -> RouteResult:
        self.history.append(result)
        self.history = self.history[-500:]
        return result

    def snapshot(self) -> list[RouteResult]:
        return list(self.history)
