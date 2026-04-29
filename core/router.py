from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any

from core.commands import CommandRegistry, build_default_commands
from core.events import CajeerEvent
from core.responses import response_from_result

logger = logging.getLogger(__name__)


@dataclass
class RouteResult:
    handled: bool
    handler: str
    details: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class EventRouter:
    def __init__(
        self,
        commands: CommandRegistry | None = None,
        idempotency: Any | None = None,
        components: Any | None = None,
    ) -> None:
        self.commands = commands or build_default_commands()
        self.idempotency = idempotency
        self.components = components
        self.history: list[RouteResult] = []

    async def route(self, event: CajeerEvent) -> RouteResult:
        if self.idempotency is not None and self.idempotency.seen(event.event_id):
            return self._remember(RouteResult(True, "idempotency", {"skipped": True, "event_id": event.event_id}))

        if event.type == "command.received":
            command_name = str(event.payload.get("command", "")).strip().lstrip("/")
            if not command_name:
                result = RouteResult(False, "commands", {"error": "команда не указана", "message": "Команда не указана."})
            else:
                component_result = None
                if self.components is not None:
                    component_result = await self.components.route_command(command_name, event)
                if component_result:
                    result = RouteResult(True, "component", component_result)
                else:
                    details = await self.commands.dispatch(command_name, event)
                    result = RouteResult(bool(details.get("ok")), "commands", details)
            if result.handled:
                await self._emit_command_response(event, result)
            return self._remember(result)

        if event.type == "command.response":
            return self._remember(RouteResult(True, "delivery.response", {"queued": True}))

        if event.type.startswith("adapter."):
            logger.info("служебное событие адаптера: %s", event.type)
            return self._remember(RouteResult(True, "system.adapter", {"type": event.type}))

        if event.type.startswith("message."):
            if self.components is not None:
                component_result = await self.components.route_event(event)
                if component_result:
                    return self._remember(RouteResult(True, "component", component_result))
            return self._remember(RouteResult(False, "message", {"reason": "для сообщения не назначен модуль-обработчик"}))

        if event.type.startswith("plugin."):
            return self._remember(RouteResult(False, "plugin", {"reason": "для события плагина не назначен обработчик"}))

        return self._remember(RouteResult(False, "unknown", {"type": event.type}))

    async def _emit_command_response(self, event: CajeerEvent, result: RouteResult) -> None:
        runtime = getattr(self.components, "runtime", None)
        if runtime is None:
            return
        response = response_from_result(event, result.details)
        if response is None:
            return
        response_event = CajeerEvent.create(
            source=event.source,
            type="command.response",
            actor=event.actor,
            chat=event.chat,
            trace_id=event.trace_id,
            payload=response.to_dict(),
        )
        await runtime.event_bus.publish(response_event)
        runtime.delivery.enqueue(response.adapter, response.chat_id, response.text, trace_id=response.trace_id)
        runtime.audit.write(
            actor_type="system",
            actor_id="router",
            action="command.response.enqueue",
            resource=response.adapter,
            trace_id=response.trace_id,
        )
        await runtime.delivery.process_once(runtime.adapter_map())

    def _remember(self, result: RouteResult) -> RouteResult:
        self.history.append(result)
        self.history = self.history[-500:]
        return result

    def snapshot(self) -> list[RouteResult]:
        return list(self.history)
