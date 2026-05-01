from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from typing import Any

from core.commands import CommandRegistry, build_default_commands
from core.events import CajeerEvent
from core.permissions import grants_from_event, has_permission
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
        if self.idempotency is not None:
            try:
                duplicate = await self.idempotency.seen_async(event.event_id)
            except AttributeError:
                duplicate = self.idempotency.seen(event.event_id)
            if duplicate:
                return self._remember(RouteResult(True, "idempotency", {"skipped": True, "event_id": event.event_id}))

        if event.type == "command.received":
            command_name = str(event.payload.get("command", "")).strip().lstrip("/")
            if not command_name:
                result = RouteResult(False, "commands", {"error": "команда не указана", "message": "Команда не указана."})
            else:
                command_definition = self.commands.get(command_name)
                permission = command_definition.permission if command_definition else None
                if permission and not await self._permission_allowed(event, permission):
                    result = RouteResult(False, "rbac", {
                        "ok": False,
                        "error": "permission_denied",
                        "permission": permission,
                        "message": f"Недостаточно прав для команды /{command_name}.",
                    })
                else:
                    component_result = None
                    if self.components is not None:
                        component_result = await self.components.route_command(command_name, event)
                    if component_result:
                        result = RouteResult(True, "component", component_result)
                    else:
                        details = await self.commands.dispatch(command_name, event)
                        result = RouteResult(bool(details.get("ok")), "commands", details)
            if result.handled or result.handler == "rbac":
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

    async def _permission_allowed(self, event: CajeerEvent, permission: str) -> bool:
        runtime = getattr(self.components, "runtime", None)
        if runtime is not None and getattr(runtime, "rbac_store", None) is not None:
            decide_async = getattr(runtime.rbac_store, "decide_async", None)
            decision = await decide_async(event, permission) if decide_async is not None else runtime.rbac_store.decide(event, permission)
            runtime.audit.write(actor_type="system", actor_id="rbac", action="rbac.decision", resource=permission, result="allow" if decision.allowed else "deny", trace_id=event.trace_id, message=decision.source)
            if not decision.allowed:
                runtime.audit.write(actor_type="system", actor_id="rbac", action="rbac.denied", resource=permission, result="denied", trace_id=event.trace_id, message=decision.source)
            return decision.allowed
        return has_permission(grants_from_event(event), permission)

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
        await runtime.delivery.enqueue_async(response.adapter, response.chat_id, response.text, trace_id=response.trace_id)
        runtime.audit.write(
            actor_type="system",
            actor_id="router",
            action="command.response.enqueue",
            resource=response.adapter,
            trace_id=response.trace_id,
        )
        adapters = runtime.adapter_map()
        if adapters:
            await runtime.delivery.process_once(adapters)

    def _remember(self, result: RouteResult) -> RouteResult:
        self.history.append(result)
        self.history = self.history[-500:]
        return result

    def snapshot(self) -> list[RouteResult]:
        return list(self.history)
