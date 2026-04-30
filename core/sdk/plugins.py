from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping

from core.events import CajeerEvent
from core.sdk.permissions import PermissionSet

PluginHandler = Callable[[CajeerEvent, Any], Awaitable[dict[str, object] | None]]


class PluginPermissionError(PermissionError):
    """Raised when a plugin uses a capability that is not declared in plugin.json."""


@dataclass(frozen=True)
class PluginRoute:
    method: str
    path: str
    summary: str
    auth_scope: str = "system.admin"
    handler: str = "handle_api_route"
    request_schema: dict[str, object] = field(default_factory=dict)
    response_schema: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class PluginRequest:
    method: str
    path: str
    body: dict[str, object] = field(default_factory=dict)
    actor: str = "plugin-api"
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass
class RegisteredPluginRoute:
    plugin_id: str
    route: PluginRoute
    instance: Any
    context: "PluginContext"

    @property
    def method(self) -> str:
        return self.route.method.upper()

    @property
    def path(self) -> str:
        return self.route.path

    @property
    def auth_scope(self) -> str:
        return self.route.auth_scope

    @property
    def summary(self) -> str:
        return self.route.summary

    def to_dict(self) -> dict[str, object]:
        return {
            "plugin_id": self.plugin_id,
            "method": self.method,
            "path": self.path,
            "summary": self.summary,
            "auth_scope": self.auth_scope,
            "handler": self.route.handler,
        }

    async def call(self, request: PluginRequest) -> dict[str, object] | str:
        self.context.require("api.route.register")
        handler = getattr(self.instance, self.route.handler, None)
        if handler is None:
            return {"ok": True, "plugin": self.plugin_id, "route": self.path}
        result = handler(request, self.context)
        if inspect.isawaitable(result):
            result = await result
        if isinstance(result, str):
            return result
        if isinstance(result, dict):
            return result
        return {"ok": True, "plugin": self.plugin_id, "result": result}


class PluginEventApi:
    def __init__(self, context: "PluginContext") -> None:
        self.context = context

    async def publish(self, event: CajeerEvent) -> None:
        self.context.require("events.publish")
        await self.context.runtime.event_bus.publish(event)

    def snapshot(self) -> list[CajeerEvent]:
        self.context.require("events.read")
        return list(self.context.runtime.event_bus.snapshot())


class PluginDeliveryApi:
    def __init__(self, context: "PluginContext") -> None:
        self.context = context

    async def enqueue(self, *, adapter: str, target: str, text: str, max_attempts: int = 3, trace_id: str | None = None):
        self.context.require("delivery.enqueue")
        return await self.context.runtime.delivery.enqueue_async(adapter=adapter, target=target, text=text, max_attempts=max_attempts, trace_id=trace_id)


class PluginAuditApi:
    def __init__(self, context: "PluginContext") -> None:
        self.context = context

    def write(self, **kwargs: object) -> None:
        self.context.require("audit.write")
        self.context.runtime.audit.write(actor_type="plugin", actor_id=self.context.manifest.id, **kwargs)


@dataclass
class PluginContext:
    runtime: Any
    manifest: Any
    logger: Any
    state: dict[str, object] = field(default_factory=dict)
    permissions: PermissionSet | None = None

    def __post_init__(self) -> None:
        if self.permissions is None:
            object.__setattr__(self, "permissions", PermissionSet.from_iterable(tuple(getattr(self.manifest, "permissions", ()) or ())))
        object.__setattr__(self, "events", PluginEventApi(self))
        object.__setattr__(self, "delivery", PluginDeliveryApi(self))
        object.__setattr__(self, "audit", PluginAuditApi(self))

    def has_permission(self, permission: str) -> bool:
        return bool(self.permissions and self.permissions.allows(permission))

    def require(self, permission: str) -> None:
        if not self.has_permission(permission):
            raise PluginPermissionError(f"plugin {getattr(self.manifest, 'id', '<unknown>')} не имеет permission {permission}")

    def safe_config(self) -> dict[str, object]:
        self.require("config.read")
        return dict(self.runtime.settings.safe_summary())


class PluginBase:
    id = "plugin"

    async def on_install(self, context: PluginContext) -> None:
        return None

    async def on_enable(self, context: PluginContext) -> None:
        return None

    async def on_disable(self, context: PluginContext) -> None:
        return None

    async def on_upgrade(self, context: PluginContext) -> None:
        return None

    async def on_uninstall(self, context: PluginContext) -> None:
        return None

    async def on_start(self, context: PluginContext) -> None:
        return None

    async def on_event(self, event: CajeerEvent, context: PluginContext) -> dict[str, object] | None:
        return None

    async def on_command(self, command: str, event: CajeerEvent, context: PluginContext) -> dict[str, object] | None:
        return None

    async def on_stop(self, context: PluginContext) -> None:
        return None

    def register_api_routes(self, context: PluginContext) -> list[PluginRoute]:
        return []

    def register_scheduled_jobs(self, context: PluginContext) -> list[dict[str, object]]:
        return []
