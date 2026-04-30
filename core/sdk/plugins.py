from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from core.events import CajeerEvent

PluginHandler = Callable[[CajeerEvent, Any], Awaitable[dict[str, object] | None]]


@dataclass(frozen=True)
class PluginRoute:
    method: str
    path: str
    summary: str
    auth_scope: str = "system.admin"


@dataclass
class PluginContext:
    runtime: Any
    manifest: Any
    logger: Any
    state: dict[str, object] = field(default_factory=dict)


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
