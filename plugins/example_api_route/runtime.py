from __future__ import annotations

from core.sdk import PluginBase
from core.sdk.plugins import PluginRequest, PluginRoute


class ExampleApiRoutePlugin(PluginBase):
    id = "example_api_route"

    def register_api_routes(self, context) -> list[PluginRoute]:
        return [PluginRoute("GET", "/plugins/example-api-route", "Пример route из плагина", "system.read", handler="handle_api_route")]

    async def handle_api_route(self, request: PluginRequest, context) -> dict[str, object]:
        context.require("api.route.register")
        return {
            "ok": True,
            "plugin": self.id,
            "path": request.path,
            "method": request.method,
            "config": context.safe_config(),
        }
