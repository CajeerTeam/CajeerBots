from __future__ import annotations

from core.sdk import PluginBase
from core.sdk.plugins import PluginRoute


class ExampleApiRoutePlugin(PluginBase):
    id = "example_api_route"

    def register_api_routes(self, context) -> list[PluginRoute]:
        return [PluginRoute("GET", "/plugins/example-api-route", "Пример route из плагина", "system.read")]
