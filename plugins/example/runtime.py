from __future__ import annotations

from core.events import CajeerEvent


class ExamplePlugin:
    id = "example_plugin"

    async def on_start(self, context) -> None:
        context.logger.info("пример плагина запущен")

    async def on_event(self, event: CajeerEvent, context) -> dict[str, object] | None:
        if event.type != "plugin.example.ping":
            return None
        message = context.manifest.settings_schema.get("enabled_message", {}).get("default", "Пример плагина включён.") if context.manifest.settings_schema else "Пример плагина включён."
        return {"ok": True, "message": str(message), "trace_id": event.trace_id}

    async def on_command(self, command: str, event: CajeerEvent, context) -> dict[str, object] | None:
        if command != "example":
            return None
        message = context.manifest.settings_schema.get("enabled_message", {}).get("default", "Пример плагина включён.") if context.manifest.settings_schema else "Пример плагина включён."
        return {"ok": True, "message": str(message), "plugin": self.id, "trace_id": event.trace_id}

    async def on_stop(self, context) -> None:
        context.logger.info("пример плагина остановлен")
