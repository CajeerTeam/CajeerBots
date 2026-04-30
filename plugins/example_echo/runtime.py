from __future__ import annotations

from core.sdk import CajeerEvent, PluginBase


class ExampleEchoPlugin(PluginBase):
    id = "example_echo"

    async def on_command(self, command: str, event: CajeerEvent, context) -> dict[str, object] | None:
        if command != "echo":
            return None
        text = str(event.payload.get("args") or event.payload.get("text") or "").strip()
        return {"ok": True, "echo": text, "plugin": self.id, "trace_id": event.trace_id}
