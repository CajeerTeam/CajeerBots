from __future__ import annotations

from core.sdk import PluginBase


class ExampleSchedulerPlugin(PluginBase):
    id = "example_scheduler"

    def register_scheduled_jobs(self, context) -> list[dict[str, object]]:
        return [{
            "job_type": "event.publish",
            "payload": {
                "source": "example_scheduler",
                "type": "plugin.example_scheduler.tick",
                "payload": {"plugin": self.id}
            }
        }]
