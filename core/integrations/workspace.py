from __future__ import annotations

import logging
from dataclasses import dataclass

from core.config import WorkspaceSettings
from core.events import CajeerEvent
from core.integrations.http import post_json

logger = logging.getLogger(__name__)


@dataclass
class WorkspaceClient:
    settings: WorkspaceSettings
    instance_id: str
    version: str

    async def register_service(self, snapshot: dict[str, object]) -> None:
        if not self.settings.enabled:
            return
        payload = {
            "project_id": self.settings.project_id,
            "team_id": self.settings.team_id,
            "service_id": self.settings.service_id,
            "instance_id": self.instance_id,
            "version": self.version,
            "snapshot": snapshot,
        }
        try:
            await post_json(
                f"{self.settings.url}/services/register",
                payload,
                {"Authorization": f"Bearer {self.settings.token}"},
                timeout=self.settings.timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("не удалось зарегистрировать сервис в Cajeer Workspace: %s", exc)

    async def report_event(self, event: CajeerEvent) -> None:
        if not self.settings.enabled:
            return
        payload = {
            "project_id": self.settings.project_id,
            "team_id": self.settings.team_id,
            "service_id": self.settings.service_id,
            "instance_id": self.instance_id,
            "version": self.version,
            "event": event.to_dict(),
        }
        try:
            await post_json(
                f"{self.settings.url}/services/events",
                payload,
                {"Authorization": f"Bearer {self.settings.token}"},
                timeout=self.settings.timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001 - интеграция не должна валить runtime
            logger.warning("не удалось отправить событие в Cajeer Workspace: %s", exc)

    async def heartbeat(self, snapshot: dict[str, object]) -> None:
        if not self.settings.enabled:
            return
        payload = {
            "project_id": self.settings.project_id,
            "team_id": self.settings.team_id,
            "service_id": self.settings.service_id,
            "instance_id": self.instance_id,
            "version": self.version,
            "snapshot": snapshot,
        }
        try:
            await post_json(
                f"{self.settings.url}/services/heartbeat",
                payload,
                {"Authorization": f"Bearer {self.settings.token}"},
                timeout=self.settings.timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("не удалось отправить heartbeat в Cajeer Workspace: %s", exc)
