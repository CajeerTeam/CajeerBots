from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceResult:
    ok: bool
    action: str
    message: str = ""

    def to_dict(self) -> dict[str, object]:
        return {"ok": self.ok, "action": self.action, "message": self.message}


class BaseServiceManager:
    def stop(self) -> list[ServiceResult]:
        return [ServiceResult(True, "stop", "noop")]

    def start(self) -> list[ServiceResult]:
        return [ServiceResult(True, "start", "noop")]

    def reload(self) -> list[ServiceResult]:
        return [ServiceResult(True, "reload", "noop")]

    def healthcheck(self) -> list[ServiceResult]:
        return [ServiceResult(True, "healthcheck", "noop")]


class SystemdServiceManager(BaseServiceManager):
    def __init__(self, services: list[str]) -> None:
        self.services = services

    def _run(self, action: str, args: list[str]) -> ServiceResult:
        try:
            completed = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60, check=False)
            return ServiceResult(completed.returncode == 0, action, completed.stdout.strip())
        except Exception as exc:
            return ServiceResult(False, action, str(exc))

    def stop(self) -> list[ServiceResult]:
        return [self._run(f"stop:{svc}", ["systemctl", "stop", svc]) for svc in self.services]

    def start(self) -> list[ServiceResult]:
        return [self._run(f"start:{svc}", ["systemctl", "start", svc]) for svc in self.services]

    def reload(self) -> list[ServiceResult]:
        return [self._run("daemon-reload", ["systemctl", "daemon-reload"])]

    def healthcheck(self) -> list[ServiceResult]:
        return [self._run(f"is-active:{svc}", ["systemctl", "is-active", "--quiet", svc]) for svc in self.services]


def build_service_manager() -> BaseServiceManager:
    manager = os.getenv("CAJEER_UPDATE_SERVICE_MANAGER", "noop").strip().lower()
    services = [item.strip() for item in os.getenv("CAJEER_UPDATE_SERVICES", "").split(",") if item.strip()]
    if manager == "systemd" and services:
        return SystemdServiceManager(services)
    return BaseServiceManager()
