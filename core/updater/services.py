from __future__ import annotations

import os
import subprocess
import time
import urllib.request
from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceResult:
    ok: bool
    action: str
    message: str = ""

    def to_dict(self) -> dict[str, object]:
        return {"ok": self.ok, "action": self.action, "message": self.message}


def _http_health_gate() -> list[ServiceResult]:
    url = os.getenv("CAJEER_UPDATE_HEALTH_URL", "").strip()
    if not url:
        return []
    timeout_seconds = max(1, int(os.getenv("CAJEER_UPDATE_HEALTH_TIMEOUT_SECONDS", "30")))
    deadline = time.time() + timeout_seconds
    last_error = ""
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:  # noqa: S310 - operator-configured local health URL
                body = response.read(512).decode("utf-8", errors="replace")
                if 200 <= response.status < 300:
                    return [ServiceResult(True, "http-health", f"{response.status} {body[:120]}")]
                last_error = f"{response.status} {body[:120]}"
        except Exception as exc:  # pragma: no cover - environment-specific
            last_error = str(exc)
        time.sleep(1)
    return [ServiceResult(False, "http-health", last_error or "health timeout")]


class BaseServiceManager:
    def stop(self) -> list[ServiceResult]:
        return [ServiceResult(True, "stop", "noop")]

    def start(self) -> list[ServiceResult]:
        return [ServiceResult(True, "start", "noop")]

    def reload(self) -> list[ServiceResult]:
        return [ServiceResult(True, "reload", "noop")]

    def healthcheck(self) -> list[ServiceResult]:
        return [ServiceResult(True, "healthcheck", "noop"), *_http_health_gate()]


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
        return [self._run(f"is-active:{svc}", ["systemctl", "is-active", "--quiet", svc]) for svc in self.services] + _http_health_gate()


def build_service_manager() -> BaseServiceManager:
    manager = os.getenv("CAJEER_UPDATE_SERVICE_MANAGER", "noop").strip().lower()
    services = [item.strip() for item in os.getenv("CAJEER_UPDATE_SERVICES", "").split(",") if item.strip()]
    if manager == "systemd" and services:
        return SystemdServiceManager(services)
    return BaseServiceManager()
