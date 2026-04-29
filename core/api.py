from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from core.events import CajeerEvent

if TYPE_CHECKING:
    from core.runtime import Runtime


PUBLIC_PATHS = {"/healthz", "/readyz"}
READONLY_PATHS = {
    "/version",
    "/adapters",
    "/modules",
    "/plugins",
    "/events",
    "/routes",
    "/dead-letters",
    "/commands",
    "/config/summary",
    "/adapter-status",
    "/worker-status",
    "/bridge-status",
    "/status/dependencies",
}


class ApiServer:
    def __init__(self, runtime: "Runtime") -> None:
        self.runtime = runtime
        self._httpd: ThreadingHTTPServer | None = None

    def _token_scope(self, headers: object) -> str | None:
        value = headers.get("Authorization", "")
        settings = self.runtime.settings
        if settings.api_token and value == f"Bearer {settings.api_token}":
            return "admin"
        if settings.api_readonly_token and value == f"Bearer {settings.api_readonly_token}":
            return "readonly"
        if settings.api_metrics_token and value == f"Bearer {settings.api_metrics_token}":
            return "metrics"
        return None

    def _can_get(self, path: str, scope: str | None) -> bool:
        if path in PUBLIC_PATHS:
            return True
        if path == "/metrics":
            return self.runtime.settings.metrics_public or scope in {"admin", "metrics"}
        if path in READONLY_PATHS:
            return scope in {"admin", "readonly"}
        return scope == "admin"

    def _payload(self, path: str) -> tuple[int, dict[str, object] | str, str]:
        runtime = self.runtime
        if path == "/healthz":
            return 200, {"ok": True, "status": "процесс работает", "version": runtime.version}, "application/json"
        if path == "/readyz":
            ready = runtime.readiness_snapshot()
            return (200 if ready["ok"] else 503), ready, "application/json"
        if path == "/metrics":
            return 200, runtime.metrics_text(), "text/plain"
        if path == "/version":
            return 200, {"version": runtime.version, "event_contract": runtime.event_contract_version}, "application/json"
        if path == "/adapters":
            return 200, {"items": [m.to_dict() for m in runtime.registry.adapters()]}, "application/json"
        if path == "/modules":
            return 200, {"items": [m.to_dict() for m in runtime.registry.modules()]}, "application/json"
        if path == "/plugins":
            return 200, {"items": [m.to_dict() for m in runtime.registry.plugins()]}, "application/json"
        if path == "/events":
            return 200, {"items": [event.to_dict() for event in runtime.event_bus.snapshot()]}, "application/json"
        if path == "/routes":
            return 200, {"items": [item.to_dict() for item in runtime.router.snapshot()]}, "application/json"
        if path == "/dead-letters":
            return 200, {"items": [item.to_dict() for item in runtime.dead_letters.snapshot()]}, "application/json"
        if path == "/commands":
            return 200, {"items": [command.to_dict() for command in runtime.commands.list()]}, "application/json"
        if path == "/config/summary":
            return 200, {"config": runtime.settings.safe_summary()}, "application/json"
        if path == "/adapter-status":
            return 200, {"items": [status.to_dict() for status in runtime.adapter_health_snapshot()]}, "application/json"
        if path == "/worker-status":
            return 200, {"status": runtime.worker.status.to_dict()}, "application/json"
        if path == "/bridge-status":
            return 200, {"status": runtime.bridge.status.to_dict()}, "application/json"
        if path == "/status/dependencies":
            return 200, {"dependencies": runtime.dependencies_snapshot()}, "application/json"
        return 404, {"ok": False, "error": "маршрут не найден"}, "application/json"

    def _post_payload(self, path: str, body: dict[str, object]) -> tuple[int, dict[str, object], str]:
        runtime = self.runtime
        if path == "/commands/dispatch":
            command = str(body.get("command", "")).strip().lstrip("/")
            event = CajeerEvent.create(source="system", type="command.received", payload={"command": command, **dict(body.get("payload") or {})})
            # Синхронный путь для API-команды: публикуем событие и сразу маршрутизируем.
            import asyncio
            async def run_command() -> dict[str, object]:
                await runtime.event_bus.publish(event)
                result = await runtime.router.route(event)
                return result.to_dict()
            result = asyncio.run(run_command())
            return 200, {"ok": True, "result": result}, "application/json"
        if path == "/delivery/enqueue":
            task = runtime.delivery.enqueue(
                adapter=str(body.get("adapter", "")),
                target=str(body.get("target", "")),
                text=str(body.get("text", "")),
                max_attempts=int(body.get("max_attempts", 3)),
            )
            return 202, {"ok": True, "task": task.to_dict()}, "application/json"
        if path == "/dead-letters/retry":
            events = runtime.dead_letters.retry_all()
            import asyncio
            async def retry() -> int:
                for event in events:
                    await runtime.event_bus.publish(event)
                return len(events)
            count = asyncio.run(retry())
            return 202, {"ok": True, "queued": count}, "application/json"
        if path == "/events/publish":
            event = CajeerEvent.create(
                source=str(body.get("source", "system")),
                type=str(body.get("type", "system.event")),
                payload=dict(body.get("payload") or {}),
            )
            import asyncio
            asyncio.run(runtime.event_bus.publish(event))
            return 202, {"ok": True, "event": event.to_dict()}, "application/json"
        if path == "/runtime/stop":
            runtime.request_stop()
            return 202, {"ok": True, "message": "запрошена остановка runtime"}, "application/json"
        return 404, {"ok": False, "error": "маршрут не найден"}, "application/json"

    def serve_forever(self) -> None:
        server = self

        class Handler(BaseHTTPRequestHandler):
            def _json_body(self) -> dict[str, object]:
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length <= 0:
                    return {}
                raw = self.rfile.read(length).decode("utf-8")
                return json.loads(raw or "{}")

            def _write(self, status: int, payload: dict[str, object] | str, content_type: str) -> None:
                if isinstance(payload, str):
                    body = payload.encode("utf-8")
                    header_value = f"{content_type}; charset=utf-8"
                else:
                    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                    header_value = "application/json; charset=utf-8"
                self.send_response(int(status))
                self.send_header("Content-Type", header_value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802
                path = urlparse(self.path).path
                scope = server._token_scope(self.headers)
                if not server._can_get(path, scope):
                    self._write(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "требуется действующий API-токен"}, "application/json")
                    return
                status, payload, content_type = server._payload(path)
                self._write(int(status), payload, content_type)

            def do_POST(self) -> None:  # noqa: N802
                path = urlparse(self.path).path
                scope = server._token_scope(self.headers)
                if scope != "admin":
                    self._write(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "требуется admin API-токен"}, "application/json")
                    return
                try:
                    status, payload, content_type = server._post_payload(path, self._json_body())
                except Exception as exc:  # pragma: no cover - защитный контур API
                    status, payload, content_type = 500, {"ok": False, "error": str(exc)}, "application/json"
                self._write(status, payload, content_type)

            def log_message(self, format: str, *args: object) -> None:  # noqa: A002
                return

        self._httpd = ThreadingHTTPServer((self.runtime.settings.api_bind, self.runtime.settings.api_port), Handler)
        self._httpd.serve_forever(poll_interval=0.5)

    def start_in_thread(self) -> threading.Thread:
        thread = threading.Thread(target=self.serve_forever, name="cajeer-bots-api", daemon=True)
        thread.start()
        return thread

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
