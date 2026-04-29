from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from core.runtime import Runtime


PUBLIC_PATHS = {"/healthz", "/readyz", "/metrics"}


class ApiServer:
    def __init__(self, runtime: "Runtime") -> None:
        self.runtime = runtime
        self._httpd: ThreadingHTTPServer | None = None

    def _authorized(self, headers: object) -> bool:
        token = self.runtime.settings.api_token
        if not token:
            return False
        value = headers.get("Authorization", "")
        return value == f"Bearer {token}"

    def _payload(self, path: str, *, authorized: bool = True) -> tuple[int, dict[str, object] | str, str]:
        runtime = self.runtime
        if path == "/healthz":
            return 200, {"ok": True, "status": "процесс работает", "version": runtime.version}, "application/json"
        if path == "/readyz":
            ready = runtime.readiness_snapshot()
            return (200 if ready["ok"] else 503), ready, "application/json"
        if path == "/metrics":
            return 200, runtime.metrics_text(), "text/plain"
        if not authorized:
            return HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "требуется Authorization: Bearer <API_TOKEN>"}, "application/json"
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
        return 404, {"ok": False, "error": "маршрут не найден"}, "application/json"

    def serve_forever(self) -> None:
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                path = urlparse(self.path).path
                authorized = path in PUBLIC_PATHS or server._authorized(self.headers)
                status, payload, content_type = server._payload(path, authorized=authorized)
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
