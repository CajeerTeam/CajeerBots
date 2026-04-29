from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from core.runtime import Runtime


class ApiServer:
    def __init__(self, runtime: "Runtime") -> None:
        self.runtime = runtime
        self._httpd: ThreadingHTTPServer | None = None

    def _payload(self, path: str) -> tuple[int, dict[str, object]]:
        runtime = self.runtime
        if path in {"/healthz", "/readyz"}:
            return 200, {"ok": True, "status": "работает", "version": runtime.version}
        if path == "/version":
            return 200, {"version": runtime.version, "event_contract": runtime.event_contract_version}
        if path == "/adapters":
            return 200, {"items": [m.to_dict() for m in runtime.registry.adapters()]}
        if path == "/modules":
            return 200, {"items": [m.to_dict() for m in runtime.registry.modules()]}
        if path == "/plugins":
            return 200, {"items": [m.to_dict() for m in runtime.registry.plugins()]}
        if path == "/events":
            return 200, {"items": [event.to_dict() for event in runtime.event_bus.snapshot()]}
        if path == "/commands":
            return 200, {"items": [command.to_dict() for command in runtime.commands.list()]}
        if path == "/config/summary":
            return 200, {"config": runtime.settings.safe_summary()}
        if path == "/adapter-status":
            return 200, {"items": [status.to_dict() for status in runtime.adapter_health_snapshot()]}
        return 404, {"ok": False, "error": "маршрут не найден"}

    def serve_forever(self) -> None:
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                status, payload = server._payload(urlparse(self.path).path)
                body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
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
