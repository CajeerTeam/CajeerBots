from __future__ import annotations

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING
from urllib.parse import urlparse
from uuid import uuid4

if TYPE_CHECKING:
    from core.runtime import Runtime

MAX_BODY_BYTES = 1024 * 1024


class ApiServer:
    """Stdlib HTTP wrapper over the same async dispatcher used by ASGI mode."""

    def __init__(self, runtime: "Runtime") -> None:
        from core.api_dispatcher import AsyncApiDispatcher

        self.runtime = runtime
        self.settings = runtime.settings
        self._dispatcher = AsyncApiDispatcher(runtime)
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._loop = asyncio.new_event_loop()

    def _run_async(self, coro):
        return self._loop.run_until_complete(coro)

    def _token_scope(self, headers) -> str:
        return self._dispatcher.token_scope(headers)

    def _scope_allowed(self, path: str, method: str, scope: str) -> bool:
        return self._dispatcher.scope_allowed(path, method, scope)

    def _payload(self, path: str) -> tuple[int, dict[str, object] | list[object]]:
        return self._run_async(self._dispatcher.get(path, actor="api"))

    def _post_payload(self, path: str, body: bytes, *, actor: str = "api") -> tuple[int, dict[str, object] | list[object] | str]:
        return self._run_async(self._dispatcher.post(path, body, actor=actor))

    def _webhook_rate_limited(self, provider: str) -> bool:
        return self._dispatcher.webhook_rate_limited(provider)

    def _telegram_webhook_authorized(self, headers) -> bool:
        return self._dispatcher.telegram_webhook_authorized(headers)

    def _vkontakte_webhook_authorized(self, body: bytes) -> tuple[bool, str | None]:
        return self._dispatcher.vkontakte_webhook_authorized(body)

    def _webhook_security_allowed(self, provider: str, headers, body: bytes) -> bool:
        return self._dispatcher.webhook_replay_allowed(provider, headers, body)

    def serve_forever(self) -> None:
        server = self

        class Handler(BaseHTTPRequestHandler):
            def _send(self, status: int, payload: dict[str, object] | list[object] | str) -> None:
                if isinstance(payload, str):
                    raw = payload.encode("utf-8")
                    content_type = "text/plain; charset=utf-8"
                else:
                    raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                    content_type = "application/json; charset=utf-8"
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self) -> None:  # noqa: N802
                path = urlparse(self.path).path
                scope = server._token_scope(self.headers)
                if not server._scope_allowed(path, "GET", scope):
                    self._send(403, {"ok": False, "error": "forbidden", "trace_id": uuid4().hex})
                    return
                status, payload = server._payload(path)
                self._send(status, payload)

            def do_POST(self) -> None:  # noqa: N802
                path = urlparse(self.path).path
                length = int(self.headers.get("Content-Length", "0") or 0)
                if length > MAX_BODY_BYTES:
                    self._send(413, {"ok": False, "error": "payload_too_large"})
                    return
                body = self.rfile.read(length) if length else b""
                actor = "api"
                if path == "/webhooks/telegram":
                    actor = "telegram-webhook"
                    if server._webhook_rate_limited("telegram") or not server._telegram_webhook_authorized(self.headers) or not server._webhook_security_allowed("telegram", self.headers, body):
                        server.runtime.audit.write(actor_type="api", actor_id="telegram", action="webhook.telegram.rejected", result="denied", trace_id=uuid4().hex)
                        self._send(403, {"ok": False, "error": "webhook_forbidden"})
                        return
                elif path == "/webhooks/vkontakte":
                    actor = "vkontakte-webhook"
                    if server._webhook_rate_limited("vkontakte") or not server._webhook_security_allowed("vkontakte", self.headers, body):
                        server.runtime.audit.write(actor_type="api", actor_id="vkontakte", action="webhook.vkontakte.rejected", result="denied", trace_id=uuid4().hex)
                        self._send(403, {"ok": False, "error": "webhook_forbidden"})
                        return
                    ok, confirmation = server._vkontakte_webhook_authorized(body)
                    if confirmation:
                        self._send(200, confirmation)
                        return
                    if not ok:
                        server.runtime.audit.write(actor_type="api", actor_id="vkontakte", action="webhook.vkontakte.rejected", result="denied", trace_id=uuid4().hex)
                        self._send(403, {"ok": False, "error": "webhook_forbidden"})
                        return
                else:
                    scope = server._token_scope(self.headers)
                    if not server._scope_allowed(path, "POST", scope):
                        self._send(403, {"ok": False, "error": "forbidden", "trace_id": uuid4().hex})
                        return
                status, payload = server._post_payload(path, body, actor=actor)
                self._send(status, payload)

            def log_message(self, fmt: str, *args) -> None:
                server.runtime.logger.debug("api: " + fmt, *args)

        self._httpd = ThreadingHTTPServer((self.settings.api_bind, self.settings.api_port), Handler)
        self.runtime.logger.info("API слушает %s:%s (%s)", self.settings.api_bind, self.settings.api_port, self.settings.api_server)
        self._httpd.serve_forever()

    def start_background(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self.serve_forever, name="cajeer-api", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
