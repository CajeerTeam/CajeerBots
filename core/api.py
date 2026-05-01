from __future__ import annotations

import asyncio
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, Any, Mapping
from urllib.parse import urlparse
from uuid import uuid4

if TYPE_CHECKING:
    from core.runtime import Runtime

MAX_BODY_BYTES = 1024 * 1024
logger = logging.getLogger(__name__)


class ApiServer:
    """Stdlib HTTP wrapper over the same async dispatcher used by ASGI mode.

    This implementation intentionally keeps parsing/auth/content-type handling
    in the thin stdlib adapter and delegates business behavior to
    ``AsyncApiDispatcher`` so ASGI and stdlib modes remain contract-compatible.
    """

    def __init__(self, runtime: "Runtime", loop: asyncio.AbstractEventLoop | None = None) -> None:
        from core.api_dispatcher import AsyncApiDispatcher

        self.runtime = runtime
        self.settings = runtime.settings
        self._dispatcher = AsyncApiDispatcher(runtime)
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._loop = loop or asyncio.new_event_loop()

    def _run_async(self, coro):
        if self._loop.is_running():
            raise RuntimeError("ApiServer stdlib dispatcher requires a non-running event loop")
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return self._loop.run_until_complete(coro)

        # Some test harnesses and notebooks execute sync code while an event loop
        # is already active in the current thread. Run the stdlib dispatcher
        # coroutine in an isolated helper thread so private contract helpers keep
        # working without leaking ASGI assumptions into stdlib mode.
        result: dict[str, Any] = {}

        def runner() -> None:
            loop = asyncio.new_event_loop()
            try:
                result["value"] = loop.run_until_complete(coro)
            except BaseException as exc:  # noqa: BLE001
                result["error"] = exc
            finally:
                loop.close()

        thread = threading.Thread(target=runner, name="cajeer-api-sync-bridge", daemon=True)
        thread.start()
        thread.join()
        if "error" in result:
            raise result["error"]
        return result["value"]

    def _token_scope(self, headers: Mapping[str, str]) -> str | None:
        return self._dispatcher.token_scope(headers)

    def _scope_allowed(self, path: str, method: str, scope: str | None) -> bool:
        return self._dispatcher.scope_allowed(path, method, scope)

    def _can_get(self, path: str, scope: str | None) -> bool:
        return self._scope_allowed(path, "GET", scope)

    def _can_post(self, path: str, scope: str | None) -> bool:
        return self._scope_allowed(path, "POST", scope)

    def _payload(self, path: str, *, headers: Mapping[str, str] | None = None) -> tuple[int, dict[str, object] | list[object] | str, str]:
        return self._run_async(self._dispatcher.get(path, headers=headers, actor="api"))

    def _json_body(self, body: bytes | bytearray | str | dict[str, object] | None) -> dict[str, object]:
        if body is None:
            return {}
        if isinstance(body, dict):
            return body
        if isinstance(body, (bytes, bytearray)):
            text = bytes(body).decode("utf-8").strip()
        else:
            text = str(body).strip()
        if not text:
            return {}
        payload = json.loads(text)
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    def _post_payload(
        self,
        path: str,
        body: bytes | bytearray | str | dict[str, object] | None,
        *,
        actor: str = "api",
        headers: Mapping[str, str] | None = None,
    ) -> tuple[int, dict[str, object] | list[object] | str, str]:
        try:
            payload = self._json_body(body)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            return 400, {"ok": False, "error": {"code": "invalid_json", "message": str(exc)}}, "application/json"
        return self._run_async(self._dispatcher.post(path, payload, actor=actor, headers=headers))

    def _webhook_rate_limited(self, provider: str) -> bool:
        return self._dispatcher.webhook_rate_limited(provider)

    def _telegram_webhook_authorized(self, headers: Mapping[str, str]) -> bool:
        return self._dispatcher.telegram_webhook_authorized(headers)

    def _vkontakte_webhook_authorized(self, body: bytes | bytearray | str | dict[str, object]) -> tuple[bool, str | None]:
        try:
            payload = self._json_body(body)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return False, None
        if payload.get("type") == "confirmation":
            confirmation = self.settings.adapters["vkontakte"].extra.get("confirmation_code", "")
            return True, confirmation or None
        return self._dispatcher.vkontakte_webhook_authorized(payload), None

    def _webhook_security_allowed(self, provider: str, headers: Mapping[str, str], body: bytes) -> bool:
        return self._dispatcher.webhook_replay_allowed(provider, headers, body)

    def serve_forever(self) -> None:
        asyncio.set_event_loop(self._loop)
        server = self

        class Handler(BaseHTTPRequestHandler):
            def _send(self, status: int, payload: dict[str, object] | list[object] | str, content_type: str | None = None) -> None:
                if content_type is None:
                    content_type = "text/plain; charset=utf-8" if isinstance(payload, str) else "application/json; charset=utf-8"
                elif content_type == "application/json":
                    content_type = "application/json; charset=utf-8"
                elif content_type == "text/plain":
                    content_type = "text/plain; charset=utf-8"

                if content_type.startswith("application/json") and not isinstance(payload, str):
                    raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                elif content_type.startswith("application/json"):
                    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                else:
                    raw = str(payload).encode("utf-8")
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
                status, payload, content_type = server._payload(path, headers=self.headers)
                self._send(status, payload, content_type)

            def do_POST(self) -> None:  # noqa: N802
                path = urlparse(self.path).path
                try:
                    length = int(self.headers.get("Content-Length", "0") or 0)
                except ValueError:
                    self._send(400, {"ok": False, "error": "invalid_content_length"})
                    return
                if length > MAX_BODY_BYTES:
                    self._send(413, {"ok": False, "error": "payload_too_large"})
                    return
                body = self.rfile.read(length) if length else b""
                actor = "api"
                if path == "/webhooks/telegram":
                    actor = "telegram-webhook"
                    if (
                        server._webhook_rate_limited("telegram")
                        or not server._telegram_webhook_authorized(self.headers)
                        or not server._webhook_security_allowed("telegram", self.headers, body)
                    ):
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
                        self._send(200, confirmation, "text/plain")
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
                status, payload, content_type = server._post_payload(path, body, actor=actor, headers=self.headers)
                self._send(status, payload, content_type)

            def log_message(self, fmt: str, *args: Any) -> None:
                runtime_logger = getattr(server.runtime, "logger", None)
                (runtime_logger or logger).debug("api: " + fmt, *args)

        self._httpd = ThreadingHTTPServer((self.settings.api_bind, self.settings.api_port), Handler)
        logger.info("API слушает %s:%s (%s)", self.settings.api_bind, self.settings.api_port, self.settings.api_server)
        self._httpd.serve_forever()

    def start_background(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self.serve_forever, name="cajeer-api", daemon=True)
        self._thread.start()

    def start_in_thread(self) -> None:
        self.start_background()

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        if not self._loop.is_closed():
            self._loop.close()
