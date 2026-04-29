from __future__ import annotations

import asyncio
import hmac
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING
from urllib.parse import urlparse
from uuid import uuid4

from bots.telegram.bot.mapper import update_to_event as telegram_update_to_event
from core.contracts import API_CONTRACT_VERSION
from core.events import CajeerEvent

if TYPE_CHECKING:
    from core.runtime import Runtime


PUBLIC_PATHS = {"/healthz", "/readyz"}
READONLY_PATHS = {
    "/version",
    "/adapters",
    "/modules",
    "/plugins",
    "/components",
    "/events",
    "/routes",
    "/dead-letters",
    "/commands",
    "/config/summary",
    "/adapter-status",
    "/worker-status",
    "/bridge-status",
    "/status/dependencies",
    "/audit",
    "/openapi.json",
}

MAX_BODY_BYTES = 1_048_576


class ApiServer:
    def __init__(self, runtime: "Runtime") -> None:
        self.runtime = runtime
        self._httpd: ThreadingHTTPServer | None = None

    def _token_scope(self, headers: object) -> str | None:
        value = headers.get("Authorization", "")
        settings = self.runtime.settings

        def same(expected: str) -> bool:
            return bool(expected) and hmac.compare_digest(value, f"Bearer {expected}")

        if same(settings.api_token):
            return "admin"
        if same(settings.api_readonly_token):
            return "readonly"
        if same(settings.api_metrics_token):
            return "metrics"
        return None

    def _telegram_webhook_authorized(self, headers: object) -> bool:
        expected = self.runtime.settings.adapters["telegram"].extra.get("webhook_secret", "")
        if not expected:
            return True
        actual = headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        return hmac.compare_digest(str(actual), str(expected))

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
            return 200, {
                "version": runtime.version,
                "api_contract": API_CONTRACT_VERSION,
                "event_contract": runtime.event_contract_version,
                "db_contract": runtime.db_contract_version,
                "workspace_contract": runtime.workspace_contract_version,
                "logs_contract": runtime.logs_contract_version,
            }, "application/json"
        if path == "/adapters":
            return 200, {"items": [m.to_dict() for m in runtime.registry.adapters()]}, "application/json"
        if path == "/modules":
            return 200, {"items": [m.to_dict() for m in runtime.registry.modules()]}, "application/json"
        if path == "/plugins":
            return 200, {"items": [m.to_dict() for m in runtime.registry.plugins()]}, "application/json"
        if path == "/components":
            return 200, {"items": runtime.components.snapshot()}, "application/json"
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
        if path == "/audit":
            return 200, {"items": [item.to_dict() for item in runtime.audit.snapshot()]}, "application/json"
        if path == "/openapi.json":
            return 200, self.openapi(), "application/json"
        return 404, {"ok": False, "error": {"code": "not_found", "message": "маршрут не найден"}}, "application/json"

    def _post_payload(self, path: str, body: dict[str, object], *, actor: str = "api") -> tuple[int, dict[str, object], str]:
        runtime = self.runtime
        if path == "/commands/dispatch":
            command = str(body.get("command", "")).strip().lstrip("/")
            event = CajeerEvent.create(source="system", type="command.received", payload={"command": command, **dict(body.get("payload") or {})})

            async def run_command() -> dict[str, object]:
                await runtime.event_bus.publish(event)
                result = await runtime.router.route(event)
                return result.to_dict()

            result = asyncio.run(run_command())
            runtime.audit.write(actor_type="api", actor_id=actor, action="commands.dispatch", resource=command, trace_id=event.trace_id)
            return 200, {"ok": True, "result": result}, "application/json"
        if path == "/delivery/enqueue":
            task = runtime.delivery.enqueue(
                adapter=str(body.get("adapter", "")),
                target=str(body.get("target", "")),
                text=str(body.get("text", "")),
                max_attempts=int(body.get("max_attempts", 3)),
                trace_id=str(body.get("trace_id") or "") or None,
            )
            runtime.audit.write(actor_type="api", actor_id=actor, action="delivery.enqueue", resource=task.adapter, trace_id=task.trace_id)
            return 202, {"ok": True, "task": task.to_dict()}, "application/json"
        if path == "/dead-letters/retry":
            events = runtime.dead_letters.retry_all()

            async def retry() -> int:
                for event in events:
                    await runtime.event_bus.publish(event)
                return len(events)

            count = asyncio.run(retry())
            runtime.audit.write(actor_type="api", actor_id=actor, action="dead_letters.retry", resource="dead_letters")
            return 202, {"ok": True, "queued": count}, "application/json"
        if path == "/events/publish":
            event = CajeerEvent.create(
                source=str(body.get("source", "system")),  # type: ignore[arg-type]
                type=str(body.get("type", "system.event")),
                payload=dict(body.get("payload") or {}),
            )
            asyncio.run(runtime.event_bus.publish(event))
            runtime.audit.write(actor_type="api", actor_id=actor, action="events.publish", resource=event.type, trace_id=event.trace_id)
            return 202, {"ok": True, "event": event.to_dict()}, "application/json"
        if path == "/runtime/stop":
            runtime.request_stop()
            return 202, {"ok": True, "message": "запрошена остановка runtime"}, "application/json"
        if path == "/webhooks/telegram":
            event = telegram_update_to_event(body)

            async def ingest() -> list[dict[str, object]]:
                return await runtime.ingest_incoming_event(event)

            results = asyncio.run(ingest())
            runtime.audit.write(actor_type="webhook", actor_id="telegram", action="webhook.telegram", resource="telegram", trace_id=event.trace_id)
            return 200, {"ok": True, "event": event.to_dict(), "results": results}, "application/json"
        return 404, {"ok": False, "error": {"code": "not_found", "message": "маршрут не найден"}}, "application/json"

    def openapi(self) -> dict[str, object]:
        return {
            "openapi": "3.1.0",
            "info": {"title": "Cajeer Bots API", "version": self.runtime.version, "x-contract": API_CONTRACT_VERSION},
            "paths": {
                "/healthz": {"get": {"summary": "Проверка процесса"}},
                "/readyz": {"get": {"summary": "Проверка готовности"}},
                "/metrics": {"get": {"summary": "Prometheus metrics"}},
                "/commands/dispatch": {"post": {"summary": "Отправить команду в router"}},
                "/events/publish": {"post": {"summary": "Опубликовать событие"}},
                "/delivery/enqueue": {"post": {"summary": "Поставить исходящее сообщение в очередь"}},
                "/webhooks/telegram": {"post": {"summary": "Принять Telegram webhook update"}},
                "/audit": {"get": {"summary": "Audit trail"}},
            },
        }

    def serve_forever(self) -> None:
        server = self

        class Handler(BaseHTTPRequestHandler):
            def _json_body(self) -> dict[str, object]:
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length < 0 or length > MAX_BODY_BYTES:
                    raise ValueError("request body слишком большой")
                if length <= 0:
                    return {}
                raw = self.rfile.read(length).decode("utf-8")
                data = json.loads(raw or "{}")
                if not isinstance(data, dict):
                    raise ValueError("request body должен быть JSON-объектом")
                return data

            def _write(self, status: int, payload: dict[str, object] | str, content_type: str, request_id: str) -> None:
                if isinstance(payload, str):
                    body = payload.encode("utf-8")
                    header_value = f"{content_type}; charset=utf-8"
                else:
                    payload.setdefault("request_id", request_id)
                    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                    header_value = "application/json; charset=utf-8"
                self.send_response(int(status))
                self.send_header("Content-Type", header_value)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("X-Request-Id", request_id)
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802
                request_id = self.headers.get("X-Request-Id") or str(uuid4())
                path = urlparse(self.path).path
                scope = server._token_scope(self.headers)
                if not server._can_get(path, scope):
                    server.runtime.audit.write(
                        actor_type="api",
                        actor_id=scope or "anonymous",
                        action="http.get",
                        resource=path,
                        result="denied",
                        ip=self.client_address[0] if self.client_address else None,
                        user_agent=self.headers.get("User-Agent"),
                    )
                    self._write(401, {"ok": False, "error": {"code": "unauthorized", "message": "требуется токен"}}, "application/json", request_id)
                    return
                status, payload, content_type = server._payload(path)
                self._write(status, payload, content_type, request_id)

            def do_POST(self) -> None:  # noqa: N802
                request_id = self.headers.get("X-Request-Id") or str(uuid4())
                path = urlparse(self.path).path
                scope = server._token_scope(self.headers)
                if path == "/webhooks/telegram":
                    if not server._telegram_webhook_authorized(self.headers):
                        self._write(401, {"ok": False, "error": {"code": "unauthorized", "message": "invalid telegram webhook secret"}}, "application/json", request_id)
                        return
                    try:
                        body = self._json_body()
                        status, payload, content_type = server._post_payload(path, body, actor="telegram")
                    except Exception as exc:  # noqa: BLE001
                        status, payload, content_type = 400, {"ok": False, "error": {"code": "bad_request", "message": str(exc)}}, "application/json"
                    self._write(status, payload, content_type, request_id)
                    return
                if scope != "admin":
                    server.runtime.audit.write(
                        actor_type="api",
                        actor_id=scope or "anonymous",
                        action="http.post",
                        resource=path,
                        result="denied",
                        ip=self.client_address[0] if self.client_address else None,
                        user_agent=self.headers.get("User-Agent"),
                    )
                    self._write(401, {"ok": False, "error": {"code": "unauthorized", "message": "требуется admin-токен"}}, "application/json", request_id)
                    return
                try:
                    body = self._json_body()
                    status, payload, content_type = server._post_payload(path, body, actor=scope)
                except Exception as exc:  # noqa: BLE001
                    status, payload, content_type = 400, {"ok": False, "error": {"code": "bad_request", "message": str(exc)}}, "application/json"
                self._write(status, payload, content_type, request_id)

            def log_message(self, format: str, *args: object) -> None:  # noqa: A002
                return

        self._httpd = ThreadingHTTPServer((self.runtime.settings.api_bind, self.runtime.settings.api_port), Handler)
        self._httpd.serve_forever()

    def start_in_thread(self) -> None:
        thread = threading.Thread(target=self.serve_forever, name="cajeer-bots-api", daemon=True)
        thread.start()

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
