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
from bots.vkontakte.bot.thin import VkontakteThinWrapper
from core.contracts import API_CONTRACT_VERSION
from core.api_routes import KNOWN_SCOPES, ROUTES, canonical_scope, openapi_document, readonly_paths
from core.events import CajeerEvent

if TYPE_CHECKING:
    from core.runtime import Runtime


PUBLIC_PATHS = {"/healthz", "/readyz"}
READONLY_PATHS = readonly_paths() | {"/openapi.json"}

MAX_BODY_BYTES = 1_048_576


class ApiServer:
    def __init__(self, runtime: "Runtime", loop: asyncio.AbstractEventLoop | None = None) -> None:
        self.runtime = runtime
        self._loop = loop
        self._httpd: ThreadingHTTPServer | None = None
        self._webhook_hits: dict[str, list[float]] = {}
        self._webhook_auth_failures: dict[str, list[float]] = {}

    def _run_async(self, coro):
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if self._loop is not None and self._loop.is_running() and running_loop is not self._loop:
            return asyncio.run_coroutine_threadsafe(coro, self._loop).result()
        if running_loop is not None and running_loop.is_running():
            raise RuntimeError("API async operation cannot be executed synchronously inside the runtime event loop")
        return asyncio.run(coro)

    def _token_scope(self, headers: object) -> str | None:
        value = headers.get("Authorization", "")
        settings = self.runtime.settings

        def same(expected: str) -> bool:
            return bool(expected) and hmac.compare_digest(value, f"Bearer {expected}")

        token_id, scopes, _prefix = self.runtime.token_registry.authenticate(value)
        if token_id:
            normalized = {canonical_scope(scope) for scope in scopes}
            if "system.admin" in normalized or "*" in normalized:
                return "system.admin"
            return ",".join(sorted(normalized))
        if same(settings.api_token):
            return "admin"
        if same(settings.api_readonly_token):
            return "readonly"
        if same(settings.api_metrics_token):
            return "metrics"
        return None

    def _scope_allowed(self, path: str, method: str, scope: str | None) -> bool:
        if scope in {"admin", "system.admin"}:
            return True
        if path in PUBLIC_PATHS and method == "GET":
            return True
        if path == "/metrics":
            return self.runtime.settings.metrics_public or scope in {"admin", "metrics", "system.admin", "system.metrics"}
        if path in READONLY_PATHS and method == "GET":
            return scope in {"admin", "readonly", "system.admin", "system.read", "system.update.read"}
        wanted = next((item.auth_scope for item in ROUTES if item.path == path and item.method == method), "admin")
        if wanted in {"public", "webhook"}:
            return True
        if not scope:
            return False
        granted = {canonical_scope(item) for item in scope.split(",")}
        return canonical_scope(wanted) in granted or "system.admin" in granted or "*" in granted

    def _webhook_rate_limited(self, key: str, *, failed_auth: bool = False) -> bool:
        import time
        now = time.time()
        bucket = self._webhook_auth_failures if failed_auth else self._webhook_hits
        limit = self.runtime.settings.webhook_auth_failure_limit if failed_auth else self.runtime.settings.webhook_rate_limit_per_minute
        items = [ts for ts in bucket.get(key, []) if now - ts < 60]
        items.append(now)
        bucket[key] = items
        return len(items) > limit

    def _telegram_webhook_authorized(self, headers: object) -> bool:
        expected = self.runtime.settings.adapters["telegram"].extra.get("webhook_secret", "")
        if not expected:
            return True
        actual = headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        return hmac.compare_digest(str(actual), str(expected))

    def _vkontakte_webhook_authorized(self, body: dict[str, object]) -> bool:
        expected = self.runtime.settings.adapters["vkontakte"].extra.get("callback_secret", "")
        if not expected:
            return True
        return hmac.compare_digest(str(body.get("secret") or ""), str(expected))

    def _can_get(self, path: str, scope: str | None) -> bool:
        return self._scope_allowed(path, "GET", scope)

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
            return 200, {"dependencies": runtime.dependencies_snapshot(), "checks": runtime.dependency_health_snapshot()}, "application/json"
        if path == "/audit":
            return 200, {"items": [item.to_dict() for item in runtime.audit.snapshot()]}, "application/json"
        if path == "/openapi.json":
            return 200, self.openapi(), "application/json"
        if path == "/updates/status":
            return 200, {"status": runtime.updater.status().to_dict()}, "application/json"
        if path == "/updates/plan":
            return 200, {"plan": runtime.updater.plan("latest", refresh_latest=False, record=False)}, "application/json"
        if path == "/updates/history":
            return 200, {"items": [item.to_dict() for item in runtime.updater.history()]}, "application/json"
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

            result = self._run_async(run_command())
            runtime.audit.write(actor_type="api", actor_id=actor, action="commands.dispatch", resource=command, trace_id=event.trace_id)
            return 200, {"ok": True, "result": result}, "application/json"
        if path == "/delivery/enqueue":
            async def enqueue_task():
                return await runtime.delivery.enqueue_async(
                    adapter=str(body.get("adapter", "")),
                    target=str(body.get("target", "")),
                    text=str(body.get("text", "")),
                    max_attempts=int(body.get("max_attempts", 3)),
                    trace_id=str(body.get("trace_id") or "") or None,
                )
            task = self._run_async(enqueue_task())
            runtime.audit.write(actor_type="api", actor_id=actor, action="delivery.enqueue", resource=task.adapter, trace_id=task.trace_id)
            return 202, {"ok": True, "task": task.to_dict()}, "application/json"
        if path == "/dead-letters/retry":
            events = runtime.dead_letters.retry_all()

            async def retry() -> int:
                for event in events:
                    await runtime.event_bus.publish(event)
                return len(events)

            count = self._run_async(retry())
            runtime.audit.write(actor_type="api", actor_id=actor, action="dead_letters.retry", resource="dead_letters")
            return 202, {"ok": True, "queued": count}, "application/json"
        if path == "/events/publish":
            event = CajeerEvent.create(
                source=str(body.get("source", "system")),
                type=str(body.get("type", "system.event")),
                payload=dict(body.get("payload") or {}),
            )
            self._run_async(runtime.event_bus.publish(event))
            runtime.audit.write(actor_type="api", actor_id=actor, action="events.publish", resource=event.type, trace_id=event.trace_id)
            return 202, {"ok": True, "event": event.to_dict()}, "application/json"
        if path == "/runtime/stop":
            runtime.request_stop()
            return 202, {"ok": True, "message": "запрошена остановка runtime"}, "application/json"
        if path == "/updates/check":
            return 200, {"ok": True, "update": runtime.updater.check()}, "application/json"
        if path == "/updates/plan":
            return 200, {"ok": True, "plan": runtime.updater.plan(str(body.get("version") or "latest"))}, "application/json"
        if path == "/updates/apply":
            version = str(body.get("version") or "latest").strip()
            staged_path = str(body.get("staged_path") or "").strip()
            auto_stage = bool(body.get("auto_stage", version == "latest" and not staged_path))
            dry_run = bool(body.get("dry_run", False))
            if version == "latest" and auto_stage:
                return 202, runtime.updater.apply_latest(dry_run=dry_run), "application/json"
            if not version or not staged_path:
                return 400, {"ok": False, "error": {"code": "bad_request", "message": "version/staged_path обязательны, кроме version=latest+auto_stage"}}, "application/json"
            return 202, runtime.updater.apply_staged(version, staged_path, dry_run=dry_run), "application/json"
        if path == "/updates/rollback":
            return 202, runtime.updater.rollback(), "application/json"
        if path == "/webhooks/telegram":
            event = telegram_update_to_event(body)

            async def ingest() -> list[dict[str, object]]:
                return await runtime.ingest_incoming_event(event)

            results = self._run_async(ingest())
            runtime.audit.write(actor_type="webhook", actor_id="telegram", action="webhook.telegram", resource="telegram", trace_id=event.trace_id)
            return 200, {"ok": True, "event": event.to_dict(), "results": results}, "application/json"
        if path == "/webhooks/vkontakte":
            if body.get("type") == "confirmation":
                code = runtime.settings.adapters["vkontakte"].extra.get("confirmation_code", "")
                return 200, {"ok": True, "response": code}, "application/json"
            if not self._vkontakte_webhook_authorized(body):
                runtime.audit.write(actor_type="webhook", actor_id="vkontakte", action="webhook.vkontakte.denied", resource="vkontakte", result="denied")
                return 401, {"ok": False, "error": {"code": "unauthorized", "message": "invalid vkontakte webhook secret"}}, "application/json"
            async def ingest_vk() -> list[dict[str, object]]:
                wrapper = VkontakteThinWrapper(runtime.settings.adapters["vkontakte"].token)
                event = await wrapper.callback_event(body)
                return await runtime.ingest_incoming_event(event)
            results = self._run_async(ingest_vk())
            runtime.audit.write(actor_type="webhook", actor_id="vkontakte", action="webhook.vkontakte", resource="vkontakte")
            return 200, {"ok": True, "results": results, "response": "ok"}, "application/json"
        return 404, {"ok": False, "error": {"code": "not_found", "message": "маршрут не найден"}}, "application/json"

    def openapi(self) -> dict[str, object]:
        return openapi_document(self.runtime.version, API_CONTRACT_VERSION)

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
                    server.runtime.audit.write(actor_type="api", actor_id=scope or "anonymous", action="http.get", resource=path, result="denied", ip=self.client_address[0] if self.client_address else None, user_agent=self.headers.get("User-Agent"))
                    self._write(401, {"ok": False, "error": {"code": "unauthorized", "message": "требуется токен"}}, "application/json", request_id)
                    return
                status, payload, content_type = server._payload(path)
                self._write(status, payload, content_type, request_id)

            def do_POST(self) -> None:  # noqa: N802
                request_id = self.headers.get("X-Request-Id") or str(uuid4())
                path = urlparse(self.path).path
                scope = server._token_scope(self.headers)
                if path in {"/webhooks/telegram", "/webhooks/vkontakte"}:
                    ip = self.client_address[0] if self.client_address else "unknown"
                    if server._webhook_rate_limited(f"{path}:{ip}"):
                        server.runtime.audit.write(actor_type="webhook", actor_id=path.rsplit("/", 1)[-1], action="webhook.rate_limited", resource=path, result="denied", ip=ip, user_agent=self.headers.get("User-Agent"))
                        self._write(429, {"ok": False, "error": {"code": "rate_limited", "message": "webhook rate limit exceeded"}}, "application/json", request_id)
                        return
                    try:
                        body = self._json_body()
                        if path == "/webhooks/telegram" and not server._telegram_webhook_authorized(self.headers):
                            server._webhook_rate_limited(f"auth:{path}:{ip}", failed_auth=True)
                            server.runtime.audit.write(actor_type="webhook", actor_id="telegram", action="webhook.telegram.denied", resource=path, result="denied", ip=ip, user_agent=self.headers.get("User-Agent"))
                            self._write(401, {"ok": False, "error": {"code": "unauthorized", "message": "invalid telegram webhook secret"}}, "application/json", request_id)
                            return
                        status, payload, content_type = server._post_payload(path, body, actor=path.rsplit("/", 1)[-1])
                    except Exception as exc:  # noqa: BLE001
                        status, payload, content_type = 400, {"ok": False, "error": {"code": "bad_request", "message": str(exc)}}, "application/json"
                    self._write(status, payload, content_type, request_id)
                    return
                if not server._scope_allowed(path, "POST", scope):
                    server.runtime.audit.write(actor_type="api", actor_id=scope or "anonymous", action="http.post", resource=path, result="denied", ip=self.client_address[0] if self.client_address else None, user_agent=self.headers.get("User-Agent"))
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
