from __future__ import annotations

import hmac
import time
from typing import Any, Mapping

from core.webhook_registry import telegram_update_to_event, vkontakte_callback_event
from core.api_routes import ROUTES, canonical_scope, openapi_document, readonly_paths
from core.contracts import API_CONTRACT_VERSION
from core.events import CajeerEvent
from core.sdk.plugins import PluginRequest
from core.webhook_security import RedisWebhookReplayGuard, WebhookReplayGuard, replay_key, verify_optional_hmac

PUBLIC_PATHS = {"/healthz", "/livez", "/readyz"}
READONLY_PATHS = readonly_paths() | {"/openapi.json"}


class AsyncApiDispatcher:
    """Async-native HTTP dispatcher for ASGI and future API frontends."""

    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self._webhook_hits: dict[str, list[float]] = {}
        self._webhook_auth_failures: dict[str, list[float]] = {}
        if runtime.settings.webhook_replay_cache == "redis":
            self._replay_guard = RedisWebhookReplayGuard(runtime.settings.redis_url or "", runtime.settings.webhook_replay_ttl_seconds)
        else:
            self._replay_guard = WebhookReplayGuard(runtime.settings.webhook_replay_ttl_seconds)

    def _plugin_route(self, path: str, method: str) -> Any | None:
        method = method.upper()
        for route in getattr(self.runtime, "plugin_routes", []) or []:
            if getattr(route, "method", "").upper() == method and getattr(route, "path", "") == path:
                return route
        return None

    def token_scope(self, headers: Mapping[str, str]) -> str | None:
        value = headers.get("authorization", headers.get("Authorization", ""))
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

    def scope_allowed(self, path: str, method: str, scope: str | None) -> bool:
        method = method.upper()
        if scope in {"admin", "system.admin"}:
            return True
        if path in PUBLIC_PATHS and method == "GET":
            return True
        if path == "/metrics":
            return self.runtime.settings.metrics_public or scope in {"admin", "metrics", "system.admin", "system.metrics"}
        plugin_route = self._plugin_route(path, method)
        if plugin_route is not None:
            wanted = str(getattr(plugin_route, "auth_scope", "system.admin"))
        else:
            if path in READONLY_PATHS and method == "GET":
                return scope in {"admin", "readonly", "system.admin", "system.read", "system.update.read"}
            wanted = next((item.auth_scope for item in ROUTES if item.path == path and item.method == method), "admin")
        if wanted in {"public", "webhook"}:
            return True
        if not scope:
            return False
        granted = {canonical_scope(item) for item in scope.split(",")}
        return canonical_scope(wanted) in granted or "system.admin" in granted or "*" in granted

    def webhook_rate_limited(self, key: str, *, failed_auth: bool = False) -> bool:
        now = time.time()
        bucket = self._webhook_auth_failures if failed_auth else self._webhook_hits
        limit = self.runtime.settings.webhook_auth_failure_limit if failed_auth else self.runtime.settings.webhook_rate_limit_per_minute
        items = [ts for ts in bucket.get(key, []) if now - ts < 60]
        items.append(now)
        bucket[key] = items
        return len(items) > limit

    def telegram_webhook_authorized(self, headers: Mapping[str, str]) -> bool:
        expected = self.runtime.settings.adapters["telegram"].extra.get("webhook_secret", "")
        if not expected:
            return True
        actual = headers.get("x-telegram-bot-api-secret-token", headers.get("X-Telegram-Bot-Api-Secret-Token", ""))
        return hmac.compare_digest(str(actual), str(expected))

    def vkontakte_webhook_authorized(self, body: dict[str, object]) -> bool:
        expected = self.runtime.settings.adapters["vkontakte"].extra.get("callback_secret", "")
        if not expected:
            return True
        return hmac.compare_digest(str(body.get("secret") or ""), str(expected))

    def webhook_replay_allowed(self, provider: str, headers: Mapping[str, str], raw_body: bytes) -> bool:
        settings = self.runtime.settings
        if not verify_optional_hmac(
            settings.event_signing_secret,
            headers,
            raw_body,
            required=settings.webhook_hmac_required,
            timestamp_required=settings.webhook_timestamp_required,
            timestamp_ttl_seconds=settings.webhook_replay_ttl_seconds,
        ):
            return False
        if not settings.webhook_replay_protection:
            return True
        return self._replay_guard.check_and_mark(replay_key(provider, headers, raw_body))

    async def _dispatch_plugin_route(self, route: Any, method: str, path: str, body: dict[str, object] | None = None, *, actor: str = "api", headers: Mapping[str, str] | None = None) -> tuple[int, dict[str, object] | str, str]:
        request = PluginRequest(method=method.upper(), path=path, body=body or {}, actor=actor, headers=headers or {})
        try:
            payload = await route.call(request)
            return 200, payload, "application/json"
        except PermissionError as exc:
            self.runtime.audit.write(actor_type="plugin", actor_id=getattr(route, "plugin_id", "unknown"), action="plugin.route.denied", resource=path, result="denied", message=str(exc))
            return 403, {"ok": False, "error": {"code": "forbidden", "message": str(exc)}}, "application/json"
        except Exception as exc:  # noqa: BLE001
            self.runtime.audit.write(actor_type="plugin", actor_id=getattr(route, "plugin_id", "unknown"), action="plugin.route.failed", resource=path, result="error", message=str(exc))
            return 500, {"ok": False, "error": {"code": "plugin_route_failed", "message": str(exc)}}, "application/json"

    async def get(self, path: str, *, headers: Mapping[str, str] | None = None, actor: str = "api") -> tuple[int, dict[str, object] | str, str]:
        runtime = self.runtime
        plugin_route = self._plugin_route(path, "GET")
        if plugin_route is not None:
            return await self._dispatch_plugin_route(plugin_route, "GET", path, actor=actor, headers=headers)
        if path == "/livez":
            return 200, {"ok": True, "status": "процесс жив", "version": runtime.version}, "application/json"
        if path == "/healthz":
            return 200, {"ok": True, "status": "процесс работает", "version": runtime.version}, "application/json"
        if path == "/readyz":
            ready = await runtime.readiness_snapshot_async()
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
            return 200, {"items": [item.to_dict() for item in runtime.router.snapshot()], "plugin_routes": [item.to_dict() for item in getattr(runtime, "plugin_routes", [])]}, "application/json"
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
            return 200, {"dependencies": runtime.dependencies_snapshot(), "checks": await runtime.dependency_health_snapshot_async()}, "application/json"
        if path == "/audit":
            return 200, {"items": [item.to_dict() for item in runtime.audit.snapshot()]}, "application/json"
        if path == "/openapi.json":
            return 200, openapi_document(runtime.version, API_CONTRACT_VERSION, getattr(runtime, "plugin_routes", [])), "application/json"
        if path == "/updates/status":
            return 200, {"status": runtime.updater.status().to_dict()}, "application/json"
        if path == "/updates/plan":
            return 200, {"plan": runtime.updater.plan("latest", refresh_latest=False, record=False)}, "application/json"
        if path == "/updates/history":
            return 200, {"items": [item.to_dict() for item in runtime.updater.history()]}, "application/json"
        return 404, {"ok": False, "error": {"code": "not_found", "message": "маршрут не найден"}}, "application/json"

    async def post(self, path: str, body: dict[str, object], *, actor: str = "api", headers: Mapping[str, str] | None = None) -> tuple[int, dict[str, object] | str, str]:
        runtime = self.runtime
        plugin_route = self._plugin_route(path, "POST")
        if plugin_route is not None:
            return await self._dispatch_plugin_route(plugin_route, "POST", path, body, actor=actor, headers=headers)
        if path == "/commands/dispatch":
            command = str(body.get("command", "")).strip().lstrip("/")
            event = CajeerEvent.create(source="system", type="command.received", payload={"command": command, **dict(body.get("payload") or {})})
            await runtime.event_bus.publish(event)
            result = await runtime.router.route(event)
            runtime.audit.write(actor_type="api", actor_id=actor, action="commands.dispatch", resource=command, trace_id=event.trace_id)
            return 200, {"ok": True, "result": result.to_dict()}, "application/json"
        if path == "/delivery/enqueue":
            task = await runtime.delivery.enqueue_async(
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
            for event in events:
                await runtime.event_bus.publish(event)
            runtime.audit.write(actor_type="api", actor_id=actor, action="dead_letters.retry", resource="dead_letters")
            return 202, {"ok": True, "queued": len(events)}, "application/json"
        if path == "/events/publish":
            event = CajeerEvent.create(source=str(body.get("source", "system")), type=str(body.get("type", "system.event")), payload=dict(body.get("payload") or {}))
            await runtime.event_bus.publish(event)
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
            results = await runtime.ingest_incoming_event(event)
            runtime.audit.write(actor_type="webhook", actor_id="telegram", action="webhook.telegram", resource="telegram", trace_id=event.trace_id)
            return 200, {"ok": True, "event": event.to_dict(), "results": results}, "application/json"
        if path == "/webhooks/vkontakte":
            if body.get("type") == "confirmation":
                code = runtime.settings.adapters["vkontakte"].extra.get("confirmation_code", "")
                return 200, {"ok": True, "response": code}, "application/json"
            if not self.vkontakte_webhook_authorized(body):
                runtime.audit.write(actor_type="webhook", actor_id="vkontakte", action="webhook.vkontakte.denied", resource="vkontakte", result="denied")
                return 401, {"ok": False, "error": {"code": "unauthorized", "message": "invalid vkontakte webhook secret"}}, "application/json"
            event = await vkontakte_callback_event(runtime.settings.adapters["vkontakte"].token, body)
            results = await runtime.ingest_incoming_event(event)
            runtime.audit.write(actor_type="webhook", actor_id="vkontakte", action="webhook.vkontakte", resource="vkontakte", trace_id=event.trace_id)
            return 200, {"ok": True, "results": results, "response": "ok"}, "application/json"
        return 404, {"ok": False, "error": {"code": "not_found", "message": "маршрут не найден"}}, "application/json"
