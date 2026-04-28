from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from aiohttp import web

from .event_contracts import build_signed_response, validate_admin_envelope, validate_transport_event
from .services.bridge_client import verify_signed_request

if TYPE_CHECKING:
    from .bot import NMDiscordBot

LOGGER = logging.getLogger("nmdiscordbot.http")


@dataclass(slots=True)
class BridgeIngressServer:
    bot: "NMDiscordBot"
    host: str
    port: int
    enabled: bool
    bearer_token: str
    hmac_secret: str
    strict_auth: bool
    runner: web.AppRunner | None = None
    site: web.TCPSite | None = None

    async def start(self) -> None:
        if not self.enabled or self.runner is not None:
            return
        app = web.Application()
        app["bot"] = self.bot
        app.add_routes(
            [
                web.get("/internal/health/liveness", self.handle_liveness),
                web.get("/internal/health/readiness", self.handle_readiness),
                web.get(self.bot.settings.metrics_path, self.handle_metrics),
                web.post("/internal/bridge/event", self.handle_transport_event),
                web.post("/internal/bridge/admin", self.handle_admin_event),
                web.post("/internal/bridge/approval", self.handle_admin_event),
            ]
        )
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, host=self.host, port=self.port)
        await self.site.start()
        LOGGER.info("HTTP ingress started on %s:%s", self.host, self.port)

    async def stop(self) -> None:
        if self.runner is None:
            return
        await self.runner.cleanup()
        self.runner = None
        self.site = None
        LOGGER.info("HTTP ingress stopped")

    async def handle_liveness(self, request: web.Request) -> web.Response:
        return web.json_response({"ok": True, "kind": "liveness", "service": "NMDiscordBot", "version": self.bot.version})

    def _request_ip(self, request: web.Request) -> str:
        forwarded = request.headers.get('X-Forwarded-For', '').split(',')[0].strip()
        if forwarded:
            return forwarded
        peer = request.transport.get_extra_info('peername') if request.transport else None
        if isinstance(peer, tuple) and peer:
            return str(peer[0])
        return ''

    async def _metrics_auth(self, request: web.Request) -> tuple[bool, str]:
        if not getattr(self.bot.settings, 'metrics_enabled', True):
            return False, 'metrics_disabled'
        if not getattr(self.bot.settings, 'metrics_require_auth', True):
            return True, 'ok'
        request_ip = self._request_ip(request)
        allowed_ips = set(getattr(self.bot.settings, 'metrics_allowed_ips', tuple()) or tuple())
        if request_ip and request_ip in allowed_ips:
            return True, 'ip_allowlist'
        token = str(getattr(self.bot.settings, 'metrics_bearer_token', '') or '').strip()
        auth = request.headers.get('Authorization', '').strip()
        if token and auth == f'Bearer {token}':
            return True, 'metrics_bearer'
        raw = await request.read()
        ok, reason = await self._auth(request, raw)
        return ok, reason

    async def handle_readiness(self, request: web.Request) -> web.Response:
        try:
            await self.bot.storage.healthcheck(strict_redis=self.bot.settings.healthcheck_strict_redis)
            platform = await self.bot.community_store.health()
            return web.json_response({"ok": True, "kind": "readiness", "service": "NMDiscordBot", "version": self.bot.version, "platform": platform})
        except Exception as exc:
            return web.json_response({"ok": False, "kind": "readiness", "error": str(exc)}, status=503)

    async def _auth(self, request: web.Request, raw_body: bytes) -> tuple[bool, str]:
        if self.bearer_token:
            auth = request.headers.get("Authorization", "")
            if auth != f"Bearer {self.bearer_token}":
                return False, "bad_bearer"
        headers = {k: v for k, v in request.headers.items()}
        secret_map = {'default': self.hmac_secret} if self.hmac_secret else {}
        previous_secret = getattr(self.bot.settings, 'ingress_previous_hmac_secret', '')
        if previous_secret:
            secret_map[str(getattr(self.bot.settings, 'ingress_previous_key_id', 'previous') or 'previous')] = previous_secret
        ok, reason = verify_signed_request(path=request.path, raw_body=raw_body, headers=headers, hmac_secret=secret_map)
        if not ok and self.strict_auth:
            return False, reason
        return True, reason

    async def handle_transport_event(self, request: web.Request) -> web.Response:
        raw = await request.read()
        ok, reason = await self._auth(request, raw)
        if not ok:
            return web.json_response({"ok": False, "error": reason}, status=401)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)
        errors = validate_transport_event(payload if isinstance(payload, dict) else {})
        if errors:
            return web.json_response({"ok": False, "errors": errors}, status=400)
        idem = str(request.headers.get("X-Idempotency-Key") or payload.get("event_id") or "")
        if idem and not await self.bot.community_store.claim_idempotency_key(f"incoming:{idem}", ttl_seconds=600):
            return web.json_response({"ok": True, "duplicate": True})
        await self.bot.handle_incoming_transport_event(payload)
        return web.json_response({"ok": True})

    async def handle_admin_event(self, request: web.Request) -> web.Response:
        raw = await request.read()
        ok, reason = await self._auth(request, raw)
        if not ok:
            return web.json_response({"ok": False, "error": reason}, status=401)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)
        errors = validate_admin_envelope(payload if isinstance(payload, dict) else {})
        if errors:
            return web.json_response({"ok": False, "errors": errors}, status=400)
        idem = str(request.headers.get("X-Idempotency-Key") or payload.get("idempotency_key") or payload.get("event_id") or "")
        if idem and not await self.bot.community_store.claim_idempotency_key(f"incoming-admin:{idem}", ttl_seconds=600):
            return web.json_response(build_signed_response(action="duplicate", ok=True, payload={"duplicate": True}))
        response = await self.bot.handle_incoming_admin_event(payload)
        return web.json_response(response)


    async def handle_metrics(self, request: web.Request) -> web.Response:
        ok, reason = await self._metrics_auth(request)
        if not ok:
            return web.json_response({"ok": False, "error": reason}, status=401)
        text = self.bot.build_metrics_text()
        return web.Response(text=text, content_type="text/plain; version=0.0.4")
