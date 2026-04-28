from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from nmbot.bridge import ReplayGuard, bridge_auth_ok, deliver_event_to_vk
from nmbot.logger import get_remote_logs_diagnostics

LOGGER = logging.getLogger(__name__)


def _compute_readiness(server: Any) -> tuple[bool, dict[str, Any]]:
    settings = server.settings
    startup = server.runtime_status.get('startup_checks', {})
    database = server.storage.database_diagnostics()
    warnings: list[str] = list(startup.get('warnings', []))
    if settings.app_profile == 'bothost' and settings.health_http_listen != '0.0.0.0':
        warnings.append('bothost_not_bound_to_0.0.0.0')
    if settings.discord_bridge_url.startswith('http://127.0.0.1') or settings.discord_bridge_url.startswith('http://localhost'):
        warnings.append('discord_bridge_url_points_to_localhost')
    ok = bool(startup.get('vk_group_lookup')) and bool(startup.get('vk_longpoll_lookup')) and bool(database.get('connection_ok'))
    payload = {
        'ok': ok,
        'kind': 'readiness',
        'service': 'NMVKBot',
        'profile': settings.app_profile,
        'entrypoint': settings.entrypoint,
        'http_listen': f'{settings.health_http_listen}:{settings.health_http_port}',
        'bridge_outbound_enabled': bool(settings.discord_bridge_url),
        'pending_outbound_queue': server.storage.pending_outbound_count(),
        'dead_outbound_queue': server.storage.dead_outbound_count(),
        'open_tickets': server.storage.open_tickets_count(),
        'startup_checks': startup,
        'replay_cache_size': server.replay_guard.size(),
        'remote_logs': get_remote_logs_diagnostics(),
        'database': database,
        'shared_dir': settings.shared_dir,
        'warnings': warnings,
    }
    return ok, payload


class _BridgeHTTPRequestHandler(BaseHTTPRequestHandler):
    server: Any

    def log_message(self, fmt: str, *args: Any) -> None:
        LOGGER.info('HTTP %s - %s', self.address_string(), fmt % args)

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _health_auth_ok(self) -> bool:
        settings = self.server.settings
        if settings.health_http_public:
            return True
        token = str(settings.health_http_token or '').strip()
        if not token:
            return True
        auth = self.headers.get('Authorization', '').strip()
        if auth == f'Bearer {token}':
            return True
        parsed = urlparse(self.path)
        return parse_qs(parsed.query).get('token', [''])[0] == token

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path in {'/internal/health/liveness', '/healthz', '/internal/health/readiness', '/readyz'} and not self._health_auth_ok():
            self._json(401, {'ok': False, 'error': 'bad_health_token'})
            return
        if path in {'/internal/health/liveness', '/healthz'}:
            self._json(200, {'ok': True, 'kind': 'liveness', 'service': 'NMVKBot'})
            return
        if path in {'/internal/health/readiness', '/readyz'}:
            ok, payload = _compute_readiness(self.server)
            self._json(200 if ok else 503, payload)
            return
        self._json(404, {'ok': False, 'error': 'not_found'})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path not in {'/internal/discord/event', '/internal/bridge/event'}:
            self._json(404, {'ok': False, 'error': 'not_found'})
            return
        length = int(self.headers.get('Content-Length') or '0')
        raw_body = self.rfile.read(length)
        ok, reason = bridge_auth_ok(
            self.server.settings,
            path=path,
            raw_body=raw_body,
            headers={key: value for key, value in self.headers.items()},
            replay_guard=self.server.replay_guard,
        )
        if not ok:
            self._json(401, {'ok': False, 'error': reason})
            return
        try:
            event = json.loads(raw_body.decode('utf-8'))
        except json.JSONDecodeError:
            self._json(400, {'ok': False, 'error': 'invalid_json'})
            return
        if not isinstance(event, dict):
            self._json(400, {'ok': False, 'error': 'payload_not_object'})
            return
        try:
            result = deliver_event_to_vk(self.server.settings, self.server.vk, self.server.storage, event)
        except ValueError as exc:
            self._json(400, {'ok': False, 'error': str(exc)})
            return
        except Exception:
            LOGGER.exception('Failed to handle incoming bridge event')
            self._json(500, {'ok': False, 'error': 'delivery_failed'})
            return
        self._json(200, {'ok': True, **result})


@dataclass(slots=True)
class VKBridgeServer:
    settings: Any
    vk: Any
    storage: Any
    runtime_status: dict[str, Any]
    replay_guard: ReplayGuard
    httpd: ThreadingHTTPServer | None = None
    thread: threading.Thread | None = None

    def start(self) -> None:
        port = int(getattr(self.settings, 'health_http_port', 0) or 0)
        if port <= 0:
            LOGGER.info('VK bridge HTTP server disabled')
            return
        host = str(getattr(self.settings, 'health_http_listen', '127.0.0.1') or '127.0.0.1')
        self.httpd = ThreadingHTTPServer((host, port), _BridgeHTTPRequestHandler)
        self.httpd.settings = self.settings  # type: ignore[attr-defined]
        self.httpd.vk = self.vk  # type: ignore[attr-defined]
        self.httpd.storage = self.storage  # type: ignore[attr-defined]
        self.httpd.runtime_status = self.runtime_status  # type: ignore[attr-defined]
        self.httpd.replay_guard = self.replay_guard  # type: ignore[attr-defined]
        self.thread = threading.Thread(target=self.httpd.serve_forever, name='NMVKBotHTTP', daemon=True)
        self.thread.start()
        LOGGER.info('VK bridge HTTP server started on %s:%s', host, port)

    def stop(self) -> None:
        if self.httpd is None:
            return
        self.httpd.shutdown()
        self.httpd.server_close()
        self.httpd = None
        LOGGER.info('VK bridge HTTP server stopped')
