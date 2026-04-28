from __future__ import annotations

import asyncio
import json
import logging
import time
import hashlib
import hmac
import os
from typing import Any
from urllib.parse import urlparse

from telegram.ext import Application

from nmbot.bridge import bridge_auth_ok, handle_discord_bridge_event
from nmbot.event_contracts import validate_transport_event

logger = logging.getLogger(__name__)


def render_metrics(application: Application) -> str:
    db = application.bot_data['db']
    cfg = application.bot_data['config']
    metrics = db.metrics_snapshot()
    health = db.db_health()
    metrics['active_locks'] = int(health.get('active_locks', 0))
    metrics['feed_backlog'] = int(health.get('feed_backlog', 0))
    metrics['broadcast_backlog'] = int(health.get('broadcast_backlog', 0))
    lines = [
        f'nmtelegrambot_info{{version="{application.bot_data.get("version", "unknown")}",mode="{cfg.bot_mode}",instance="{cfg.instance_id}"}} 1',
    ]
    for key, value in sorted(metrics.items()):
        lines.append(f'nmtelegrambot_{key} {int(value)}')
    return "\n".join(lines) + "\n"


async def start_health_server(application: Application, *, host: str, port: int) -> asyncio.base_events.Server | None:
    if port <= 0:
        return None

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        status_code = 200
        content_type = 'application/json; charset=utf-8'
        body = b''
        try:
            line = await reader.readline()
            parts = line.decode('utf-8', errors='ignore').strip().split()
            method = parts[0].upper() if len(parts) >= 1 else 'GET'
            path = parts[1] if len(parts) >= 2 else '/'
            headers: dict[str, str] = {}
            while True:
                header = await reader.readline()
                if not header or header in {b'\r\n', b'\n'}:
                    break
                decoded = header.decode('utf-8', errors='ignore').strip()
                if ':' in decoded:
                    k, v = decoded.split(':', 1)
                    headers[k.strip().lower()] = v.strip()
            cfg = application.bot_data['config']
            length = int(headers.get('content-length', '0') or '0')
            raw_body = await reader.readexactly(length) if length > 0 else b''
            allowed_paths = {'/', '/healthz', '/readyz', '/metrics', '/push/security', '/push/feed', '/internal/discord/event', '/internal/bridge/event'}
            if path not in allowed_paths:
                status_code = 404
                payload = {'ok': False, 'path': path, 'error': 'not_found'}
                body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            elif path in {'/push/security', '/push/feed', '/internal/discord/event', '/internal/bridge/event'}:
                if method != 'POST':
                    status_code = 405
                    payload = {'ok': False, 'path': path, 'error': 'method_not_allowed'}
                    body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
                else:
                    payload, code = await _handle_signed_push(application, path=path, headers=headers, raw_body=raw_body)
                    status_code = code
                    body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            elif method != 'GET':
                status_code = 405
                payload: dict[str, Any] = {'ok': False, 'path': path, 'error': 'method_not_allowed'}
                body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            elif cfg.health_http_token and headers.get('x-health-token', '') != cfg.health_http_token:
                status_code = 403
                payload = {'ok': False, 'path': path, 'error': 'forbidden'}
                body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            elif path == '/metrics':
                content_type = 'text/plain; version=0.0.4; charset=utf-8'
                body = render_metrics(application).encode('utf-8')
            else:
                payload = await build_health_payload(application, minimal=cfg.health_http_minimal)
                if path == '/readyz' and not payload.get('ready', False):
                    status_code = 503
                body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            writer.write(
                f"HTTP/1.1 {status_code} {'OK' if status_code < 400 else 'ERROR'}\r\n"
                f"Content-Type: {content_type}\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Connection: close\r\n\r\n".encode('utf-8') + body
            )
            await writer.drain()
        except Exception:
            logger.exception('health http handler failed')
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, host=host, port=port)
    logger.info('health http server listening on %s:%s', host, port)
    return server




def _probe_database(db) -> dict[str, Any]:
    backend = getattr(db, 'backend_name', 'sqlite')
    result: dict[str, Any] = {'ok': False, 'backend': backend}
    try:
        with db.connect() as connection:
            row = connection.execute('SELECT 1 AS ok').fetchone()
            schema_row = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
        probe_value = None
        if row is not None:
            if isinstance(row, dict):
                probe_value = row.get('ok')
            else:
                try:
                    probe_value = row['ok']
                except Exception:
                    probe_value = row[0]
        result['ok'] = bool(probe_value == 1)
        result['schema_version'] = str(schema_row['value']) if schema_row is not None else None
    except Exception as exc:
        result['error'] = str(exc)
    return result

def _database_target(cfg, db) -> str:
    backend = getattr(db, 'backend_name', 'sqlite')
    if backend == 'postgresql' and cfg.database_url:
        parsed = urlparse(cfg.database_url)
        host = parsed.hostname or 'localhost'
        port = parsed.port or 5432
        database = (parsed.path or '/').lstrip('/') or '-'
        return f'postgresql://{host}:{port}/{database}'
    return str(cfg.sqlite_path)


async def build_health_payload(application: Application, *, minimal: bool = False) -> dict[str, Any]:
    cfg = application.bot_data['config']
    db = application.bot_data['db']
    started_at = float(application.bot_data.get('started_at') or time.time())
    db_health = db.db_health()
    backend = getattr(db, 'backend_name', 'sqlite')
    paths_ready = cfg.data_dir.exists() and cfg.log_file.parent.exists() and cfg.templates_dir.exists() and cfg.artifact_root.exists()
    db_probe = _probe_database(db)
    db_ready = bool(cfg.sqlite_path.exists()) and bool(db_probe.get('ok')) if backend == 'sqlite' else bool(db_probe.get('ok'))
    runtime_ready = paths_ready and db_ready
    last_status_ok_at = db.runtime_value('last_status_ok_at', '')
    ready = runtime_ready
    limiter = application.bot_data.get('rate_limiter')
    limiter_diag = limiter.diagnostics() if hasattr(limiter, 'diagnostics') else {'backend_mode': getattr(limiter, 'backend_mode', 'sqlite') if limiter else 'sqlite'}
    payload: dict[str, Any] = {
        'ok': ready,
        'ready': ready,
        'backend': backend,
        'database_target': _database_target(cfg, db),
        'database_probe': db_probe,
        'rate_limit_backend': limiter_diag.get('backend_mode', 'sqlite'),
        'mode': cfg.bot_mode,
        'instance_id': cfg.instance_id,
        'strict_compatibility_gate': bool(getattr(cfg, 'strict_compatibility_gate', False)),
        'uptime_seconds': int(max(time.time() - started_at, 0)),
        'public_http_server_url': cfg.public_http_server_url or cfg.webhook_url,
        'webhook_port': cfg.webhook_port,
        'webhook_listen': cfg.webhook_listen,
        'status_url_configured': bool(cfg.server_status_url),
        'feed_url_configured': bool(cfg.announcement_feed_url),
        'last_status_ok_at': last_status_ok_at or None,
    }
    if minimal:
        return payload
    payload['db'] = db_health
    payload['paths'] = {
        'database_target': _database_target(cfg, db),
        'database_probe': db_probe,
        'data_dir': str(cfg.data_dir),
        'shared_dir': str(cfg.shared_dir),
        'shared_dir_available': bool(cfg.shared_dir_available),
        'artifact_root': str(cfg.artifact_root),
        'log_file': str(cfg.log_file),
        'templates_dir': str(cfg.templates_dir),
    }
    if backend == 'sqlite':
        payload['paths']['database_file'] = str(cfg.sqlite_path)
    payload['maintenance'] = db.get_maintenance_state()
    payload['redis'] = limiter_diag
    payload['housekeeping'] = db.get_json_state('housekeeping:last', default={})
    payload['build_info'] = application.bot_data.get('build_manifest', {})
    payload['last_security_challenge_poll_at'] = db.runtime_value('last_security_challenge_poll_at', '') or None
    return payload
