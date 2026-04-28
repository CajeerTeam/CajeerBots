from __future__ import annotations

import argparse
import html
import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from telegram import BotCommand, BotCommandScopeChat, BotCommandScopeDefault, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, ApplicationBuilder, CallbackQueryHandler, CommandHandler

from nmbot import __version__
from nmbot.config import ConfigValidationError, load_config
from nmbot.delivery import OutgoingPayload, build_inline_buttons, send_payloads_bounded
from nmbot.event_contracts import build_transport_event, normalize_admin_action
from nmbot.handlers.admin import (
    admin_help_handler,
    adminstats_handler,
    announce_handler,
    broadcast_handler,
    chatsettings_handler,
    diag_handler,
    health_handler,
    pull_announcements_handler,
    schedule_handler,
)
from nmbot.handlers.callbacks import menu_callback_handler
from nmbot.handlers.errors import generic_error_handler, permission_error_handler, rate_limit_error_handler
from nmbot.handlers.linking import link_handler
from nmbot.handlers.ops import (
    ops_help_handler,
    adminsite_handler,
    approval_handler,
    alerts_handler,
    delivery_handler,
    delivery_help_handler,
    incident_handler,
    maintenance_handler,
    metrics_handler,
    mode_handler,
    onboarding_handler,
    rbac_handler,
    security_help_handler,
    subscribe_handler,
    template_handler,
    timezone_handler,
    unsubscribe_handler,
    webhook_handler,
)
from nmbot.handlers.public import (
    help_handler,
    links_handler,
    me_handler,
    notifications_handler,
    online_handler,
    quiethours_handler,
    sessions_handler,
    start_handler,
    stats_handler,
    status_handler,
    twofa_handler,
)
from nmbot.handlers.security import security_handler
from nmbot.health_http import start_health_server
from nmbot.logging_setup import configure_logging
from nmbot.services.access import RateLimiter
from nmbot.services.server_api import ServerStatusClient, push_external_event
from nmbot.storage import create_database
from nmbot.templates import feed_text

logger = logging.getLogger(__name__)

_TRANSIENT_DB_ERROR_MARKERS = (
    'No address associated with hostname',
    'Temporary failure in name resolution',
    'could not translate host name',
    'Name or service not known',
    'Connection refused',
    'Connection timed out',
    'timeout expired',
    'server closed the connection unexpectedly',
    'connection is bad',
    'connection not open',
    'Connection reset',
    'Network is unreachable',
)


def _is_transient_database_error(exc: BaseException) -> bool:
    message = str(exc)
    if any(marker.lower() in message.lower() for marker in _TRANSIENT_DB_ERROR_MARKERS):
        return True
    cls_name = exc.__class__.__name__.lower()
    module = exc.__class__.__module__.lower()
    if cls_name in {'operationalerror', 'interfaceerror'} and 'psycopg' in module:
        return True
    if cls_name == 'operationalerror' and 'sqlite3' in module and 'database is locked' in message.lower():
        return True
    errno_value = getattr(exc, 'errno', None)
    return errno_value in {-5, -3, -2, 101, 104, 110, 111}


def _safe_set_runtime_value(db, key: str, value: str) -> None:
    try:
        db.set_runtime_value(key, value)
    except Exception as exc:
        if _is_transient_database_error(exc):
            logger.debug('runtime state update skipped because database is temporarily unavailable: %s', key)
            return
        raise


def _cleanup_has_changes(counters: dict | None, housekeeping: dict | None) -> bool:
    for payload in (counters or {}, housekeeping or {}):
        for key, value in payload.items():
            if key in {'backend', 'at'}:
                continue
            try:
                if int(value) != 0:
                    return True
            except (TypeError, ValueError):
                if value not in ('', None, False):
                    return True
    return False



def _backend_mode(config) -> str:
    return 'postgresql' if str(getattr(config, 'database_url', '') or '').startswith(('postgres://', 'postgresql://')) else 'sqlite'


def _artifact_root(config) -> Path:
    return config.artifact_root


def _cleanup_runtime_artifacts(config) -> dict[str, int]:
    now = time.time()
    removed = {'backups': 0, 'exports': 0}
    artifact_root = _artifact_root(config)
    for dirname, days in [('backups', getattr(config, 'backup_retention_days', 30)), ('exports', getattr(config, 'export_retention_days', 30))]:
        root = artifact_root / dirname
        if not root.exists():
            continue
        ttl = max(0, int(days)) * 86400
        for path in root.rglob('*'):
            if path.is_file() and now - path.stat().st_mtime > ttl:
                path.unlink(missing_ok=True)
                removed[dirname] += 1
    return removed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='NeverMine Telegram Bot')
    parser.add_argument('--check-config', action='store_true', help='validate .env and exit')
    parser.add_argument('--prepare-runtime', action='store_true', help='prepare runtime directories and state')
    parser.add_argument('--readiness-check', action='store_true', help='validate runtime readiness and exit')
    parser.add_argument('--safe-startup', action='store_true', help='start runtime without Telegram transport')
    return parser.parse_args(argv)


def _utc_now_iso() -> str:
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')


def _load_build_manifest(config) -> dict[str, object]:
    path = config.build_manifest_path
    if not path.exists():
        return {
            'version': __version__,
            'build': _utc_now_iso(),
            'storage_backend': 'sqlite|postgresql',
            'schema_version': 'unknown',
            'features': ['telegram-runtime', 'sqlite', 'health-http', 'delivery-ledger'],
        }
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {'version': __version__, 'build': 'unknown'}


def _secret_from_env(name: str) -> str:
    value = os.getenv(name, '').strip()
    if value:
        return value
    file_path = os.getenv(f'{name}_FILE', '').strip()
    if not file_path:
        return ''
    try:
        return Path(file_path).read_text(encoding='utf-8').strip()
    except Exception:
        return ''


def _env_flag(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name, '').strip()
    if not value:
        return default
    return value.lower() in {'1', 'true', 'yes', 'on'}


def _redis_backend_required() -> bool:
    return _env_flag('REQUIRE_REDIS_BACKEND') or _env_flag('REDIS_REQUIRED')


async def _push_external_admin_snapshot(application: Application, *, reason: str, actor_user_id: int = 0) -> bool:
    db = application.bot_data['db']
    url = os.getenv('EXTERNAL_ADMIN_API_URL', '').strip()
    if not url:
        return False
    payload = normalize_admin_action(action='incident_snapshot', payload={
        'kind': 'incident_snapshot',
        'reason': reason,
        'version': application.bot_data.get('version', 'unknown'),
        'build_info': application.bot_data.get('build_manifest', {}),
        'snapshot': db.collect_incident_snapshot(),
    }, actor_user_id=actor_user_id)
    event_id = db.queue_external_sync_event(event_kind='external_admin_snapshot', destination=url, payload=payload) if hasattr(db, 'queue_external_sync_event') else 0
    try:
        ok = await push_external_event(url, payload, bearer_token=_secret_from_env('EXTERNAL_ADMIN_API_TOKEN'), hmac_secret=_secret_from_env('INBOUND_HMAC_SECRET'), key_id=os.getenv('OUTBOUND_KEY_ID', 'v1').strip() or 'v1')
        if event_id and hasattr(db, 'mark_external_sync_event'):
            db.mark_external_sync_event(event_id, status='sent' if ok else 'retry', error='' if ok else 'push_failed')
        return ok
    except Exception as exc:
        if event_id and hasattr(db, 'mark_external_sync_event'):
            db.mark_external_sync_event(event_id, status='retry', error=str(exc))
        return False


async def _push_community_event(application: Application, *, event_kind: str, payload: dict) -> None:
    db = application.bot_data['db']
    cfg = application.bot_data['config']

    destinations: list[tuple[str, str, str, str]] = []
    if getattr(cfg, 'discord_bridge_url', ''):
        destinations.append((
            cfg.discord_bridge_url,
            getattr(cfg, 'discord_bridge_bearer_token', '') or _secret_from_env('EXTERNAL_ADMIN_API_TOKEN'),
            getattr(cfg, 'discord_bridge_hmac_secret', '') or _secret_from_env('INBOUND_HMAC_SECRET'),
            'discord',
        ))
    for env_name, label in (('COMMUNITY_CORE_EVENT_URL', 'community-core'), ('VK_BRIDGE_URL', 'vk')):
        url = os.getenv(env_name, '').strip()
        if url:
            destinations.append((url, _secret_from_env('EXTERNAL_ADMIN_API_TOKEN'), _secret_from_env('INBOUND_HMAC_SECRET'), label))

    for url, bearer_token, hmac_secret, label in destinations:
        envelope = build_transport_event(event_type=event_kind, payload=payload, source='telegram-bridge', ttl_seconds=300)
        event_id = db.queue_external_sync_event(event_kind=event_kind, destination=url, payload=envelope) if hasattr(db, 'queue_external_sync_event') else 0
        try:
            ok = await push_external_event(
                url,
                envelope,
                bearer_token=bearer_token,
                hmac_secret=hmac_secret,
                key_id=os.getenv('OUTBOUND_KEY_ID', 'v1').strip() or 'v1',
                timeout_seconds=getattr(cfg, 'request_timeout_seconds', 5.0),
            )
            if event_id and hasattr(db, 'mark_external_sync_event'):
                db.mark_external_sync_event(event_id, status='sent' if ok else 'retry', error='' if ok else f'{label}_push_failed')
        except Exception as exc:
            if event_id and hasattr(db, 'mark_external_sync_event'):
                db.mark_external_sync_event(event_id, status='retry', error=str(exc))


def prepare_runtime() -> None:
    config = load_config()
    configure_logging(config.log_level, config.log_file, log_format=config.log_format)
    config.data_dir.mkdir(parents=True, exist_ok=True)
    config.log_file.parent.mkdir(parents=True, exist_ok=True)
    if _backend_mode(config) == 'sqlite':
        config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    config.templates_dir.mkdir(parents=True, exist_ok=True)
    artifact_root = _artifact_root(config)
    (artifact_root / 'backups').mkdir(parents=True, exist_ok=True)
    (artifact_root / 'exports').mkdir(parents=True, exist_ok=True)
    db = create_database(config)
    db.set_runtime_value('last_runtime_prepare_at', _utc_now_iso())
    db.set_runtime_value('last_runtime_version', __version__)
    db_health = db.db_health()
    logger.info('runtime prepared backend=%s schema=%s', getattr(db, 'backend_name', 'sqlite'), db_health['schema_version'])


def readiness_check() -> None:
    config = load_config()
    configure_logging(config.log_level, config.log_file, log_format=config.log_format)
    if config.bot_mode == 'webhook' and not config.webhook_url:
        raise ConfigValidationError('WEBHOOK_URL обязателен для BOT_MODE=webhook')
    required_dirs = [config.data_dir, config.log_file.parent, config.templates_dir, _artifact_root(config)]
    if _backend_mode(config) == 'sqlite':
        required_dirs.append(config.sqlite_path.parent)
    missing_dirs = [str(item) for item in required_dirs if not item.exists()]
    if missing_dirs:
        raise ConfigValidationError('Readiness failed: missing directories: ' + ', '.join(missing_dirs))
    try:
        db = create_database(config)
        db_health = db.db_health()
        with db.connect() as connection:
            probe = connection.execute('SELECT 1 AS ok').fetchone()
            schema_row = connection.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
        if not probe:
            raise ConfigValidationError('Readiness failed: database probe returned no rows')
        if not schema_row:
            raise ConfigValidationError('Readiness failed: schema_meta недоступна')
    except FileNotFoundError as exc:
        raise ConfigValidationError(str(exc)) from exc
    logger.info('readiness ok backend=%s schema=%s', getattr(db, 'backend_name', 'sqlite'), db_health['schema_version'])


async def _send_operator_alert(application: Application, *, kind: str, text: str, severity: str = 'warning', payload: dict | None = None) -> None:
    cfg = application.bot_data['config']
    db = application.bot_data['db']
    incident_key = str((payload or {}).get('incident_key') or f'{kind}:{severity}')
    try:
        if hasattr(db, 'operator_alert_muted') and db.operator_alert_muted(incident_key):
            return
        key = f'operator_alert:{kind}'
        state_key = f'operator_alert_state:{kind}'
        last = float(db.runtime_value(key, '0') or '0')
        previous_state = db.runtime_value(state_key, '')
        now = time.time()
        state_value = severity
        if previous_state == state_value and now - last < cfg.operator_alert_cooldown_seconds:
            return
        db.set_runtime_value(key, str(now))
        db.set_runtime_value(state_key, state_value)
        alert_id = db.upsert_operator_alert(incident_key=incident_key, kind=kind, severity=severity, summary=text[:4000], payload=payload) if hasattr(db, 'upsert_operator_alert') else 0
    except Exception as exc:
        if _is_transient_database_error(exc):
            logger.debug('operator alert skipped because database is temporarily unavailable: kind=%s', kind)
            return
        raise
    targets = sorted(cfg.admin_like_ids | cfg.telegram_owner_ids)
    if not targets:
        return
    compact_payload = ''
    if payload:
        try:
            compact_payload = '\n<pre>' + html.escape(json.dumps(payload, ensure_ascii=False, sort_keys=True)[:1500]) + '</pre>'
        except Exception:
            compact_payload = ''
    message = f"⚠️ NMTelegramBot alert\nID: <code>{alert_id or '-'}</code>\nType: <b>{html.escape(kind)}</b>\nSeverity: <b>{html.escape(severity)}</b>\nMessage: <code>{html.escape(text[:3000])}</code>{compact_payload}"
    for chat_id in targets:
        try:
            await application.bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
        except Exception:
            logger.exception('failed to send operator alert to chat_id=%s', chat_id)


async def _run_status_refresh_loop(application: Application, stop_event: asyncio.Event) -> None:
    cfg = application.bot_data['config']
    db = application.bot_data['db']
    status_client: ServerStatusClient = application.bot_data['status_client']
    if cfg.status_refresh_seconds <= 0 or not status_client.is_configured():
        return
    while not stop_event.is_set():
        try:
            status = await status_client.fetch_status(force=True)
            now = _utc_now_iso()
            db.set_runtime_value('last_status_refresh_at', now)
            if status.ok:
                db.set_runtime_value('last_status_ok_at', now)
                db.set_runtime_value('last_status_error', '')
                if status.latency_ms is not None:
                    db.set_runtime_value('last_status_latency_ms', str(status.latency_ms))
        except Exception as exc:
            db.set_runtime_value('last_status_error_at', _utc_now_iso())
            db.set_runtime_value('last_status_error', str(exc))
            logger.exception('status refresh loop failed')
            await _send_operator_alert(application, kind='status_loop', text=str(exc))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=cfg.status_refresh_seconds)
        except TimeoutError:
            continue


async def _run_cleanup_loop(application: Application, stop_event: asyncio.Event) -> None:
    cfg = application.bot_data['config']
    db = application.bot_data['db']
    next_transient_db_log_at = 0.0
    while not stop_event.is_set():
        try:
            if db.acquire_leader_lock(name='cleanup', owner=cfg.instance_id, ttl_seconds=cfg.leader_lock_ttl_seconds):
                counters = db.cleanup(
                    interaction_retention_days=cfg.interaction_retention_days,
                    admin_action_retention_days=cfg.admin_action_retention_days,
                    runtime_state_retention_days=cfg.runtime_state_retention_days,
                    dead_letter_retention_days=cfg.dead_letter_retention_days,
                )
                housekeeping = db.housekeeping()
                if hasattr(db, 'cleanup_runtime_state'):
                    housekeeping['runtime_values'] = db.cleanup_runtime_state(older_than_days=getattr(cfg, 'runtime_state_retention_days', 30)).get('runtime_values', 0)
                _safe_set_runtime_value(db, 'last_cleanup_at', _utc_now_iso())
                _safe_set_runtime_value(db, 'last_housekeeping_at', housekeeping.get('at', ''))
                onboarding = db.list_onboarding()
                if onboarding:
                    pending = [item for item in onboarding if str(item.get('status')) == 'pending']
                    if pending:
                        await _send_operator_alert(application, kind='onboarding_pending', text=f'Ожидающие onboarding-чаты: {len(pending)}')
                if _cleanup_has_changes(counters, housekeeping):
                    logger.info('cleanup finished: %s housekeeping=%s', counters, housekeeping)
                else:
                    logger.debug('cleanup finished: %s housekeeping=%s', counters, housekeeping)
        except Exception as exc:
            if _is_transient_database_error(exc):
                _safe_set_runtime_value(db, 'last_cleanup_error', str(exc))
                now = time.time()
                if now >= next_transient_db_log_at:
                    logger.warning('cleanup skipped: database is temporarily unavailable: %s', exc)
                    next_transient_db_log_at = now + max(1800, int(getattr(cfg, 'operator_alert_cooldown_seconds', 300)))
                else:
                    logger.debug('cleanup skipped: database is temporarily unavailable: %s', exc)
            else:
                _safe_set_runtime_value(db, 'last_cleanup_error', str(exc))
                logger.exception('cleanup loop failed')
                await _send_operator_alert(application, kind='cleanup_loop', text=str(exc))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=cfg.cleanup_interval_seconds)
        except TimeoutError:
            continue


def _paused_until_ts(db) -> float:
    try:
        return float(db.runtime_value('delivery:paused_until_ts', '0') or '0')
    except Exception:
        return 0.0


def _dry_run_enabled(db) -> bool:
    return db.runtime_value('delivery:dry_run', '0') == '1'


def _security_challenge_keyboard(challenge_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton('Approve', callback_data=f'security:approve:{challenge_id}'), InlineKeyboardButton('Deny', callback_data=f'security:deny:{challenge_id}')]])


async def _maybe_reconcile_webhook(application: Application) -> None:
    cfg = application.bot_data['config']
    db = application.bot_data['db']
    if cfg.bot_mode != 'webhook' or not cfg.webhook_url or not cfg.auto_reconcile_webhook:
        return
    token_part = cfg.telegram_bot_token.split(':', 1)[0]
    target_url = cfg.webhook_url.rstrip('/') + '/' + f"{cfg.webhook_path_prefix}/{token_part}"
    try:
        info = await application.bot.get_webhook_info()
        if info.url != target_url:
            ok = await application.bot.set_webhook(url=target_url, secret_token=cfg.webhook_secret_token or None)
            db.set_runtime_value('last_webhook_reconcile_at', _utc_now_iso())
            db.set_runtime_value('last_webhook_reconcile_ok', '1' if ok else '0')
            await _send_operator_alert(application, kind='webhook_reconcile', text=f'mismatch fixed={ok}', severity='info', payload={'current': info.url, 'target': target_url})
    except Exception as exc:
        db.set_runtime_value('last_webhook_reconcile_error', str(exc))
        await _send_operator_alert(application, kind='webhook_reconcile', text=str(exc), severity='error')


async def _poll_security_challenges(application: Application, stop_event: asyncio.Event) -> None:
    cfg = application.bot_data['config']
    db = application.bot_data['db']
    status_client: ServerStatusClient = application.bot_data['status_client']
    if not cfg.security_challenges_url:
        return
    while not stop_event.is_set():
        try:
            if db.acquire_leader_lock(name='security_challenges', owner=cfg.instance_id, ttl_seconds=cfg.leader_lock_ttl_seconds):
                items = await status_client.fetch_security_challenges()
                for item in items:
                    replay_key = item.nonce or f"{item.challenge_id}:{item.timestamp or item.created_at}"
                    if hasattr(db, 'claim_replay_guard') and not db.claim_replay_guard('security_challenge', replay_key, ttl_seconds=cfg.security_nonce_ttl_seconds):
                        continue
                    notice_payload = {'player_name': item.player_name, 'server_name': item.server_name, 'ip_address': item.ip_address, 'message': item.message, 'created_at': item.created_at, 'expires_at': item.expires_at, 'action': item.action, 'metadata': item.metadata or {}}
                    if hasattr(db, 'upsert_security_challenge_notice') and db.upsert_security_challenge_notice(item.challenge_id, action=item.action, payload=notice_payload):
                        lines = ['<b>Security challenge</b>', f"Player: <b>{html.escape(item.player_name)}</b>"]
                        if item.title:
                            lines.append(f"Title: <b>{html.escape(item.title)}</b>")
                        if item.server_name:
                            lines.append(f"Server: <code>{html.escape(item.server_name)}</code>")
                        if item.ip_address:
                            lines.append(f"IP: <code>{html.escape(item.ip_address)}</code>")
                        if item.message:
                            lines.append(f"Message: <code>{html.escape(item.message)}</code>")
                        meta = item.metadata or {}
                        if meta.get('device_label'):
                            lines.append(f"Device: <code>{html.escape(str(meta.get('device_label')))}</code>")
                        if meta.get('fingerprint'):
                            lines.append(f"Fingerprint: <code>{html.escape(str(meta.get('fingerprint')))}</code>")
                        if meta.get('attempt_type'):
                            lines.append(f"Attempt: <code>{html.escape(str(meta.get('attempt_type')))}</code>")
                        if item.expires_at:
                            lines.append(f"Expires: <code>{html.escape(item.expires_at)}</code>")
                        target_chats = set(cfg.admin_like_ids | cfg.telegram_owner_ids)
                        if item.telegram_user_id:
                            prefs = db.get_user_notification_prefs(item.telegram_user_id) if hasattr(db, 'get_user_notification_prefs') else {}
                            if prefs.get('security_enabled', True):
                                target_chats.add(int(item.telegram_user_id))
                        elif hasattr(db, 'find_linked_account_by_player_name'):
                            linked = db.find_linked_account_by_player_name(item.player_name)
                            if linked is not None:
                                prefs = db.get_user_notification_prefs(linked.user_id) if hasattr(db, 'get_user_notification_prefs') else {}
                                if prefs.get('security_enabled', True):
                                    target_chats.add(int(linked.chat_id or linked.user_id))
                        for chat_id in sorted(target_chats):
                            try:
                                await application.bot.send_message(chat_id=chat_id, text='\n'.join(lines), parse_mode='HTML', reply_markup=_security_challenge_keyboard(item.challenge_id))
                            except Exception:
                                logger.exception('failed to send security challenge notification')
                        await _send_operator_alert(application, kind='security_challenge_pending', text=f'{item.challenge_id}:{item.player_name}', severity='warning', payload={'incident_key': f'security:{item.challenge_id}'})
                db.set_runtime_value('last_security_challenge_poll_at', _utc_now_iso())
        except Exception as exc:
            db.set_runtime_value('last_security_challenge_error', str(exc))
            await _send_operator_alert(application, kind='security_challenge_loop', text=str(exc), severity='error')
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=max(10, cfg.announcement_feed_interval_seconds))
        except TimeoutError:
            continue


async def _dispatch_scheduled_loop(application: Application, stop_event: asyncio.Event) -> None:
    cfg = application.bot_data['config']
    db = application.bot_data['db']
    next_transient_db_log_at = 0.0
    while not stop_event.is_set():
        try:
            if db.acquire_leader_lock(name='scheduled', owner=cfg.instance_id, ttl_seconds=cfg.leader_lock_ttl_seconds):
                due = db.due_scheduled_broadcasts(_utc_now_iso())
                for row in due:
                    source_id = str(row['id'])
                    targets = db.resolve_target_chats(
                        allowed_chat_ids=cfg.telegram_allowed_chat_ids,
                        fallback_chat_id=None,
                        target_scope=str(row['target_scope'] or 'all'),
                        target_tags=[item for item in str(row['target_tags'] or '').split(',') if item],
                        feature='broadcasts',
                    )
                    payload_data = {
                        'text': row['message'],
                        'media_kind': str(row.get('media_kind') or ''),
                        'media_ref': str(row.get('media_ref') or ''),
                        'message_thread_id': row.get('message_thread_id'),
                        'disable_notification': bool(row.get('disable_notification')),
                    }
                    deliveries: list[tuple[int, OutgoingPayload]] = []
                    for chat_id in targets:
                        db.ensure_broadcast_delivery(source_type='scheduled', source_id=source_id, chat_id=chat_id, delivery_key=str(row.get('delivery_key') or ''), payload=payload_data)
                        if db.broadcast_delivery_is_sent(source_type='scheduled', source_id=source_id, chat_id=chat_id):
                            continue
                        allowed, reason = db.should_deliver_now(chat_id=chat_id, tag=str(row.get('target_tags') or '').split(',')[0] if row.get('target_tags') else '') if hasattr(db, 'should_deliver_now') else (True, '')
                        if not allowed:
                            db.mark_broadcast_delivery_failed(source_type='scheduled', source_id=source_id, chat_id=chat_id, error=reason)
                            continue
                        db.mark_broadcast_delivery_attempt(source_type='scheduled', source_id=source_id, chat_id=chat_id)
                        deliveries.append((chat_id, OutgoingPayload(**payload_data, dry_run=_dry_run_enabled(db))))
                    if not deliveries:
                        db.mark_scheduled_broadcast_sent(int(row['id']))
                        continue
                    results = await send_payloads_bounded(
                        application.bot,
                        deliveries,
                        parse_mode=cfg.telegram_parse_mode,
                        max_concurrency=cfg.delivery_max_concurrency,
                        max_per_minute=cfg.delivery_max_per_minute,
                        paused_until_ts=_paused_until_ts(db),
                        db=db,
                    )
                    sent = 0
                    errors: list[str] = []
                    for result in results:
                        if result.ok:
                            sent += 1
                            db.mark_broadcast_delivery_sent(source_type='scheduled', source_id=source_id, chat_id=result.chat_id)
                        else:
                            errors.append(f'{result.chat_id}:{result.error}')
                            db.mark_broadcast_delivery_failed(source_type='scheduled', source_id=source_id, chat_id=result.chat_id, error=result.error)
                            db.enqueue_dead_letter(
                                source_type='scheduled',
                                source_id=source_id,
                                chat_id=result.chat_id,
                                payload=payload_data,
                                error=result.error,
                                retry_count=int(row.get('retry_count') or 0) + 1,
                            )
                    if errors:
                        retry_count = int(row.get('retry_count') or 0) + 1
                        if retry_count <= cfg.delivery_retry_attempts:
                            next_retry_at = (datetime.utcnow() + timedelta(seconds=cfg.delivery_retry_backoff_seconds * retry_count)).strftime('%Y-%m-%d %H:%M:%S')
                            db.mark_scheduled_broadcast_retry(int(row['id']), error='; '.join(errors), retry_count=retry_count, next_retry_at=next_retry_at)
                        else:
                            db.mark_scheduled_broadcast_dead(int(row['id']), error='; '.join(errors), retry_count=retry_count)
                        db.increment_runtime_counter('scheduled_failed_total', 1)
                        await _send_operator_alert(application, kind='scheduled_delivery', text='; '.join(errors)[:3000])
                    else:
                        db.mark_scheduled_broadcast_sent(int(row['id']))
                        db.increment_runtime_counter('scheduled_sent_total', 1)
                    _safe_set_runtime_value(db, 'last_scheduled_dispatch_at', _utc_now_iso())
                    logger.info('scheduled broadcast processed id=%s sent=%s targets=%s', row['id'], sent, len(targets))
        except Exception as exc:
            _safe_set_runtime_value(db, 'last_scheduled_error', str(exc))
            if _is_transient_database_error(exc):
                now = time.time()
                if now >= next_transient_db_log_at:
                    logger.warning('scheduled loop skipped: database is temporarily unavailable: %s', exc)
                    next_transient_db_log_at = now + max(1800, int(getattr(cfg, 'operator_alert_cooldown_seconds', 300)))
                else:
                    logger.debug('scheduled loop skipped: database is temporarily unavailable: %s', exc)
            else:
                logger.exception('scheduled loop failed')
                await _send_operator_alert(application, kind='scheduled_loop', text=str(exc))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=cfg.scheduler_tick_seconds)
        except TimeoutError:
            continue


async def _process_due_feed_deliveries(application: Application) -> int:
    cfg = application.bot_data['config']
    db = application.bot_data['db']
    rows = db.due_feed_deliveries(_utc_now_iso(), limit=200)
    deliveries: list[tuple[int, OutgoingPayload]] = []
    meta: list[tuple[dict[str, object], OutgoingPayload]] = []
    now_dt = datetime.utcnow()
    for row in rows:
        settings = db.get_chat_settings(int(row['chat_id']))
        allowed, reason = db.should_deliver_now(chat_id=int(row['chat_id']), tag=str(row.get('tag') or ''), now_utc=now_dt) if hasattr(db, 'should_deliver_now') else (True, '')
        if not allowed:
            next_retry_at = db.next_delivery_not_before(chat_id=int(row['chat_id']), now_utc=now_dt) if hasattr(db, 'next_delivery_not_before') else (datetime.utcnow() + timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
            db.mark_feed_delivery_retry(int(row['id']), error=reason, retry_count=int(row.get('retry_count') or 0), next_retry_at=next_retry_at)
            continue
        payload = OutgoingPayload(
            text=feed_text(cfg, text=str(row.get('text') or ''), tag=str(row.get('tag') or '')),
            media_kind=str(row.get('media_kind') or ''),
            media_ref=str(row.get('media_ref') or ''),
            message_thread_id=settings.default_thread_id if settings else None,
            disable_notification=bool(row.get('silent')) or (settings.disable_notifications if settings else False),
            reply_markup=build_inline_buttons(row.get('buttons') if isinstance(row.get('buttons'), list) else None),
            parse_mode=str(row.get('parse_mode') or '') or cfg.telegram_parse_mode,
            priority=int(row.get('priority') or 0),
            dry_run=_dry_run_enabled(db),
        )
        deliveries.append((int(row['chat_id']), payload))
        meta.append((row, payload))
    if not deliveries:
        return 0
    results = await send_payloads_bounded(
        application.bot,
        deliveries,
        parse_mode=cfg.telegram_parse_mode,
        max_concurrency=cfg.delivery_max_concurrency,
        max_per_minute=cfg.delivery_max_per_minute,
        paused_until_ts=_paused_until_ts(db),
        db=db,
    )
    delivered = 0
    for (row, payload), result in zip(meta, results):
        if result.ok:
            delivered += 1
            db.mark_feed_delivery_sent(int(row['id']))
        else:
            retry_count = int(row.get('retry_count') or 0) + 1
            if retry_count <= cfg.delivery_retry_attempts:
                next_retry_at = (datetime.utcnow() + timedelta(seconds=cfg.delivery_retry_backoff_seconds * retry_count)).strftime('%Y-%m-%d %H:%M:%S')
                db.mark_feed_delivery_retry(int(row['id']), error=result.error, retry_count=retry_count, next_retry_at=next_retry_at)
            else:
                db.mark_feed_delivery_dead(int(row['id']), error=result.error, retry_count=retry_count)
                db.enqueue_dead_letter(source_type='feed', source_id=str(row.get('event_id') or row.get('id')), chat_id=int(row['chat_id']), payload={'text': payload.text, 'media_kind': payload.media_kind, 'media_ref': payload.media_ref}, error=result.error, retry_count=retry_count)
            await _send_operator_alert(application, kind='feed_delivery', text=f"event={row.get('event_id')} chat={row.get('chat_id')} error={result.error}", severity='error')
    return delivered


async def _sync_feed_loop(application: Application, stop_event: asyncio.Event) -> None:
    cfg = application.bot_data['config']
    db = application.bot_data['db']
    status_client: ServerStatusClient = application.bot_data['status_client']
    if cfg.announcement_feed_interval_seconds <= 0 or not status_client.feed_is_configured():
        return
    while not stop_event.is_set():
        try:
            if db.acquire_leader_lock(name='feed_sync', owner=cfg.instance_id, ttl_seconds=cfg.leader_lock_ttl_seconds):
                items = await status_client.fetch_announcements()
                db.increment_runtime_counter('feed_sync_total', 1)
                for item in items:
                    replay_key = item.nonce or f"{item.event_id}:{item.timestamp or item.created_at}"
                    if hasattr(db, 'claim_replay_guard') and not db.claim_replay_guard('feed_item', replay_key, ttl_seconds=cfg.feed_nonce_ttl_seconds):
                        continue
                    db.mark_external_announcement_delivered(event_id=item.event_id, tag=item.tag, text=item.text, source_created_at=item.created_at)
                    targets = db.resolve_target_chats(allowed_chat_ids=cfg.telegram_allowed_chat_ids, fallback_chat_id=None, target_scope='all', target_tags=[item.tag] if item.tag else [], feature='announcements', target_shards=[getattr(item, 'shard', '')] if getattr(item, 'shard', '') else [])
                    if hasattr(db, 'enqueue_feed_deliveries_rich'):
                        db.enqueue_feed_deliveries_rich(event_id=item.event_id, tag=item.tag, text=item.text, source_created_at=item.created_at, chat_ids=targets, media_kind=item.media_kind, media_ref=item.media_ref, buttons=item.buttons, priority=item.priority, silent=item.silent, parse_mode=item.parse_mode)
                    else:
                        db.enqueue_feed_deliveries(event_id=item.event_id, tag=item.tag, text=item.text, source_created_at=item.created_at, chat_ids=targets)
                delivered = await _process_due_feed_deliveries(application)
                db.set_runtime_value('last_feed_sync_at', _utc_now_iso())
                db.set_runtime_value('last_feed_error', '')
                if delivered:
                    logger.info('announcement feed delivered=%s', delivered)
        except Exception as exc:
            db.set_runtime_value('last_feed_error', str(exc))
            logger.exception('announcement feed sync failed')
            await _send_operator_alert(application, kind='feed_loop', text=str(exc), severity='error')
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=cfg.announcement_feed_interval_seconds)
        except TimeoutError:
            continue


def build_application() -> tuple[Application, object]:
    config = load_config()
    configure_logging(config.log_level, config.log_file, log_format=config.log_format)
    db = create_database(config)
    status_client = ServerStatusClient(
        config.server_status_url,
        config.request_timeout_seconds,
        cache_ttl_seconds=config.status_cache_seconds,
        retry_attempts=config.status_retry_attempts,
        retry_backoff_seconds=config.status_retry_backoff_seconds,
        bearer_token=config.server_api_bearer_token,
        hmac_secret=config.server_api_hmac_secret,
        request_id_header=config.server_api_request_id_header,
        announcement_feed_url=config.announcement_feed_url,
        link_verify_url=config.link_verify_url,
        strict_schemas=config.strict_api_schemas,
        security_status_url=config.security_status_url,
        security_challenges_url=config.security_challenges_url,
        security_2fa_action_url=config.security_2fa_action_url,
        security_recovery_url=config.security_recovery_url,
        security_sessions_url=config.security_sessions_url,
        security_session_action_url=config.security_session_action_url,
        circuit_threshold=config.external_api_circuit_threshold,
        circuit_reset_seconds=config.external_api_circuit_reset_seconds,
    )
    build_manifest = _load_build_manifest(config)

    async def post_init(application: Application) -> None:
        public_commands = [
            BotCommand('start', 'стартовое сообщение'),
            BotCommand('help', 'список команд'),
            BotCommand('status', 'статус NeverMine'),
            BotCommand('online', 'онлайн игроков'),
            BotCommand('links', 'ссылки проекта'),
            BotCommand('stats', 'базовая статистика'),
            BotCommand('me', 'мой профиль и настройки'),
            BotCommand('sessions', 'мои security sessions'),
            BotCommand('2fa', 'статус 2FA/security'),
            BotCommand('notifications', 'пользовательские уведомления'),
            BotCommand('quiethours', 'тихие часы пользователя'),
            BotCommand('link', 'привязка Telegram ↔ NeverMine'),
            BotCommand('security', 'security status / sessions'),
        ]
        admin_commands = public_commands + [
            BotCommand('health', 'служебная проверка'),
            BotCommand('diag', 'runtime диагностика'),
            BotCommand('adminstats', 'расширенная статистика'),
            BotCommand('announce', 'анонс в текущий чат'),
            BotCommand('broadcast', 'рассылка с preview'),
            BotCommand('schedule', 'отложенная рассылка и DLQ'),
            BotCommand('chatsettings', 'настройки чатов'),
            BotCommand('pullannouncements', 'ручной sync внешнего feed'),
            BotCommand('maintenance', 'режим техработ'),
            BotCommand('template', 'preview/validate шаблонов'),
            BotCommand('rbac', 'RBAC overrides'),
            BotCommand('webhook', 'webhook sanity'),
            BotCommand('metrics', 'runtime metrics'),
            BotCommand('onboarding', 'onboarding новых чатов'),
            BotCommand('delivery', 'delivery pause/status/dry-run'),
            BotCommand('subscribe', 'подписка чата на теги'),
            BotCommand('unsubscribe', 'отписка чата от тегов'),
            BotCommand('alerts', 'operator alerts lifecycle'),
            BotCommand('approval', 'workflow чувствительных операций'),
            BotCommand('adminsite', 'внешняя operator-панель'),
            BotCommand('timezone', 'часовой пояс чата'),
            BotCommand('mode', 'подготовка следующего transport mode'),
            BotCommand('incident', 'incident snapshot'),
            BotCommand('adminhelp', 'справка по админ-командам'),
            BotCommand('opshelp', 'сводка по operational-командам'),
            BotCommand('admin', 'алиас adminhelp'),
            BotCommand('ops', 'алиас opshelp'),
            BotCommand('deliveryhelp', 'справка по delivery'),
            BotCommand('securityhelp', 'справка по security'),
        ]
        await application.bot.set_my_commands(public_commands, scope=BotCommandScopeDefault())
        for admin_chat_id in sorted(config.admin_like_ids | config.telegram_owner_ids):
            await application.bot.set_my_commands(admin_commands, scope=BotCommandScopeChat(chat_id=admin_chat_id))
        initial_status = await status_client.fetch_status(force=True)
        now = _utc_now_iso()
        db.set_runtime_value('last_status_refresh_at', now)
        compat = await status_client.check_compatibility()
        db.set_runtime_value('last_nm_auth_compat', json.dumps({'ok': compat.ok, 'api_version': compat.api_version, 'message': compat.message}, ensure_ascii=False))
        if not compat.ok:
            await _send_operator_alert(application, kind='nm_auth_compatibility', text=compat.message or 'compatibility mismatch', severity='error', payload={'incident_key': 'compat:nm_auth', 'api_version': compat.api_version})
            if getattr(config, 'strict_compatibility_gate', False):
                raise RuntimeError(f'NMAuth compatibility gate failed: {compat.message or compat.api_version or "unknown"}')
        if initial_status.ok:
            db.set_runtime_value('last_status_ok_at', now)
            if initial_status.latency_ms is not None:
                db.set_runtime_value('last_status_latency_ms', str(initial_status.latency_ms))
        stop_event = asyncio.Event()
        application.bot_data['stop_event'] = stop_event
        application.bot_data['status_refresh_task'] = asyncio.create_task(_run_status_refresh_loop(application, stop_event))
        application.bot_data['cleanup_task'] = asyncio.create_task(_run_cleanup_loop(application, stop_event))
        application.bot_data['scheduled_task'] = asyncio.create_task(_dispatch_scheduled_loop(application, stop_event))
        application.bot_data['feed_task'] = asyncio.create_task(_sync_feed_loop(application, stop_event))
        application.bot_data['security_task'] = asyncio.create_task(_poll_security_challenges(application, stop_event))
        application.bot_data['push_external_admin_snapshot'] = _push_external_admin_snapshot
        await _maybe_reconcile_webhook(application)
        health_server = await start_health_server(application, host=config.health_http_listen, port=config.health_http_port)
        application.bot_data['health_server'] = health_server
        if os.getenv('EXTERNAL_ADMIN_API_URL', '').strip():
            await _push_external_admin_snapshot(application, reason='startup')

    async def post_shutdown(application: Application) -> None:
        stop_event = application.bot_data.get('stop_event')
        if stop_event is not None:
            stop_event.set()
        for task_key in ('status_refresh_task', 'cleanup_task', 'scheduled_task', 'feed_task', 'security_task'):
            task = application.bot_data.get(task_key)
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        health_server = application.bot_data.get('health_server')
        if health_server is not None:
            health_server.close()
            await health_server.wait_closed()
        await status_client.close()

    application = ApplicationBuilder().token(config.telegram_bot_token).post_init(post_init).post_shutdown(post_shutdown).build()
    application.bot_data['config'] = config
    application.bot_data['db'] = db
    application.bot_data['status_client'] = status_client
    application.bot_data['rate_limiter'] = RateLimiter(db)
    limiter = application.bot_data['rate_limiter']
    db.set_runtime_value('storage_backend_mode', getattr(db, 'backend_name', 'sqlite'))
    db.set_runtime_value('rate_limit_backend_mode', getattr(limiter, 'backend_mode', 'sqlite'))
    if getattr(config, 'strict_compatibility_gate', False):
        redis_url = os.getenv('REDIS_URL', '').strip()
        redis_backend_mode = getattr(limiter, 'backend_mode', 'sqlite')
        if redis_url and redis_backend_mode != 'redis':
            redis_error = getattr(limiter, 'redis_last_error', '') or 'Redis backend unavailable'
            db.set_runtime_value('rate_limit_redis_fallback_reason', redis_error)
            db.set_runtime_value('rate_limit_redis_required', str(_redis_backend_required()).lower())
            if _redis_backend_required():
                raise RuntimeError('STRICT_COMPATIBILITY_GATE: REQUIRE_REDIS_BACKEND=true, но Redis backend недоступен')
            logger.warning(
                'REDIS_URL is configured, but Redis is unavailable; using local rate-limit fallback: %s',
                redis_error,
            )
        if os.getenv('EXTERNAL_ADMIN_API_URL', '').strip() and not _secret_from_env('EXTERNAL_ADMIN_API_TOKEN'):
            raise RuntimeError('STRICT_COMPATIBILITY_GATE: EXTERNAL_ADMIN_API_URL задан без EXTERNAL_ADMIN_API_TOKEN')
        if getattr(db, 'backend_name', 'sqlite') == 'postgresql' and not str(config.database_url).startswith(('postgres://', 'postgresql://')):
            raise RuntimeError('STRICT_COMPATIBILITY_GATE: backend postgresql, но DATABASE_URL некорректен')
    application.bot_data['started_at'] = time.time()
    application.bot_data['version'] = __version__
    application.bot_data['build_manifest'] = build_manifest

    application.add_handler(CommandHandler('start', start_handler))
    application.add_handler(CommandHandler('help', help_handler))
    application.add_handler(CommandHandler('status', status_handler))
    application.add_handler(CommandHandler('online', online_handler))
    application.add_handler(CommandHandler('links', links_handler))
    application.add_handler(CommandHandler('stats', stats_handler))
    application.add_handler(CommandHandler('me', me_handler))
    application.add_handler(CommandHandler('sessions', sessions_handler))
    application.add_handler(CommandHandler('2fa', twofa_handler))
    application.add_handler(CommandHandler('notifications', notifications_handler))
    application.add_handler(CommandHandler('quiethours', quiethours_handler))
    application.add_handler(CommandHandler('adminstats', adminstats_handler))
    application.add_handler(CommandHandler('adminhelp', admin_help_handler))
    application.add_handler(CommandHandler('admin', admin_help_handler))
    application.add_handler(CommandHandler('opshelp', ops_help_handler))
    application.add_handler(CommandHandler('ops', ops_help_handler))
    application.add_handler(CommandHandler('deliveryhelp', delivery_help_handler))
    application.add_handler(CommandHandler('securityhelp', security_help_handler))
    application.add_handler(CommandHandler('health', health_handler))
    application.add_handler(CommandHandler('diag', diag_handler))
    application.add_handler(CommandHandler('announce', announce_handler))
    application.add_handler(CommandHandler('broadcast', broadcast_handler))
    application.add_handler(CommandHandler('schedule', schedule_handler))
    application.add_handler(CommandHandler('chatsettings', chatsettings_handler))
    application.add_handler(CommandHandler('pullannouncements', pull_announcements_handler))
    application.add_handler(CommandHandler('link', link_handler))
    application.add_handler(CommandHandler('maintenance', maintenance_handler))
    application.add_handler(CommandHandler('template', template_handler))
    application.add_handler(CommandHandler('rbac', rbac_handler))
    application.add_handler(CommandHandler('webhook', webhook_handler))
    application.add_handler(CommandHandler('metrics', metrics_handler))
    application.add_handler(CommandHandler('onboarding', onboarding_handler))
    application.add_handler(CommandHandler('delivery', delivery_handler))
    application.add_handler(CommandHandler('subscribe', subscribe_handler))
    application.add_handler(CommandHandler('unsubscribe', unsubscribe_handler))
    application.add_handler(CommandHandler('alerts', alerts_handler))
    application.add_handler(CommandHandler('approval', approval_handler))
    application.add_handler(CommandHandler('adminsite', adminsite_handler))
    application.add_handler(CommandHandler('timezone', timezone_handler))
    application.add_handler(CommandHandler('mode', mode_handler))
    application.add_handler(CommandHandler('incident', incident_handler))
    application.add_handler(CommandHandler('security', security_handler))
    application.add_handler(CallbackQueryHandler(menu_callback_handler, pattern=r'^(menu:|broadcast:|security:|onboarding:).*'))

    application.add_error_handler(permission_error_handler)
    application.add_error_handler(rate_limit_error_handler)
    application.add_error_handler(generic_error_handler)

    logger.info('NMTelegramBot %s initialized in %s mode [instance=%s]', __version__, config.bot_mode, config.instance_id)
    return application, config


async def _run_safe_startup(application: Application) -> None:
    cfg = application.bot_data['config']
    db = application.bot_data['db']
    health_server = await start_health_server(application, host=cfg.health_http_listen, port=cfg.health_http_port)
    application.bot_data['health_server'] = health_server
    db.set_runtime_value('last_safe_startup_at', _utc_now_iso())
    logger.info('safe startup active; telegram transport disabled')
    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    finally:
        if health_server is not None:
            health_server.close()
            await health_server.wait_closed()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.check_config:
            load_config()
            warnings = os.environ.get('NMBOT_CONFIG_WARNINGS', '')
            print('[OK] Конфиг валиден')
            if warnings:
                print('[WARN]', warnings)
            return 0
        if args.prepare_runtime:
            prepare_runtime()
            print('[OK] Runtime prepared')
            return 0
        if args.readiness_check:
            readiness_check()
            print('[OK] Runtime readiness валиден')
            return 0
        application, config = build_application()
    except ConfigValidationError as exc:
        print(f'[CONFIG ERROR] {exc}', file=sys.stderr)
        return 1

    if args.safe_startup:
        asyncio.run(_run_safe_startup(application))
        return 0

    if config.bot_mode == 'webhook':
        url_path = f"{config.webhook_path_prefix}/{config.telegram_bot_token.split(':', 1)[0]}"
        webhook_url = config.webhook_url.rstrip('/') + '/' + url_path
        application.run_webhook(
            listen=config.webhook_listen,
            port=config.webhook_port,
            url_path=url_path,
            webhook_url=webhook_url,
            secret_token=config.webhook_secret_token or None,
            allowed_updates=None,
        )
    else:
        application.run_polling(allowed_updates=None)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
