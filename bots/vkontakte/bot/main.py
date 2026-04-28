from __future__ import annotations

import logging
import os
import time
from typing import Any

from nmbot.bridge import ReplayGuard
from nmbot.config import settings
from nmbot.handlers import CommandHandler, MessageContext
from nmbot.health_http import VKBridgeServer
from nmbot.logger import configure_logging, get_remote_logs_diagnostics
from nmbot.outbound_queue import OutboundDeliveryService
from nmbot.storage import Storage
from nmbot.vk_api import LongPollServer, VKAPIError, VKClient

logger = logging.getLogger(__name__)


def _extract_vk_attachment_ids(message: dict[str, Any], limit: int) -> list[str]:
    result: list[str] = []
    for item in message.get('attachments', []) or []:
        if not isinstance(item, dict):
            continue
        kind = str(item.get('type') or '').strip()
        payload = item.get(kind)
        if not isinstance(payload, dict):
            continue
        owner_id = payload.get('owner_id')
        media_id = payload.get('id')
        access_key = payload.get('access_key')
        if owner_id is None or media_id is None:
            continue
        token = f'{kind}{owner_id}_{media_id}'
        if access_key:
            token = f'{token}_{access_key}'
        result.append(token)
        if len(result) >= limit:
            break
    return result


def extract_message_context(update: dict[str, Any]) -> MessageContext | None:
    if update.get('type') != 'message_new':
        return None
    message = update.get('object', {}).get('message', {})
    if not message or message.get('out') == 1:
        return None
    peer_id = int(message.get('peer_id', 0))
    user_id = int(message.get('from_id', 0))
    text = str(message.get('text', ''))
    chat_id: int | None = None
    if peer_id >= 2_000_000_000:
        chat_id = peer_id - 2_000_000_000
    source_message_id = message.get('conversation_message_id') or message.get('id')
    try:
        source_message_id_int = int(source_message_id) if source_message_id is not None else None
    except (TypeError, ValueError):
        source_message_id_int = None
    attachments = _extract_vk_attachment_ids(message, settings.attachment_max_items)
    return MessageContext(user_id=user_id, peer_id=peer_id, chat_id=chat_id, text=text, source_message_id=source_message_id_int, attachments=attachments)


def process_updates(vk: VKClient, handler: CommandHandler, updates: list[dict[str, Any]], runtime_status: dict[str, Any], replay_guard: ReplayGuard) -> None:
    runtime_status['replay_cache_size'] = replay_guard.size()
    for update in updates:
        ctx = extract_message_context(update)
        if ctx is None:
            continue
        try:
            handled = handler.handle(ctx)
            logger.debug('Update handled=%s peer_id=%s text=%r attachments=%s', handled, ctx.peer_id, ctx.text, len(ctx.attachments))
        except Exception:
            logger.exception('Handler failure for peer_id=%s', ctx.peer_id)
            try:
                vk.send_message(ctx.peer_id, 'Внутренняя ошибка обработки команды.')
            except Exception:
                logger.exception('Failed to send error message to peer_id=%s', ctx.peer_id)


def update_longpoll_state(lp: LongPollServer, payload: dict[str, Any], vk: VKClient) -> LongPollServer:
    failed = payload.get('failed')
    if failed is None or failed == 1:
        return LongPollServer(server=lp.server, key=lp.key, ts=str(payload['ts']))
    return vk.get_longpoll_server(settings.vk_group_id)


def perform_startup_checks(vk: VKClient, storage: Storage) -> dict[str, Any]:
    checks: dict[str, Any] = {
        'profile': settings.app_profile,
        'entrypoint': settings.entrypoint,
        'vk_group_lookup': False,
        'vk_longpoll_lookup': False,
        'bridge_http_enabled': settings.health_http_port > 0,
        'discord_outbound_enabled': bool(settings.discord_bridge_url),
        'database_backend': settings.database_backend,
        'database_ready': False,
        'shared_dir': settings.shared_dir,
        'remote_logs_enabled': bool(settings.remote_logs_enabled),
        'remote_logs_configured': bool(settings.remote_logs_url and settings.remote_logs_token),
        'warnings': [],
    }
    group = vk.get_group_info(settings.vk_group_id)
    checks['vk_group_lookup'] = True
    checks['vk_group_name'] = group.get('name', '')
    vk.get_longpoll_server(settings.vk_group_id)
    checks['vk_longpoll_lookup'] = True
    checks['database_ready'] = storage.ping()
    checks['database_diagnostics'] = storage.database_diagnostics()
    if settings.app_profile == 'bothost' and settings.health_http_listen != '0.0.0.0':
        checks['warnings'].append('bothost profile should bind to 0.0.0.0')
    if settings.discord_bridge_url.startswith('http://127.0.0.1') or settings.discord_bridge_url.startswith('http://localhost'):
        checks['warnings'].append('DISCORD_BRIDGE_URL points to localhost; this is unsafe for multi-host production')
    if not os.path.exists('.env.example'):
        checks['warnings'].append('.env.example missing in runtime directory')
    checks['ok'] = bool(checks['vk_group_lookup'] and checks['vk_longpoll_lookup'] and checks['database_ready'])
    return checks


def _cleanup_shared_files(days: int) -> dict[str, int]:
    cutoff = time.time() - days * 86400
    removed = 0
    for base in ('dead-letter', 'remote-logs', 'tmp', 'exports'):
        path = settings.shared_path(base)
        for root, _dirs, files in os.walk(path):
            for name in files:
                full = os.path.join(root, name)
                try:
                    if os.path.getmtime(full) <= cutoff:
                        os.remove(full)
                        removed += 1
                except FileNotFoundError:
                    continue
                except Exception:
                    logger.exception('Failed to cleanup shared file %s', full)
    return {'shared_files_removed': removed}


def main() -> None:
    configure_logging(settings)
    logger.info('Starting %s', settings.bot_name)

    storage = Storage(database_url=settings.database_url, sqlite_path=settings.sqlite_path, schema_prefix=settings.db_schema_prefix)
    storage.initialize()
    cleanup_stats = storage.cleanup_old_records(
        processed_events_retention_days=settings.processed_events_retention_days,
        outbound_sent_retention_days=settings.outbound_sent_retention_days,
        outbound_dead_retention_days=settings.outbound_dead_retention_days,
        closed_ticket_retention_days=settings.closed_ticket_retention_days,
    )
    cleanup_stats.update(_cleanup_shared_files(settings.shared_file_retention_days))
    replay_guard = ReplayGuard(ttl_seconds=settings.replay_cache_ttl_seconds)
    runtime_status: dict[str, Any] = {'startup_checks': {}, 'database_backend': storage.backend, 'cleanup': cleanup_stats}

    vk = VKClient(token=settings.vk_group_token, api_version=settings.vk_api_version, timeout=settings.request_timeout)
    outbound = OutboundDeliveryService(settings=settings, storage=storage)
    bridge_server = VKBridgeServer(settings=settings, vk=vk, storage=storage, runtime_status=runtime_status, replay_guard=replay_guard)
    handler = CommandHandler(settings=settings, vk=vk, storage=storage, outbound=outbound, runtime_status=runtime_status)

    try:
        runtime_status['startup_checks'] = perform_startup_checks(vk, storage)
        runtime_status['remote_logs'] = get_remote_logs_diagnostics()
        logger.info('Startup checks: %s', runtime_status['startup_checks'])
        logger.info('Cleanup: %s', cleanup_stats)
        logger.info('Remote logs: %s', runtime_status['remote_logs'])
        outbound.start()
        bridge_server.start()
        longpoll = vk.get_longpoll_server(settings.vk_group_id)
        logger.info('Long Poll connected')

        while True:
            try:
                payload = vk.poll(longpoll, wait=settings.longpoll_wait)
                if 'updates' in payload:
                    process_updates(vk, handler, payload['updates'], runtime_status, replay_guard)
                runtime_status['last_poll_ts'] = str(payload.get('ts', ''))
                longpoll = update_longpoll_state(longpoll, payload, vk)
            except KeyboardInterrupt:
                raise
            except Exception:
                logger.exception('Polling failure, reconnecting')
                time.sleep(settings.reconnect_delay_seconds)
                longpoll = vk.get_longpoll_server(settings.vk_group_id)
    except KeyboardInterrupt:
        logger.info('Shutdown requested')
    except VKAPIError:
        logger.exception('VK API error during startup')
        raise
    finally:
        bridge_server.stop()
        outbound.stop()
        vk.close()
        storage.close()
        logger.info('Stopped %s', settings.bot_name)


if __name__ == '__main__':
    main()
