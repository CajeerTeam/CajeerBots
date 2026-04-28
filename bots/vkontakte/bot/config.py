from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import FrozenSet

from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    value = os.getenv(name, '').strip()
    if not value:
        raise RuntimeError(f'Environment variable {name} is required')
    return value


def _parse_int_set(raw: str) -> FrozenSet[int]:
    result: set[int] = set()
    for chunk in raw.split(','):
        chunk = chunk.strip()
        if chunk:
            result.add(int(chunk))
    return frozenset(result)


def _parse_str_set(raw: str) -> FrozenSet[str]:
    result: set[str] = set()
    for chunk in raw.split(','):
        chunk = chunk.strip()
        if chunk:
            result.add(chunk.lower())
    return frozenset(result)


def _parse_bool(raw: str | None, *, default: bool = False) -> bool:
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {'1', 'true', 'yes', 'y', 'on'}


def _get_int(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = os.getenv(name, '').strip()
    value = int(raw) if raw else default
    if minimum is not None and value < minimum:
        raise RuntimeError(f'{name} must be >= {minimum}')
    return value


def _parse_command_permissions(raw: str) -> dict[str, FrozenSet[str]]:
    if not raw.strip():
        return {}
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise RuntimeError('COMMAND_PERMISSIONS_JSON must be a JSON object')
    result: dict[str, FrozenSet[str]] = {}
    for command, roles in payload.items():
        name = str(command).strip().lower()
        if not name:
            continue
        if isinstance(roles, str):
            values = {roles.strip().lower()} if roles.strip() else set()
        elif isinstance(roles, list):
            values = {str(item).strip().lower() for item in roles if str(item).strip()}
        else:
            raise RuntimeError('COMMAND_PERMISSIONS_JSON values must be string or array')
        result[name] = frozenset(values)
    return result


def _detect_profile() -> tuple[str, bool]:
    profile = (os.getenv('APP_PROFILE', 'production').strip() or 'production').lower()
    bothost_mode = _parse_bool(os.getenv('BOTHOST_MODE'), default=(profile == 'bothost'))
    if bothost_mode:
        profile = 'bothost'
    return profile, bothost_mode


def _build_database_url() -> str:
    direct = os.getenv('DATABASE_URL', '').strip()
    if direct:
        return direct
    host = os.getenv('PGHOST', '').strip()
    database = os.getenv('PGDATABASE', '').strip()
    user = os.getenv('PGUSER', '').strip()
    password = os.getenv('PGPASSWORD', '').strip()
    port = os.getenv('PGPORT', '5432').strip() or '5432'
    if host and database and user:
        auth = user
        if password:
            auth = f'{user}:{password}'
        return f'postgresql://{auth}@{host}:{port}/{database}'
    return ''


def _resolve_http_port(profile: str) -> int:
    if profile == 'bothost':
        port_raw = os.getenv('PORT', '').strip() or os.getenv('HEALTH_HTTP_PORT', '').strip()
        return int(port_raw) if port_raw else 8080
    return _get_int('HEALTH_HTTP_PORT', 0, minimum=0)


def _resolve_http_host(profile: str) -> str:
    default_host = '0.0.0.0' if profile == 'bothost' else '127.0.0.1'
    return os.getenv('HEALTH_HTTP_LISTEN', default_host).strip() or default_host


def _resolve_shared_dir(profile: str) -> str:
    value = os.getenv('SHARED_DIR', '').strip()
    if value:
        base = Path(value)
    elif profile == 'bothost':
        base = Path('/app/shared')
    else:
        base = Path('data') / 'shared'
    base.mkdir(parents=True, exist_ok=True)
    for extra in (
        base / 'attachments',
        base / 'exports',
        base / 'bridge',
        base / 'tmp',
        base / 'dead-letter' / 'outbound',
        base / 'remote-logs',
    ):
        extra.mkdir(parents=True, exist_ok=True)
    return str(base)


_PROFILE, _BOTHOST_MODE = _detect_profile()
_DATABASE_URL = _build_database_url()
_DATABASE_BACKEND = 'postgres' if _DATABASE_URL.startswith(('postgres://', 'postgresql://')) else 'sqlite'
_SHARED_DIR = _resolve_shared_dir(_PROFILE)


@dataclass(frozen=True)
class Settings:
    app_profile: str
    bothost_mode: bool
    entrypoint: str
    vk_group_token: str
    vk_group_id: int
    vk_wall_post_enabled: bool
    vk_api_version: str
    bot_prefix: str
    bot_name: str
    bot_admins: FrozenSet[int]
    bot_moderators: FrozenSet[int]
    allowed_peer_ids: FrozenSet[int]
    denied_peer_ids: FrozenSet[int]
    blocked_user_ids: FrozenSet[int]
    blocked_peer_ids: FrozenSet[int]
    command_permissions: dict[str, FrozenSet[str]]
    command_mode: str
    support_command_mode: str
    announce_command_mode: str
    ignore_private_messages: bool
    ignore_group_chats: bool
    log_level: str
    longpoll_wait: int
    request_timeout: int
    reconnect_delay_seconds: int
    command_rate_limit_window_seconds: int
    command_rate_limit_max_calls: int
    support_cooldown_seconds: int
    support_max_length: int
    nevermine_name: str
    nevermine_url: str
    nevermine_telegram: str
    nevermine_discord: str
    nevermine_vk: str
    health_http_listen: str
    health_http_port: int
    health_http_token: str
    health_http_minimal: bool
    health_http_public: bool
    bridge_inbound_hmac_secret: str
    bridge_inbound_bearer_token: str
    bridge_ingress_strict_auth: bool
    bridge_target_peer_ids: FrozenSet[int]
    bridge_target_scope: str
    bridge_target_tags: FrozenSet[str]
    bridge_allowed_event_types: FrozenSet[str]
    discord_bridge_url: str
    discord_bridge_hmac_secret: str
    discord_bridge_bearer_token: str
    outbound_key_id: str
    bridge_timeout_seconds: int
    outbound_worker_interval_seconds: int
    outbound_retry_base_seconds: int
    outbound_retry_max_seconds: int
    outbound_max_attempts: int
    replay_cache_ttl_seconds: int
    event_max_future_skew_seconds: int
    database_url: str
    db_schema_prefix: str
    database_backend: str
    sqlite_path: str
    shared_dir: str
    remote_logs_enabled: bool
    remote_logs_url: str
    remote_logs_token: str
    remote_logs_project: str
    remote_logs_bot: str
    remote_logs_environment: str
    remote_logs_level: str
    remote_logs_batch_size: int
    remote_logs_flush_interval: float
    remote_logs_timeout: float
    remote_logs_sign_requests: bool
    remote_logs_spool_dir: str
    remote_logs_max_spool_files: int
    processed_events_retention_days: int
    outbound_sent_retention_days: int
    outbound_dead_retention_days: int
    closed_ticket_retention_days: int
    shared_file_retention_days: int
    attachment_max_items: int

    def shared_path(self, *parts: str) -> str:
        path = Path(self.shared_dir or str(Path('data') / 'shared'), *parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        return str(path)


settings = Settings(
    app_profile=_PROFILE,
    bothost_mode=_BOTHOST_MODE,
    entrypoint='main.py',
    vk_group_token=_require('VK_GROUP_TOKEN'),
    vk_group_id=int(_require('VK_GROUP_ID')),
    vk_wall_post_enabled=_parse_bool(os.getenv('VK_WALL_POST_ENABLED'), default=False),
    vk_api_version=os.getenv('VK_API_VERSION', '5.199').strip() or '5.199',
    bot_prefix=os.getenv('BOT_PREFIX', '!').strip() or '!',
    bot_name=os.getenv('BOT_NAME', 'NMVKBot').strip() or 'NMVKBot',
    bot_admins=_parse_int_set(os.getenv('BOT_ADMINS', '')),
    bot_moderators=_parse_int_set(os.getenv('BOT_MODERATORS', '')),
    allowed_peer_ids=_parse_int_set(os.getenv('ALLOWED_PEER_IDS', '')),
    denied_peer_ids=_parse_int_set(os.getenv('DENIED_PEER_IDS', '')),
    blocked_user_ids=_parse_int_set(os.getenv('BLOCKED_USER_IDS', '')),
    blocked_peer_ids=_parse_int_set(os.getenv('BLOCKED_PEER_IDS', '')),
    command_permissions=_parse_command_permissions(os.getenv('COMMAND_PERMISSIONS_JSON', '')),
    command_mode=(os.getenv('COMMAND_MODE', 'both').strip() or 'both').lower(),
    support_command_mode=(os.getenv('SUPPORT_COMMAND_MODE', os.getenv('COMMAND_MODE', 'both')).strip() or 'both').lower(),
    announce_command_mode=(os.getenv('ANNOUNCE_COMMAND_MODE', os.getenv('COMMAND_MODE', 'groups')).strip() or 'groups').lower(),
    ignore_private_messages=_parse_bool(os.getenv('IGNORE_PRIVATE_MESSAGES'), default=False),
    ignore_group_chats=_parse_bool(os.getenv('IGNORE_GROUP_CHATS'), default=False),
    log_level=os.getenv('LOG_LEVEL', 'INFO').strip().upper() or 'INFO',
    longpoll_wait=_get_int('LONGPOLL_WAIT', 25, minimum=1),
    request_timeout=_get_int('REQUEST_TIMEOUT', 35, minimum=1),
    reconnect_delay_seconds=_get_int('RECONNECT_DELAY_SECONDS', 3, minimum=0),
    command_rate_limit_window_seconds=_get_int('COMMAND_RATE_LIMIT_WINDOW_SECONDS', 10, minimum=1),
    command_rate_limit_max_calls=_get_int('COMMAND_RATE_LIMIT_MAX_CALLS', 8, minimum=1),
    support_cooldown_seconds=_get_int('SUPPORT_COOLDOWN_SECONDS', 60, minimum=0),
    support_max_length=_get_int('SUPPORT_MAX_LENGTH', 1200, minimum=50),
    nevermine_name=os.getenv('NEVERMINE_NAME', 'NeverMine').strip() or 'NeverMine',
    nevermine_url=os.getenv('NEVERMINE_URL', 'https://nevermine.ru').strip() or 'https://nevermine.ru',
    nevermine_telegram=os.getenv('NEVERMINE_TELEGRAM', 'https://t.me/nevermineru').strip() or 'https://t.me/nevermineru',
    nevermine_discord=os.getenv('NEVERMINE_DISCORD', 'https://discord.gg/2akQCk9kSP').strip() or 'https://discord.gg/2akQCk9kSP',
    nevermine_vk=os.getenv('NEVERMINE_VK', 'https://vk.com/nevermineru').strip() or 'https://vk.com/nevermineru',
    health_http_listen=_resolve_http_host(_PROFILE),
    health_http_port=_resolve_http_port(_PROFILE),
    health_http_token=os.getenv('HEALTH_HTTP_TOKEN', '').strip(),
    health_http_minimal=_parse_bool(os.getenv('HEALTH_HTTP_MINIMAL'), default=False),
    health_http_public=_parse_bool(os.getenv('HEALTH_HTTP_PUBLIC'), default=False),
    bridge_inbound_hmac_secret=os.getenv('BRIDGE_INBOUND_HMAC_SECRET', '').strip(),
    bridge_inbound_bearer_token=os.getenv('BRIDGE_INBOUND_BEARER_TOKEN', '').strip(),
    bridge_ingress_strict_auth=_parse_bool(os.getenv('BRIDGE_INGRESS_STRICT_AUTH'), default=True),
    bridge_target_peer_ids=_parse_int_set(os.getenv('BRIDGE_TARGET_PEER_IDS', '')),
    bridge_target_scope=(os.getenv('BRIDGE_TARGET_SCOPE', 'all').strip() or 'all').lower(),
    bridge_target_tags=_parse_str_set(os.getenv('BRIDGE_TARGET_TAGS', '')),
    bridge_allowed_event_types=_parse_str_set(os.getenv('BRIDGE_ALLOWED_EVENT_TYPES', '')),
    discord_bridge_url=os.getenv('DISCORD_BRIDGE_URL', '').strip(),
    discord_bridge_hmac_secret=os.getenv('DISCORD_BRIDGE_HMAC_SECRET', '').strip(),
    discord_bridge_bearer_token=os.getenv('DISCORD_BRIDGE_BEARER_TOKEN', '').strip(),
    outbound_key_id=os.getenv('OUTBOUND_KEY_ID', 'v1').strip() or 'v1',
    bridge_timeout_seconds=_get_int('BRIDGE_TIMEOUT_SECONDS', 5, minimum=1),
    outbound_worker_interval_seconds=_get_int('OUTBOUND_WORKER_INTERVAL_SECONDS', 5, minimum=1),
    outbound_retry_base_seconds=_get_int('OUTBOUND_RETRY_BASE_SECONDS', 10, minimum=1),
    outbound_retry_max_seconds=_get_int('OUTBOUND_RETRY_MAX_SECONDS', 300, minimum=5),
    outbound_max_attempts=_get_int('OUTBOUND_MAX_ATTEMPTS', 8, minimum=1),
    replay_cache_ttl_seconds=_get_int('REPLAY_CACHE_TTL_SECONDS', 600, minimum=30),
    event_max_future_skew_seconds=_get_int('EVENT_MAX_FUTURE_SKEW_SECONDS', 300, minimum=30),
    database_url=_DATABASE_URL,
    db_schema_prefix=(os.getenv('DB_SCHEMA_PREFIX', 'nmvkbot').strip() or 'nmvkbot').lower(),
    database_backend=_DATABASE_BACKEND,
    sqlite_path=os.getenv('SQLITE_PATH', 'data/nmvkbot.sqlite3').strip() or 'data/nmvkbot.sqlite3',
    shared_dir=_SHARED_DIR,
    remote_logs_enabled=_parse_bool(os.getenv('REMOTE_LOGS_ENABLED'), default=False),
    remote_logs_url=os.getenv('REMOTE_LOGS_URL', '').strip(),
    remote_logs_token=os.getenv('REMOTE_LOGS_TOKEN', '').strip(),
    remote_logs_project=os.getenv('REMOTE_LOGS_PROJECT', 'NeverMine').strip() or 'NeverMine',
    remote_logs_bot=os.getenv('REMOTE_LOGS_BOT', os.getenv('BOT_NAME', 'NMVKBot').strip() or 'NMVKBot').strip() or 'NMVKBot',
    remote_logs_environment=os.getenv('REMOTE_LOGS_ENVIRONMENT', 'production').strip() or 'production',
    remote_logs_level=os.getenv('REMOTE_LOGS_LEVEL', 'INFO').strip().upper() or 'INFO',
    remote_logs_batch_size=_get_int('REMOTE_LOGS_BATCH_SIZE', 25, minimum=1),
    remote_logs_flush_interval=float(os.getenv('REMOTE_LOGS_FLUSH_INTERVAL', '5') or '5'),
    remote_logs_timeout=float(os.getenv('REMOTE_LOGS_TIMEOUT', '3') or '3'),
    remote_logs_sign_requests=_parse_bool(os.getenv('REMOTE_LOGS_SIGN_REQUESTS'), default=False),
    remote_logs_spool_dir=os.getenv('REMOTE_LOGS_SPOOL_DIR', '').strip() or str(Path(_SHARED_DIR) / 'remote-logs'),
    remote_logs_max_spool_files=_get_int('REMOTE_LOGS_MAX_SPOOL_FILES', 200, minimum=1),
    processed_events_retention_days=_get_int('PROCESSED_EVENTS_RETENTION_DAYS', 14, minimum=1),
    outbound_sent_retention_days=_get_int('OUTBOUND_SENT_RETENTION_DAYS', 14, minimum=1),
    outbound_dead_retention_days=_get_int('OUTBOUND_DEAD_RETENTION_DAYS', 30, minimum=1),
    closed_ticket_retention_days=_get_int('CLOSED_TICKET_RETENTION_DAYS', 90, minimum=1),
    shared_file_retention_days=_get_int('SHARED_FILE_RETENTION_DAYS', 30, minimum=1),
    attachment_max_items=_get_int('ATTACHMENT_MAX_ITEMS', 8, minimum=0),
)

_ALLOWED_MODES = {'both', 'private', 'groups', 'none'}
for mode_name, mode_value in {
    'COMMAND_MODE': settings.command_mode,
    'SUPPORT_COMMAND_MODE': settings.support_command_mode,
    'ANNOUNCE_COMMAND_MODE': settings.announce_command_mode,
}.items():
    if mode_value not in _ALLOWED_MODES:
        raise RuntimeError(f'{mode_name} must be one of: {", ".join(sorted(_ALLOWED_MODES))}')

if settings.bridge_target_scope not in {'all', 'private', 'groups', 'current'}:
    raise RuntimeError('BRIDGE_TARGET_SCOPE must be one of: all, private, groups, current')

if settings.health_http_port > 0 and settings.bridge_ingress_strict_auth and not (settings.bridge_inbound_hmac_secret or settings.bridge_inbound_bearer_token):
    raise RuntimeError(
        'HEALTH_HTTP_PORT is enabled and BRIDGE_INGRESS_STRICT_AUTH=true, '
        'but neither BRIDGE_INBOUND_HMAC_SECRET nor BRIDGE_INBOUND_BEARER_TOKEN is configured'
    )
