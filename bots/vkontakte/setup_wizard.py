from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / '.env'


def ask(prompt: str, default: str = '') -> str:
    suffix = f' [{default}]' if default else ''
    value = input(f'{prompt}{suffix}: ').strip()
    return value or default


def main() -> None:
    print('NMVKBot production setup wizard')
    print('=' * 30)
    profile = ask('Профиль runtime (development/production/bothost)', 'bothost')
    bothost_mode = 'true' if profile == 'bothost' else ask('BOTHOST_MODE (true/false)', 'false')
    listen = ask('HTTP listen host', '0.0.0.0' if profile == 'bothost' else '127.0.0.1')
    port = ask('HTTP port', '8080' if profile == 'bothost' else '8100')
    token = ask('VK group token')
    group_id = ask('VK group id')
    admins = ask('VK admin user ids через запятую', '')
    moderators = ask('VK moderator user ids через запятую', '')
    inbound_secret = ask('Inbound HMAC secret', '')
    discord_bridge_url = ask('Discord ingress URL for VK -> Discord events', '')
    discord_bridge_secret = ask('Discord ingress HMAC secret', '')
    database_url = ask('DATABASE_URL (PostgreSQL URL, оставить пустым для SQLite)', '')
    sqlite_path = ask('SQLITE_PATH', 'data/nmvkbot.sqlite3')
    shared_dir = ask('SHARED_DIR', '/app/shared' if profile == 'bothost' else 'data/shared')
    content = f"""APP_PROFILE={profile}
BOTHOST_MODE={bothost_mode}
PORT={port}
VK_GROUP_TOKEN={token}
VK_GROUP_ID={group_id}
VK_WALL_POST_ENABLED=true
VK_API_VERSION=5.199
BOT_PREFIX=!
BOT_ADMINS={admins}
BOT_MODERATORS={moderators}
BOT_NAME=NMVKBot
LOG_LEVEL=INFO
LONGPOLL_WAIT=25
REQUEST_TIMEOUT=35
RECONNECT_DELAY_SECONDS=3
COMMAND_RATE_LIMIT_WINDOW_SECONDS=10
COMMAND_RATE_LIMIT_MAX_CALLS=8
SUPPORT_COOLDOWN_SECONDS=60
SUPPORT_MAX_LENGTH=1200
COMMAND_MODE=both
SUPPORT_COMMAND_MODE=both
ANNOUNCE_COMMAND_MODE=groups
IGNORE_PRIVATE_MESSAGES=false
IGNORE_GROUP_CHATS=false
ALLOWED_PEER_IDS=
DENIED_PEER_IDS=
BLOCKED_USER_IDS=
BLOCKED_PEER_IDS=
COMMAND_PERMISSIONS_JSON=
NEVERMINE_NAME=NeverMine
NEVERMINE_URL=https://nevermine.ru
NEVERMINE_TELEGRAM=https://t.me/nevermineru
NEVERMINE_DISCORD=https://discord.gg/2akQCk9kSP
NEVERMINE_VK=https://vk.com/nevermineru
HEALTH_HTTP_LISTEN={listen}
HEALTH_HTTP_PORT={port}
HEALTH_HTTP_TOKEN=
HEALTH_HTTP_MINIMAL=false
HEALTH_HTTP_PUBLIC=false
BRIDGE_INBOUND_HMAC_SECRET={inbound_secret}
BRIDGE_INBOUND_BEARER_TOKEN=
BRIDGE_INGRESS_STRICT_AUTH=true
BRIDGE_TARGET_PEER_IDS=
BRIDGE_TARGET_SCOPE=all
BRIDGE_TARGET_TAGS=news,events,devlogs
BRIDGE_ALLOWED_EVENT_TYPES=community.announcement.created,community.devlog.created,community.event.created,community.world_signal.created,community.support.reply
DISCORD_BRIDGE_URL={discord_bridge_url}
DISCORD_BRIDGE_HMAC_SECRET={discord_bridge_secret}
DISCORD_BRIDGE_BEARER_TOKEN=
OUTBOUND_KEY_ID=v1
BRIDGE_TIMEOUT_SECONDS=5
OUTBOUND_WORKER_INTERVAL_SECONDS=5
OUTBOUND_RETRY_BASE_SECONDS=10
OUTBOUND_RETRY_MAX_SECONDS=300
OUTBOUND_MAX_ATTEMPTS=8
REPLAY_CACHE_TTL_SECONDS=600
EVENT_MAX_FUTURE_SKEW_SECONDS=300
DATABASE_URL={database_url}
SQLITE_PATH={sqlite_path}
DB_SCHEMA_PREFIX=nmvkbot
SHARED_DIR={shared_dir}
PROCESSED_EVENTS_RETENTION_DAYS=14
OUTBOUND_SENT_RETENTION_DAYS=14
OUTBOUND_DEAD_RETENTION_DAYS=30
CLOSED_TICKET_RETENTION_DAYS=90
SHARED_FILE_RETENTION_DAYS=30
ATTACHMENT_MAX_ITEMS=8
REMOTE_LOGS_ENABLED=false
REMOTE_LOGS_URL=https://logs.cajeer.ru/api/v1/ingest
REMOTE_LOGS_TOKEN=
REMOTE_LOGS_PROJECT=NeverMine
REMOTE_LOGS_BOT=NMVKBot
REMOTE_LOGS_ENVIRONMENT=production
REMOTE_LOGS_LEVEL=INFO
REMOTE_LOGS_BATCH_SIZE=25
REMOTE_LOGS_FLUSH_INTERVAL=5
REMOTE_LOGS_TIMEOUT=3
REMOTE_LOGS_SIGN_REQUESTS=false
REMOTE_LOGS_SPOOL_DIR=
REMOTE_LOGS_MAX_SPOOL_FILES=200
"""
    ENV_PATH.write_text(content, encoding='utf-8')
    print(f'[✓] Сохранено: {ENV_PATH}')


if __name__ == '__main__':
    main()
