#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from nmbot import __version__

ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"


def ask(prompt: str, current: str = "") -> str:
    suffix = f" [{current}]" if current else ""
    value = input(f"{prompt}{suffix}: ").strip()
    return value if value else current


def read_lines(path: Path) -> list[str]:
    return path.read_text(encoding='utf-8').splitlines()


def find_current(lines: list[str], key: str) -> str:
    prefix = f"{key}="
    for line in lines:
        if line.startswith(prefix):
            return line.split('=', 1)[1]
    return ''


def update_env_in_place(lines: list[str], updates: dict[str, str]) -> list[str]:
    remaining = dict(updates)
    out: list[str] = []
    for line in lines:
        if not line or line.lstrip().startswith('#') or '=' not in line:
            out.append(line)
            continue
        key, _ = line.split('=', 1)
        stripped = key.strip()
        if stripped in remaining:
            out.append(f"{stripped}={remaining.pop(stripped)}")
        else:
            out.append(line)
    if remaining:
        if out and out[-1].strip():
            out.append('')
        out.append('# Added by setup_wizard.py')
        for key, value in remaining.items():
            out.append(f"{key}={value}")
    return out


def main() -> int:
    print(f"NMTelegramBot setup wizard v{__version__}")
    print("=" * 34)
    if not ENV_PATH.exists():
        raise SystemExit("[!] Ожидается существующий .env в production archive")
    lines = read_lines(ENV_PATH)
    updates = {
        'TELEGRAM_BOT_TOKEN': ask('Telegram Bot Token', find_current(lines, 'TELEGRAM_BOT_TOKEN')),
        'TELEGRAM_BOT_USERNAME': ask('Bot username', find_current(lines, 'TELEGRAM_BOT_USERNAME')),
        'TELEGRAM_OWNER_IDS': ask('Owner ID через запятую', find_current(lines, 'TELEGRAM_OWNER_IDS')),
        'TELEGRAM_ALLOWED_CHAT_IDS': ask('Allowed chat ID через запятую', find_current(lines, 'TELEGRAM_ALLOWED_CHAT_IDS')),
        'TELEGRAM_CHAT_SCOPE': ask('Chat scope (all/private/groups)', find_current(lines, 'TELEGRAM_CHAT_SCOPE') or 'private'),
        'BOT_MODE': ask('Bot mode (polling/webhook)', find_current(lines, 'BOT_MODE') or 'webhook'),
        'PUBLIC_HTTP_SERVER_URL': ask('BotHost public HTTP server URL', find_current(lines, 'PUBLIC_HTTP_SERVER_URL') or find_current(lines, 'WEBHOOK_URL') or 'https://nmtelegrambot.bothost.ru/'),
        'WEBHOOK_URL': ask('Telegram webhook base URL', find_current(lines, 'WEBHOOK_URL') or find_current(lines, 'PUBLIC_HTTP_SERVER_URL') or 'https://nmtelegrambot.bothost.ru/'),
        'WEBHOOK_LISTEN': ask('Webhook listen host', find_current(lines, 'WEBHOOK_LISTEN') or '0.0.0.0'),
        'PORT': ask('BotHost internal web app port', find_current(lines, 'PORT') or find_current(lines, 'WEBHOOK_PORT') or '8080'),
        'WEBHOOK_PORT': ask('Webhook port', find_current(lines, 'WEBHOOK_PORT') or find_current(lines, 'PORT') or '8080'),
        'HEALTH_HTTP_PORT': ask('Health HTTP port (0 to disable; webhook uses PORT)', find_current(lines, 'HEALTH_HTTP_PORT') or '0'),
        'DATA_DIR': ask('BotHost persistent DATA_DIR', find_current(lines, 'DATA_DIR') or '/app/data'),
        'SHARED_DIR': ask('BotHost shared SHARED_DIR', find_current(lines, 'SHARED_DIR') or '/app/shared'),
        'NMBOT_RUNTIME_DIR': ask('Runtime dir fallback', find_current(lines, 'NMBOT_RUNTIME_DIR') or '/app/data'),
        'INSTANCE_ID': ask('Instance ID', find_current(lines, 'INSTANCE_ID') or 'nmtgbot-instance-1'),
        'SERVER_STATUS_URL': ask('HTTP URL статуса NeverMine/community-core', find_current(lines, 'SERVER_STATUS_URL')),
        'ANNOUNCEMENT_FEED_URL': ask('Announcement feed URL', find_current(lines, 'ANNOUNCEMENT_FEED_URL')),
        'LINK_VERIFY_URL': ask('Link verify URL', find_current(lines, 'LINK_VERIFY_URL')),
        'SERVER_API_BEARER_TOKEN': ask('Server API bearer token', find_current(lines, 'SERVER_API_BEARER_TOKEN')),
        'SERVER_API_HMAC_SECRET': ask('Server API HMAC secret', find_current(lines, 'SERVER_API_HMAC_SECRET')),
        'HEALTH_HTTP_TOKEN': ask('Health HTTP token', find_current(lines, 'HEALTH_HTTP_TOKEN')),
        'DISCORD_BRIDGE_URL': ask('Discord bridge URL (/internal/bridge/event)', find_current(lines, 'DISCORD_BRIDGE_URL')),
        'DISCORD_BRIDGE_HMAC_SECRET': ask('Discord bridge HMAC secret', find_current(lines, 'DISCORD_BRIDGE_HMAC_SECRET')),
        'DISCORD_BRIDGE_BEARER_TOKEN': ask('Discord bridge bearer token', find_current(lines, 'DISCORD_BRIDGE_BEARER_TOKEN')),
        'BRIDGE_INBOUND_HMAC_SECRET': ask('Inbound bridge HMAC secret from NMDiscordBot', find_current(lines, 'BRIDGE_INBOUND_HMAC_SECRET')),
        'BRIDGE_INBOUND_BEARER_TOKEN': ask('Inbound bridge bearer token from NMDiscordBot', find_current(lines, 'BRIDGE_INBOUND_BEARER_TOKEN')),
        'BRIDGE_TARGET_CHAT_IDS': ask('Bridge target Telegram chat IDs', find_current(lines, 'BRIDGE_TARGET_CHAT_IDS')),
        'BRIDGE_TARGET_SCOPE': ask('Bridge target scope (all/private/groups/current)', find_current(lines, 'BRIDGE_TARGET_SCOPE') or 'all'),
        'BRIDGE_TARGET_TAGS': ask('Bridge target tags', find_current(lines, 'BRIDGE_TARGET_TAGS')),
        'BRIDGE_ALLOWED_EVENT_TYPES': ask('Allowed inbound event types', find_current(lines, 'BRIDGE_ALLOWED_EVENT_TYPES')),
        'REMOTE_LOGS_ENABLED': ask('Remote logs enabled (true/false)', find_current(lines, 'REMOTE_LOGS_ENABLED') or 'false'),
        'REMOTE_LOGS_URL': ask('Remote logs ingest URL', find_current(lines, 'REMOTE_LOGS_URL') or 'https://logs.cajeer.ru/api/v1/ingest'),
        'REMOTE_LOGS_TOKEN': ask('Remote logs token', find_current(lines, 'REMOTE_LOGS_TOKEN')),
        'REMOTE_LOGS_PROJECT': ask('Remote logs project', find_current(lines, 'REMOTE_LOGS_PROJECT') or 'NeverMine'),
        'REMOTE_LOGS_BOT': ask('Remote logs bot name', find_current(lines, 'REMOTE_LOGS_BOT') or 'NMTelegramBot'),
        'REMOTE_LOGS_ENVIRONMENT': ask('Remote logs environment', find_current(lines, 'REMOTE_LOGS_ENVIRONMENT') or 'production'),
        'REMOTE_LOGS_LEVEL': ask('Remote logs level', find_current(lines, 'REMOTE_LOGS_LEVEL') or 'INFO'),
        'REMOTE_LOGS_BATCH_SIZE': ask('Remote logs batch size', find_current(lines, 'REMOTE_LOGS_BATCH_SIZE') or '25'),
        'REMOTE_LOGS_FLUSH_INTERVAL': ask('Remote logs flush interval seconds', find_current(lines, 'REMOTE_LOGS_FLUSH_INTERVAL') or '5'),
    }
    backup = ENV_PATH.with_name(f".env.bak.{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}")
    backup.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    ENV_PATH.write_text('\n'.join(update_env_in_place(lines, updates)) + '\n', encoding='utf-8')
    print(f"[✓] Сохранено: {ENV_PATH}")
    print(f"[✓] Backup: {backup}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
