#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import stat
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ENV_FILE = ROOT / ".env"

from nmbot.config_schema import DEFAULTS, PRODUCTION_REQUIRED_KEYS, SECRET_KEYS

PROFILE_KEYS: dict[str, tuple[str, ...]] = {
    "minimal": (
        "DISCORD_TOKEN", "DISCORD_GUILD_ID", "NEVERMINE_SERVER_NAME", "NEVERMINE_SERVER_ADDRESS",
        "STORAGE_BACKEND", "SQLITE_PATH", "DATA_DIR", "SHARED_DIR", "LOG_DIR", "BACKUP_DIR", "LOG_LEVEL", "COMMAND_SURFACE_MODE",
    ),
    "production": tuple(DEFAULTS.keys()),
    "bridge": (
        "INGRESS_ENABLED", "APP_PUBLIC_URL", "INGRESS_HOST", "INGRESS_PORT", "PORT", "INGRESS_STRICT_AUTH", "INGRESS_BEARER_TOKEN",
        "INGRESS_HMAC_SECRET", "INGRESS_PREVIOUS_HMAC_SECRET", "COMMUNITY_CORE_EVENT_URL", "TELEGRAM_BRIDGE_URL",
        "VK_BRIDGE_URL", "WORKSPACE_BRIDGE_URL", "OUTBOUND_HMAC_SECRET", "OUTBOUND_BEARER_TOKEN", "OUTBOUND_KEY_ID",
        "BRIDGE_EVENT_RULES_JSON", "BRIDGE_PAYLOAD_ALLOWLIST_JSON", "BRIDGE_MAX_ATTEMPTS",
        "BRIDGE_RETRY_BACKOFF_BASE_SECONDS", "BRIDGE_RETRY_BACKOFF_MAX_SECONDS",
    ),
    "integrations": (
        "COMMUNITY_CORE_EVENT_URL", "TELEGRAM_BRIDGE_URL", "VK_BRIDGE_URL", "WORKSPACE_BRIDGE_URL",
        "OUTBOUND_HMAC_SECRET", "OUTBOUND_BEARER_TOKEN", "OUTBOUND_KEY_ID",
        "BRIDGE_EVENT_RULES_JSON", "BRIDGE_PAYLOAD_ALLOWLIST_JSON",
        "BRIDGE_SYNC_ANNOUNCEMENTS", "BRIDGE_SYNC_EVENTS", "BRIDGE_SYNC_SUPPORT", "BRIDGE_SYNC_REPORTS",
        "BRIDGE_SYNC_GUILD_RECRUITMENT", "BRIDGE_SYNC_IDENTITY",
        "INGRESS_ENABLED", "APP_PUBLIC_URL", "INGRESS_HOST", "INGRESS_PORT", "PORT", "INGRESS_STRICT_AUTH",
        "INGRESS_HMAC_SECRET", "INGRESS_BEARER_TOKEN", "INGRESS_PREVIOUS_HMAC_SECRET",
        "METRICS_ENABLED", "METRICS_REQUIRE_AUTH", "METRICS_BEARER_TOKEN",
    ),
    "discord-layout": (
        "DISCORD_GUILD_ID", "DISCORD_START_HERE_CHANNEL_ID", "DISCORD_RULES_CHANNEL_ID",
        "DISCORD_ROLES_AND_ACCESS_CHANNEL_ID", "DISCORD_FAQ_CHANNEL_ID", "DISCORD_ANNOUNCEMENTS_CHANNEL_ID",
        "DISCORD_DEVLOG_CHANNEL_ID", "DISCORD_WORLD_SIGNALS_CHANNEL_ID", "DISCORD_STAGE_CHANNEL_ID",
        "DISCORD_FORUM_SUGGESTIONS_CHANNEL_ID", "DISCORD_FORUM_BUG_REPORTS_CHANNEL_ID",
        "DISCORD_FORUM_GUILD_RECRUITMENT_CHANNEL_ID", "DISCORD_FORUM_HELP_CHANNEL_ID",
        "DISCORD_FORUM_LAUNCHER_AND_TECH_CHANNEL_ID", "DISCORD_FORUM_ACCOUNT_HELP_CHANNEL_ID",
        "DISCORD_FORUM_APPEALS_CHANNEL_ID", "VISITOR_ROLE_ID", "MEMBER_ROLE_ID", "GUILD_LEADER_ROLE_ID",
        "STAFF_ROLE_IDS", "MODERATION_ROLE_IDS", "SUPPORT_ROLE_IDS", "CONTENT_ROLE_IDS", "EVENT_ROLE_IDS",
        "COMMUNITY_MANAGER_ROLE_IDS", "INTEREST_ROLE_NEWS_ID", "INTEREST_ROLE_LORE_ID", "INTEREST_ROLE_GAMEPLAY_ID",
        "INTEREST_ROLE_EVENTS_ID", "INTEREST_ROLE_GUILDS_ID", "INTEREST_ROLE_MEDIA_ID", "INTEREST_ROLE_DEVLOGS_ID",
    ),
}


def ask(key: str, current: str = "") -> str:
    display_current = current
    if key in SECRET_KEYS and current:
        display_current = "***hidden***"
    label = key if not current else f"{key} [{display_current}]"
    value = input(label + ": ").strip()
    if not value:
        return current
    return value


def parse_env_lines(text: str) -> list[str]:
    return text.splitlines()


def extract_value(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in line:
        return None
    key, value = line.split("=", 1)
    return key.strip(), value.rstrip("\n")


def collect_values(lines: list[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in lines:
        parsed = extract_value(line)
        if parsed is None:
            continue
        key, value = parsed
        values[key] = value
    return values


def render_env(lines: list[str], values: dict[str, str], managed_keys: tuple[str, ...]) -> str:
    rendered: list[str] = []
    seen_keys: set[str] = set()
    managed = set(managed_keys)
    for line in lines:
        parsed = extract_value(line)
        if parsed is None:
            rendered.append(line)
            continue
        key, _old_value = parsed
        if key in values:
            rendered.append(f"{key}={values[key]}")
            seen_keys.add(key)
        else:
            rendered.append(line)
    missing = [key for key in managed_keys if key not in seen_keys]
    if missing:
        if rendered and rendered[-1].strip():
            rendered.append("")
        rendered.append("# NMDiscordBot managed values")
        for key in missing:
            rendered.append(f"{key}={values.get(key, DEFAULTS[key])}")
    return "\n".join(rendered).rstrip() + "\n"


def atomic_write_env(path: Path, content: str) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    os.chmod(tmp_path, stat.S_IRUSR | stat.S_IWUSR)
    tmp_path.replace(path)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)


def validate_current_env() -> int:
    from nmbot.config import Settings, SettingsError

    try:
        settings = Settings.load()
    except SettingsError as exc:
        print(f"[!] Конфигурация невалидна: {exc}")
        return 2
    missing_required = [key for key in sorted(PRODUCTION_REQUIRED_KEYS) if not os.getenv(key, DEFAULTS.get(key, "")).strip()]
    if missing_required:
        print("[!] Не заполнены production-required ключи: " + ", ".join(missing_required))
        return 3
    print("[✓] .env валиден для NMDiscordBot")
    print(f"Storage: {settings.storage_backend}; command surface: {settings.command_surface_mode}; ingress: {settings.ingress_enabled}; metrics: {settings.metrics_enabled}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NMDiscordBot setup wizard")
    parser.add_argument("--profile", choices=sorted(PROFILE_KEYS), default="production", help="набор env-полей для настройки")
    parser.add_argument("--validate-only", action="store_true", help="только проверить текущий .env")
    parser.add_argument("--non-interactive", action="store_true", help="не задавать вопросы, только добавить отсутствующие значения профиля")
    parser.add_argument("--list-profiles", action="store_true", help="показать профили и выйти")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.list_profiles:
        for name, keys in PROFILE_KEYS.items():
            print(f"{name}: {len(keys)} keys")
        return 0
    if args.validate_only:
        return validate_current_env()

    lines = parse_env_lines(ENV_FILE.read_text(encoding="utf-8")) if ENV_FILE.exists() else []
    current_values = collect_values(lines)
    managed_keys = PROFILE_KEYS[args.profile]
    values = {**DEFAULTS, **current_values}

    print("NMDiscordBot setup wizard")
    print(f"Профиль: {args.profile}. Пустой ввод оставляет текущее значение. Существующий .env сохраняется.\n")
    if not args.non_interactive:
        for key in managed_keys:
            values[key] = ask(key, values.get(key, DEFAULTS[key]))

    atomic_write_env(ENV_FILE, render_env(lines, values, managed_keys))
    print(f"\n[✓] Конфиг сохранён: {ENV_FILE}")
    print("Права на .env выставлены как 0600.")
    print("Проверка: ./setup_wizard.py --validate-only")
    print("Первое развёртывание: ./install.sh; обновление: ./upgrade.sh; запуск: ./run.sh")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
