from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from dotenv import dotenv_values

from .config import Settings, SettingsError, CANONICAL_BRIDGE_DESTINATIONS, normalize_bridge_destination_name, _is_known_bridge_event_rule_key
from .config_schema import ENV_SCHEMA, SECRET_KEYS, DEFAULTS
from .event_contracts import PAYLOAD_VALIDATORS

ROOT_DIR = Path(__file__).resolve().parent.parent

CHANNEL_KEYS = tuple(item.name for item in ENV_SCHEMA if item.name.startswith("DISCORD_") and item.name.endswith("_CHANNEL_ID"))
FORUM_CHANNEL_KEYS = tuple(item.name for item in ENV_SCHEMA if item.name.startswith("DISCORD_FORUM_") and item.name.endswith("_CHANNEL_ID"))
ROLE_KEYS = tuple(item.name for item in ENV_SCHEMA if item.name.endswith("ROLE_ID") or item.name.endswith("ROLE_IDS"))
BRIDGE_URL_KEYS = ("COMMUNITY_CORE_EVENT_URL", "TELEGRAM_BRIDGE_URL", "VK_BRIDGE_URL", "WORKSPACE_BRIDGE_URL")


def _read_env(path: Path = ROOT_DIR / ".env") -> dict[str, str]:
    if not path.exists():
        return {}
    return {str(k): str(v or "") for k, v in dotenv_values(path).items() if k is not None}


def _filled(env: dict[str, str], key: str) -> bool:
    return bool(str(env.get(key, "")).strip())


def _redacted(value: str, *, secret: bool) -> str:
    if not value:
        return ""
    if not secret:
        return value
    if len(value) <= 8:
        return "***redacted***"
    return f"{value[:3]}***{value[-3:]}"


def _parse_bridge_event_rules(raw: str) -> tuple[list[str], dict[str, tuple[str, ...]]]:
    errors: list[str] = []
    if not raw.strip():
        return errors, {}
    try:
        payload = json.loads(raw)
    except Exception as exc:
        return [f"BRIDGE_EVENT_RULES_JSON invalid JSON: {exc}"], {}
    if not isinstance(payload, dict):
        return ["BRIDGE_EVENT_RULES_JSON must be a JSON object"], {}
    parsed: dict[str, tuple[str, ...]] = {}
    for key, value in payload.items():
        key = str(key)
        if not _is_known_bridge_event_rule_key(key):
            errors.append(f"unsupported event rule key: {key}")
        if isinstance(value, str):
            values = (value,)
        elif isinstance(value, list):
            values = tuple(str(item) for item in value if str(item).strip())
        else:
            errors.append(f"BRIDGE_EVENT_RULES_JSON[{key}] must be string or string list")
            values = tuple()
        normalized = []
        for destination in values:
            dest = normalize_bridge_destination_name(destination)
            if dest not in CANONICAL_BRIDGE_DESTINATIONS and dest != "*":
                errors.append(f"unsupported bridge destination for {key}: {destination}")
            else:
                normalized.append(dest)
        parsed[key] = tuple(normalized)
    return errors, parsed


def run_env_doctor(*, runtime_version: str, env_path: Path = ROOT_DIR / ".env") -> int:
    env = _read_env(env_path)
    warnings: list[str] = []
    errors: list[str] = []

    try:
        settings = Settings.load()
        settings_error = ""
    except SettingsError as exc:
        settings = None
        settings_error = str(exc)
        errors.append(settings_error)

    required_missing = [item.name for item in ENV_SCHEMA if item.production_required and not _filled(env, item.name)]
    if required_missing:
        errors.append("missing production-required env keys: " + ", ".join(required_missing))

    bridge_rule_errors, bridge_rules = _parse_bridge_event_rules(env.get("BRIDGE_EVENT_RULES_JSON", DEFAULTS.get("BRIDGE_EVENT_RULES_JSON", "")))
    errors.extend(bridge_rule_errors)

    filled_channels = [key for key in CHANNEL_KEYS if _filled(env, key)]
    missing_channels = [key for key in CHANNEL_KEYS if not _filled(env, key)]
    filled_forums = [key for key in FORUM_CHANNEL_KEYS if _filled(env, key)]
    filled_roles = [key for key in ROLE_KEYS if _filled(env, key)]
    missing_roles = [key for key in ROLE_KEYS if not _filled(env, key)]
    configured_bridges = [key for key in BRIDGE_URL_KEYS if _filled(env, key)]

    if _filled(env, "REDIS_URL") and env.get("ALLOW_DEGRADED_WITHOUT_REDIS", DEFAULTS.get("ALLOW_DEGRADED_WITHOUT_REDIS", "true")).lower() == "true":
        warnings.append("REDIS_URL is set, but ALLOW_DEGRADED_WITHOUT_REDIS=true")
    if env.get("INGRESS_ENABLED", DEFAULTS.get("INGRESS_ENABLED", "false")).lower() == "true":
        if not (_filled(env, "INGRESS_HMAC_SECRET") or _filled(env, "INGRESS_BEARER_TOKEN")):
            errors.append("INGRESS_ENABLED=true requires INGRESS_HMAC_SECRET or INGRESS_BEARER_TOKEN")
        app_public_url = (env.get("APP_PUBLIC_URL") or env.get("PUBLIC_BASE_URL") or env.get("PUBLIC_URL") or "").strip()
        if app_public_url and not app_public_url.startswith("https://"):
            warnings.append("APP_PUBLIC_URL should start with https:// for BotHost public HTTP server")
        if (env.get("INGRESS_HOST") or DEFAULTS.get("INGRESS_HOST", "")).strip() not in {"0.0.0.0", "::"}:
            warnings.append("INGRESS_HOST is not 0.0.0.0; BotHost reverse proxy must reach the app inside the container")
        expected_port = (env.get("PORT") or DEFAULTS.get("PORT", "8080")).strip() or "8080"
        ingress_port_value = (env.get("INGRESS_PORT") or DEFAULTS.get("INGRESS_PORT", "8080")).strip() or "8080"
        if ingress_port_value != expected_port:
            warnings.append(f"INGRESS_PORT={ingress_port_value} differs from PORT={expected_port}; BotHost app port should match")
    if env.get("METRICS_ENABLED", DEFAULTS.get("METRICS_ENABLED", "false")).lower() == "true":
        if env.get("METRICS_REQUIRE_AUTH", DEFAULTS.get("METRICS_REQUIRE_AUTH", "true")).lower() == "true" and not _filled(env, "METRICS_BEARER_TOKEN"):
            warnings.append("METRICS_REQUIRE_AUTH=true but METRICS_BEARER_TOKEN is empty; ingress auth fallback may be used")
    remote_logs_enabled = env.get("REMOTE_LOGS_ENABLED", DEFAULTS.get("REMOTE_LOGS_ENABLED", "false")).lower() in {"1", "true", "yes", "on"}
    if remote_logs_enabled:
        if not _filled(env, "REMOTE_LOGS_URL"):
            errors.append("REMOTE_LOGS_ENABLED=true requires REMOTE_LOGS_URL")
        if not _filled(env, "REMOTE_LOGS_TOKEN"):
            errors.append("REMOTE_LOGS_ENABLED=true requires REMOTE_LOGS_TOKEN")
        remote_logs_url = (env.get("REMOTE_LOGS_URL") or DEFAULTS.get("REMOTE_LOGS_URL", "")).strip()
        if remote_logs_url and not remote_logs_url.startswith("https://"):
            warnings.append("REMOTE_LOGS_URL should start with https:// in production")
    if not env.get("COMMAND_SURFACE_MODE", "").strip():
        warnings.append("COMMAND_SURFACE_MODE is empty; runtime fallback is compat")
    if configured_bridges and not (_filled(env, "OUTBOUND_HMAC_SECRET") or _filled(env, "OUTBOUND_BEARER_TOKEN")):
        errors.append("Bridge URLs are configured, but outbound auth secret/token is empty")

    suspicious: list[str] = []
    sqlite_path = env.get("SQLITE_PATH", DEFAULTS.get("SQLITE_PATH", ""))
    data_dir = env.get("DATA_DIR", DEFAULTS.get("DATA_DIR", ""))
    log_dir = env.get("LOG_DIR", DEFAULTS.get("LOG_DIR", ""))
    backup_dir = env.get("BACKUP_DIR", DEFAULTS.get("BACKUP_DIR", ""))
    shared_dir = env.get("SHARED_DIR", DEFAULTS.get("SHARED_DIR", "/app/shared"))
    if "telegram" in sqlite_path.lower():
        suspicious.append("SQLITE_PATH mentions Telegram")
    if not str(data_dir).startswith("/app/data"):
        warnings.append("DATA_DIR is not /app/data; BotHost persistent per-bot files should live under /app/data")
    if str(sqlite_path) and not str(sqlite_path).startswith("/app/data"):
        warnings.append("SQLITE_PATH is not under /app/data; SQLite may be lost or unwritable after Git redeploy")
    if str(log_dir) and not str(log_dir).startswith("/app/data"):
        warnings.append("LOG_DIR is not under /app/data; use stdout or /app/data/logs on BotHost")
    if str(backup_dir) and not str(backup_dir).startswith("/app/data"):
        warnings.append("BACKUP_DIR is not under /app/data; backups should be stored in persistent storage")
    storage_backend = (env.get("STORAGE_BACKEND") or DEFAULTS.get("STORAGE_BACKEND", "sqlite")).strip().lower()
    if storage_backend == "postgres":
        suspicious.append("STORAGE_BACKEND=postgres should be normalized to postgresql")

    payload: dict[str, Any] = {
        "ok": not errors,
        "runtime_version": runtime_version,
        "env_path": str(env_path),
        "settings_load_ok": settings is not None,
        "settings_error": settings_error,
        "env_keys_known": len(DEFAULTS),
        "env_keys_present": len(env),
        "required_missing": required_missing,
        "storage_backend": getattr(settings, "storage_backend", storage_backend) if settings else storage_backend,
        "paths": {
            "data_dir": str(getattr(settings, "data_dir", data_dir) if settings else data_dir),
            "log_dir": str(getattr(settings, "log_dir", log_dir) if settings else log_dir),
            "backup_dir": str(getattr(settings, "backup_dir", backup_dir) if settings else backup_dir),
            "sqlite_path": str(getattr(settings, "sqlite_path", sqlite_path) if settings else sqlite_path),
            "shared_dir": str(getattr(settings, "shared_dir", Path(shared_dir)) if settings else shared_dir),
            "shared_dir_available": bool(getattr(settings, "shared_dir", Path(shared_dir)).exists()) if settings else Path(shared_dir).exists(),
        },
        "redis_configured": _filled(env, "REDIS_URL"),
        "redis_degraded_allowed": (env.get("ALLOW_DEGRADED_WITHOUT_REDIS", DEFAULTS.get("ALLOW_DEGRADED_WITHOUT_REDIS", "true")).lower() == "true"),
        "ingress_enabled": (env.get("INGRESS_ENABLED", DEFAULTS.get("INGRESS_ENABLED", "false")).lower() == "true"),
        "http_server": {
            "public_url": str(getattr(settings, "app_public_url", "") if settings else (env.get("APP_PUBLIC_URL") or "")),
            "bind_host": str(getattr(settings, "ingress_host", env.get("INGRESS_HOST", "")) if settings else env.get("INGRESS_HOST", "")),
            "container_port": int(getattr(settings, "ingress_port", int((env.get("INGRESS_PORT") or env.get("PORT") or DEFAULTS.get("INGRESS_PORT", "8080") or "8080"))) if settings else int((env.get("INGRESS_PORT") or env.get("PORT") or DEFAULTS.get("INGRESS_PORT", "8080") or "8080"))),
            "port_env": str(env.get("PORT", DEFAULTS.get("PORT", "8080"))),
        },
        "metrics_enabled": (env.get("METRICS_ENABLED", DEFAULTS.get("METRICS_ENABLED", "false")).lower() == "true"),
        "remote_logs": {
            "enabled": remote_logs_enabled,
            "url": env.get("REMOTE_LOGS_URL", DEFAULTS.get("REMOTE_LOGS_URL", "")),
            "token_present": _filled(env, "REMOTE_LOGS_TOKEN"),
            "project": env.get("REMOTE_LOGS_PROJECT", DEFAULTS.get("REMOTE_LOGS_PROJECT", "NeverMine")),
            "bot": env.get("REMOTE_LOGS_BOT", DEFAULTS.get("REMOTE_LOGS_BOT", "NMDiscordBot")),
            "environment": env.get("REMOTE_LOGS_ENVIRONMENT", DEFAULTS.get("REMOTE_LOGS_ENVIRONMENT", "production")),
            "spool_dir": env.get("REMOTE_LOGS_SPOOL_DIR", DEFAULTS.get("REMOTE_LOGS_SPOOL_DIR", "/app/data/remote-logs-spool")),
        },
        "command_surface_mode": env.get("COMMAND_SURFACE_MODE") or DEFAULTS.get("COMMAND_SURFACE_MODE"),
        "channels": {"filled": len(filled_channels), "missing": missing_channels},
        "forums": {"filled": len(filled_forums), "expected": len(FORUM_CHANNEL_KEYS)},
        "roles": {"filled": len(filled_roles), "missing": missing_roles},
        "bridge_destinations_configured": configured_bridges,
        "bridge_event_rules_count": len(bridge_rules),
        "bridge_event_rule_keys": sorted(bridge_rules),
        "payload_validators_known": len(PAYLOAD_VALIDATORS),
        "suspicious_values": suspicious,
        "warnings": warnings,
        "errors": errors,
    }
    # Include redacted secret presence only, never raw secret values.
    payload["secrets"] = {key: {"present": _filled(env, key), "preview": _redacted(env.get(key, ""), secret=True)} for key in sorted(SECRET_KEYS)}
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not errors else 4
