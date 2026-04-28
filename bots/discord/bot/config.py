from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Any
from pathlib import Path

from dotenv import load_dotenv

from .config_schema import env_default
from .event_contracts import PAYLOAD_VALIDATORS

load_dotenv()


class SettingsError(ValueError):
    pass


def _get_raw(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        raw = env_default(name)
    return raw


def _get_bool(name: str, default: bool) -> bool:
    raw = _get_raw(name)
    if raw is None or not raw.strip():
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise SettingsError(f"{name} must be a boolean value")


def _get_int(name: str, default: int | None = None, *, minimum: int | None = None) -> int | None:
    raw = _get_raw(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw.strip())
    except ValueError as exc:
        raise SettingsError(f"{name} must be an integer") from exc
    if minimum is not None and value < minimum:
        raise SettingsError(f"{name} must be >= {minimum}")
    return value


def _get_float(name: str, default: float, *, minimum: float | None = None) -> float:
    raw = _get_raw(name)
    if raw is None or not raw.strip():
        value = default
    else:
        try:
            value = float(raw.strip())
        except ValueError as exc:
            raise SettingsError(f"{name} must be a number") from exc
    if minimum is not None and value < minimum:
        raise SettingsError(f"{name} must be >= {minimum}")
    return value


def _get_csv_ints(name: str) -> tuple[int, ...]:
    raw = (_get_raw(name) or "").strip()
    if not raw:
        return tuple()
    values: list[int] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            values.append(int(item))
        except ValueError as exc:
            raise SettingsError(f"{name} must contain only comma-separated integers") from exc
    return tuple(values)


def _get_str(name: str, default: str = "") -> str:
    raw = _get_raw(name)
    if raw is None:
        raw = default
    return raw.strip()



def _runtime_path(name: str, default: str | Path) -> Path:
    raw = _get_str(name, str(default)) or str(default)
    return Path(raw).expanduser()


def _ensure_writable_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    probe = path / ".write-test"
    try:
        probe.write_text("ok", encoding="utf-8")
    finally:
        try:
            probe.unlink()
        except OSError:
            pass


def _warn_runtime_path_fallback(path_name: str, original: Path, fallback: Path) -> None:
    print(
        f"[WARN] {path_name} is not writable/creatable: {original}; "
        f"using fallback {fallback}",
        file=os.sys.stderr,
    )


def _bot_tmp_dir(*parts: str) -> Path:
    return Path(os.getenv("TMPDIR") or "/tmp", "nmdiscordbot", *parts).expanduser()


def _path_is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.absolute().relative_to(parent.absolute())
    except ValueError:
        return False
    return True


def _rebase_runtime_path(path: Path, old_base: Path, new_base: Path) -> Path:
    try:
        return new_base / path.absolute().relative_to(old_base.absolute())
    except ValueError:
        return path


def _shared_runtime_path() -> Path:
    # BotHost exposes shared storage as /app/shared and may also inject SHARED_DIR.
    # Do not create it automatically: the mount appears only after Shared Storage is
    # enabled for the bot and the bot is redeployed.
    raw = _get_str("SHARED_DIR", os.getenv("SHARED_DIR") or "/app/shared") or "/app/shared"
    return Path(raw).expanduser()


def _public_url_from_env() -> str:
    raw = _get_str("APP_PUBLIC_URL") or _get_str("PUBLIC_BASE_URL") or _get_str("PUBLIC_URL")
    if not raw:
        domain = _get_str("DOMAIN")
        if domain:
            raw = domain if domain.startswith(("http://", "https://")) else f"https://{domain}"
    return raw.rstrip("/")


def _get_json_object(name: str) -> dict[str, Any]:
    raw = (_get_raw(name) or '').strip()
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except Exception as exc:
        raise SettingsError(f"{name} must be valid JSON object") from exc
    if not isinstance(value, dict):
        raise SettingsError(f"{name} must be a JSON object")
    return value


def _get_json_mapping(name: str) -> dict[str, tuple[str, ...]]:
    raw = (_get_raw(name) or '').strip()
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except Exception as exc:
        raise SettingsError(f"{name} must be valid JSON object") from exc
    if not isinstance(value, dict):
        raise SettingsError(f"{name} must be a JSON object")
    parsed: dict[str, tuple[str, ...]] = {}
    for key, item in value.items():
        if isinstance(item, str):
            parsed[str(key)] = (item.strip(),) if item.strip() else tuple()
        elif isinstance(item, list):
            vals=[]
            for v in item:
                if not isinstance(v, str):
                    raise SettingsError(f"{name}[{key}] must contain only strings")
                s=v.strip()
                if s:
                    vals.append(s)
            parsed[str(key)] = tuple(vals)
        else:
            raise SettingsError(f"{name}[{key}] must be string or list of strings")
    return parsed




def _get_json_nested_object(name: str) -> dict[str, dict[str, Any]]:
    raw = (_get_raw(name) or '').strip()
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except Exception as exc:
        raise SettingsError(f"{name} must be valid JSON object") from exc
    if not isinstance(value, dict):
        raise SettingsError(f"{name} must be a JSON object")
    parsed: dict[str, dict[str, Any]] = {}
    for key, item in value.items():
        if not isinstance(item, dict):
            raise SettingsError(f"{name}[{key}] must be a JSON object")
        parsed[str(key)] = dict(item)
    return parsed


KNOWN_BRIDGE_DESTINATIONS = {'community_core', 'community', 'telegram', 'vk', 'workspace'}
CANONICAL_BRIDGE_DESTINATIONS = {'community_core', 'telegram', 'vk', 'workspace'}
BRIDGE_DESTINATION_ALIASES = {'community': 'community_core', 'community-core': 'community_core'}
LEGACY_BRIDGE_RULE_KEYS = {'announcements', 'events', 'support', 'reports', 'guild_recruitment', 'identity'}
KNOWN_BRIDGE_EVENT_RULE_KEYS = {'*'} | LEGACY_BRIDGE_RULE_KEYS | set(PAYLOAD_VALIDATORS)
KNOWN_BRIDGE_EVENT_PREFIXES = ('community.', 'identity.', 'admin.')
KNOWN_FORUM_POLICY_KINDS = {'support', 'bug', 'suggestion', 'appeal', 'guild_recruitment', 'report', 'chronicle', 'lore_discussion'}
KNOWN_COMMAND_SURFACE_MODES = {'compat', 'grouped-only', 'legacy-only'}


def normalize_bridge_destination_name(name: str) -> str:
    normalized = str(name or '').strip().lower().replace('-', '_')
    return BRIDGE_DESTINATION_ALIASES.get(normalized, normalized)


def _normalize_bridge_rules(rules: dict[str, tuple[str, ...]]) -> dict[str, tuple[str, ...]]:
    normalized: dict[str, tuple[str, ...]] = {}
    for event_key, destinations in (rules or {}).items():
        key = str(event_key or '').strip()
        if not key:
            continue
        normalized_destinations: list[str] = []
        for destination in destinations:
            dest = normalize_bridge_destination_name(destination)
            if dest == '*':
                normalized_destinations.append(dest)
                continue
            if dest not in CANONICAL_BRIDGE_DESTINATIONS:
                raise SettingsError(f"BRIDGE_EVENT_RULES_JSON[{key}] contains unsupported destination: {destination}")
            normalized_destinations.append(dest)
        normalized[key] = tuple(dict.fromkeys(normalized_destinations))
    return normalized


def _is_known_bridge_event_rule_key(key: str) -> bool:
    return key in KNOWN_BRIDGE_EVENT_RULE_KEYS or any(key.startswith(prefix) for prefix in KNOWN_BRIDGE_EVENT_PREFIXES)


def _is_known_bridge_payload_key(key: str) -> bool:
    # Payload allowlist is keyed by event type in runtime, but older drafts used
    # destination names. Accept both to avoid breaking existing private archives.
    return _is_known_bridge_event_rule_key(key) or normalize_bridge_destination_name(key) in CANONICAL_BRIDGE_DESTINATIONS


def _normalize_and_validate_settings(settings: "Settings") -> "Settings":
    if settings.content_schema_version_required < 1:
        raise SettingsError("CONTENT_SCHEMA_VERSION_REQUIRED must be >= 1")
    for key in settings.bridge_payload_allowlist.keys():
        if not _is_known_bridge_payload_key(key):
            raise SettingsError(f"BRIDGE_PAYLOAD_ALLOWLIST_JSON contains unsupported key: {key}")
    settings.bridge_event_rules = _normalize_bridge_rules(settings.bridge_event_rules)
    for key in settings.bridge_event_rules.keys():
        if not _is_known_bridge_event_rule_key(key):
            raise SettingsError(f"BRIDGE_EVENT_RULES_JSON contains unsupported event key: {key}")
    for kind in settings.forum_policy_overrides.keys():
        if kind not in KNOWN_FORUM_POLICY_KINDS:
            raise SettingsError(f"FORUM_POLICY_OVERRIDES_JSON contains unsupported forum kind: {kind}")
    if settings.ingress_enabled and settings.ingress_strict_auth and not (settings.ingress_bearer_token or settings.ingress_hmac_secret):
        raise SettingsError("INGRESS_ENABLED=true and INGRESS_STRICT_AUTH=true require INGRESS_BEARER_TOKEN or INGRESS_HMAC_SECRET")
    if settings.app_public_url and not settings.app_public_url.startswith("https://"):
        raise SettingsError("APP_PUBLIC_URL must start with https:// for BotHost/Telegram-compatible public endpoints")
    if settings.command_surface_mode not in KNOWN_COMMAND_SURFACE_MODES:
        raise SettingsError("COMMAND_SURFACE_MODE must be one of: compat, grouped-only, legacy-only")
    if settings.min_free_disk_mb < 64:
        raise SettingsError("MIN_FREE_DISK_MB must be >= 64")
    if settings.max_topic_attachments < 1 or settings.max_topic_attachments > 5:
        raise SettingsError("MAX_TOPIC_ATTACHMENTS must be between 1 and 5")
    if not settings.metrics_path.startswith("/"):
        raise SettingsError("METRICS_PATH must start with /")
    if settings.metrics_enabled and settings.metrics_require_auth and not (settings.metrics_bearer_token or settings.ingress_bearer_token or settings.ingress_hmac_secret):
        raise SettingsError("METRICS_REQUIRE_AUTH=true requires METRICS_BEARER_TOKEN or ingress auth secrets")
    try:
        _ensure_writable_dir(settings.data_dir)
    except OSError:
        original_data_dir = settings.data_dir
        fallback_data_dir = _bot_tmp_dir("data")
        try:
            _ensure_writable_dir(fallback_data_dir)
        except OSError as fallback_exc:
            raise SettingsError(
                f"DATA_DIR is not writable/creatable: {original_data_dir}; "
                f"fallback data dir is also unavailable: {fallback_data_dir}."
            ) from fallback_exc
        settings.data_dir = fallback_data_dir
        if _path_is_relative_to(settings.sqlite_path, original_data_dir):
            settings.sqlite_path = _rebase_runtime_path(settings.sqlite_path, original_data_dir, fallback_data_dir)
        if _path_is_relative_to(settings.backup_dir, original_data_dir):
            settings.backup_dir = _rebase_runtime_path(settings.backup_dir, original_data_dir, fallback_data_dir)
        if _path_is_relative_to(settings.log_dir, original_data_dir):
            settings.log_dir = _rebase_runtime_path(settings.log_dir, original_data_dir, fallback_data_dir)
        _warn_runtime_path_fallback("DATA_DIR", original_data_dir, fallback_data_dir)

    try:
        _ensure_writable_dir(settings.backup_dir)
    except OSError:
        original_backup_dir = settings.backup_dir
        fallback_backup_dir = _bot_tmp_dir("backups")
        try:
            _ensure_writable_dir(fallback_backup_dir)
        except OSError:
            settings.backup_on_critical_changes = False
            print(
                f"[WARN] BACKUP_DIR is not writable/creatable: {original_backup_dir}; "
                "fallback backup dir is also unavailable; critical-change backups disabled",
                file=os.sys.stderr,
            )
        else:
            settings.backup_dir = fallback_backup_dir
            _warn_runtime_path_fallback("BACKUP_DIR", original_backup_dir, fallback_backup_dir)

    try:
        _ensure_writable_dir(settings.log_dir)
    except OSError:
        original_log_dir = settings.log_dir
        fallback_log_dir = _bot_tmp_dir("logs")
        try:
            _ensure_writable_dir(fallback_log_dir)
        except OSError as fallback_exc:
            raise SettingsError(
                f"LOG_DIR is not writable/creatable: {original_log_dir}; "
                f"fallback log dir is also unavailable: {fallback_log_dir}."
            ) from fallback_exc
        settings.log_dir = fallback_log_dir
        _warn_runtime_path_fallback("LOG_DIR", original_log_dir, fallback_log_dir)
    return settings

@dataclass(slots=True)
class Settings:
    discord_token: str
    discord_guild_id: int | None
    discord_status_channel_id: int | None
    discord_announcements_channel_id: int | None
    discord_events_channel_id: int | None
    discord_audit_channel_id: int | None
    discord_security_audit_channel_id: int | None
    discord_business_audit_channel_id: int | None
    discord_ops_audit_channel_id: int | None
    discord_start_here_channel_id: int | None
    discord_rules_channel_id: int | None
    discord_roles_channel_id: int | None
    discord_faq_channel_id: int | None
    discord_devlog_channel_id: int | None
    discord_world_signals_channel_id: int | None
    discord_reports_channel_id: int | None
    discord_bot_logs_channel_id: int | None
    discord_stage_channel_id: int | None
    discord_forum_suggestions_channel_id: int | None
    discord_forum_bug_reports_channel_id: int | None
    discord_forum_guild_recruitment_channel_id: int | None
    discord_forum_help_channel_id: int | None
    discord_forum_launcher_and_tech_channel_id: int | None
    discord_forum_account_help_channel_id: int | None
    discord_forum_appeals_channel_id: int | None
    visitor_role_id: int | None
    member_role_id: int | None
    guild_leader_role_id: int | None
    interest_role_news_id: int | None
    interest_role_lore_id: int | None
    interest_role_gameplay_id: int | None
    interest_role_events_id: int | None
    interest_role_guilds_id: int | None
    interest_role_media_id: int | None
    interest_role_devlogs_id: int | None
    discord_sync_commands_on_start: bool
    discord_startup_validation_strict: bool
    command_prefix: str
    command_surface_mode: str
    log_level: str
    log_json: bool
    bot_presence_text: str
    bot_use_prefix_commands: bool
    allow_degraded_without_redis: bool
    audit_payload_max_string_length: int
    audit_payload_max_collection_items: int
    audit_payload_max_depth: int
    audit_payload_max_bytes: int
    sqlite_optimize_on_cleanup: bool
    sqlite_analyze_on_cleanup: bool
    sqlite_vacuum_min_interval_seconds: int
    nevermine_server_name: str
    nevermine_server_address: str
    nevermine_website_url: str
    nevermine_vk_url: str
    nevermine_telegram_url: str
    nevermine_discord_invite_url: str
    nevermine_api_base_url: str
    nevermine_api_token: str
    nevermine_status_endpoint: str
    nevermine_players_endpoint: str
    nevermine_announcements_endpoint: str
    nevermine_events_endpoint: str
    nevermine_verify_start_endpoint: str
    nevermine_verify_complete_endpoint: str
    nevermine_link_status_endpoint: str
    nevermine_link_unlink_endpoint: str
    nevermine_request_timeout: float
    nevermine_request_retries: int
    nevermine_request_retry_backoff_seconds: float
    nevermine_request_retry_backoff_max_seconds: float
    staff_role_ids: tuple[int, ...]
    admin_user_ids: tuple[int, ...]
    moderation_role_ids: tuple[int, ...]
    support_role_ids: tuple[int, ...]
    content_role_ids: tuple[int, ...]
    event_role_ids: tuple[int, ...]
    community_manager_role_ids: tuple[int, ...]
    strict_runtime_precheck: bool
    verified_role_id: int | None
    relay_enabled: bool
    relay_poll_interval_seconds: int
    relay_status_changes: bool
    relay_announcements: bool
    relay_events: bool
    storage_backend: str
    database_url: str
    sqlite_path: Path
    postgres_pool_min_size: int
    postgres_pool_max_size: int
    redis_url: str
    redis_namespace: str
    redis_relay_dedupe_ttl_seconds: int
    redis_lock_ttl_seconds: int
    redis_command_cooldown_seconds: int
    audit_relay_max_preview_length: int
    audit_log_retention_days: int
    verification_session_retention_days: int
    relay_history_retention_days: int
    cleanup_interval_seconds: int
    data_dir: Path
    shared_dir: Path
    healthcheck_strict_storage: bool
    healthcheck_strict_redis: bool
    community_core_event_url: str
    telegram_bridge_url: str
    vk_bridge_url: str
    workspace_bridge_url: str
    outbound_hmac_secret: str
    outbound_bearer_token: str
    outbound_key_id: str
    bridge_timeout_seconds: float
    bridge_event_ttl_seconds: int
    bridge_delivery_batch_size: int
    bridge_max_attempts: int
    bridge_retry_backoff_base_seconds: int
    bridge_retry_backoff_max_seconds: int
    bridge_destination_circuit_breaker_threshold: int
    bridge_destination_circuit_open_seconds: int
    ingress_enabled: bool
    app_public_url: str
    ingress_host: str
    ingress_port: int
    ingress_bearer_token: str
    ingress_hmac_secret: str
    ingress_strict_auth: bool
    bridge_sync_announcements: bool
    bridge_sync_events: bool
    bridge_sync_support: bool
    bridge_sync_reports: bool
    bridge_sync_guild_recruitment: bool
    bridge_sync_identity: bool
    panel_auto_reconcile_on_ready: bool
    community_command_cooldown_seconds: int
    guild_recruit_command_cooldown_seconds: int
    scheduler_poll_interval_seconds: int
    scheduler_max_attempts: int
    scheduler_retry_backoff_base_seconds: int
    scheduler_retry_backoff_max_seconds: int
    forum_auto_close_inactive_hours: int
    forum_recruitment_auto_close_hours: int
    forum_tag_bug_name: str
    forum_tag_support_name: str
    forum_tag_suggestion_name: str
    forum_tag_appeal_name: str
    forum_tag_guild_recruitment_name: str
    forum_tag_status_open_name: str
    forum_tag_status_in_review_name: str
    forum_tag_status_resolved_name: str
    forum_tag_status_closed_name: str
    discord_content_file_path: Path
    content_schema_version_required: int
    bridge_payload_allowlist: dict[str, tuple[str, ...]]
    bridge_event_rules: dict[str, tuple[str, ...]]
    approval_required_for_announce: bool
    approval_required_for_verify_unlink: bool

    forum_auto_create_tags: bool
    rules_version: str
    rules_reacceptance_enforcement_enabled: bool
    rules_reacceptance_grace_hours: int
    rules_reacceptance_reminder_hours: int
    rules_reacceptance_check_interval_seconds: int
    support_escalation_hours: int
    appeal_escalation_hours: int
    report_escalation_hours: int
    guild_recruitment_expiry_warning_hours: int
    forum_status_source: str
    discord_content_require_russian: bool
    interest_role_ping_map: dict[str, tuple[str, ...]]
    forum_duplicate_detection_hours: int
    topic_transcript_history_limit: int
    forum_attachment_policy: dict[str, dict[str, Any]]
    attachment_max_bytes_default: int
    attachment_allowed_extensions_default: tuple[str, ...]
    attachment_blocked_extensions_default: tuple[str, ...]
    forum_policy_overrides: dict[str, dict[str, Any]]
    maintenance_mode_default: bool
    maintenance_mode_message: str
    ingress_previous_hmac_secret: str
    staff_scope_role_map: dict[str, tuple[str, ...]]
    strict_env_production_hygiene: bool
    backup_on_critical_changes: bool
    backup_dir: Path
    bridge_dlq_after_hours: int
    job_dlq_after_hours: int
    systemd_service_name: str
    min_free_disk_mb: int
    log_dir: Path
    recovery_mode_default: bool
    metrics_enabled: bool
    metrics_path: str
    metrics_require_auth: bool
    metrics_bearer_token: str
    metrics_allowed_ips: tuple[str, ...]
    layout_schema_version_required: int
    alias_binding_version_required: int
    permission_matrix_version_required: int
    drift_monitor_enabled: bool
    drift_monitor_interval_seconds: int
    drift_alert_cooldown_seconds: int
    max_topic_attachments: int
    attachment_total_max_bytes_default: int
    telegram_event_semantic_aliases: dict[str, tuple[str, ...]]

    @classmethod
    def load(cls) -> "Settings":
        token = _get_str("DISCORD_TOKEN")
        if not token:
            raise SettingsError("DISCORD_TOKEN is required")

        command_prefix = _get_str("COMMAND_PREFIX", "!") or "!"
        log_level = _get_str("LOG_LEVEL", "INFO").upper() or "INFO"
        allowed_levels = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
        if log_level not in allowed_levels:
            raise SettingsError(f"LOG_LEVEL must be one of: {', '.join(sorted(allowed_levels))}")

        data_dir = _runtime_path("DATA_DIR", "/app/data")
        shared_dir = _shared_runtime_path()
        storage_backend = (_get_str("STORAGE_BACKEND", "sqlite") or "sqlite").lower()
        if storage_backend not in {"sqlite", "postgresql"}:
            raise SettingsError("STORAGE_BACKEND must be one of: sqlite, postgresql")
        database_url = _get_str("DATABASE_URL")
        sqlite_path = _runtime_path("SQLITE_PATH", data_dir / "nmdiscordbot.sqlite3")
        if storage_backend == "postgresql" and not database_url:
            raise SettingsError("DATABASE_URL is required when STORAGE_BACKEND=postgresql")

        pool_min_size = int(_get_float("POSTGRES_POOL_MIN_SIZE", 1.0, minimum=1.0))
        pool_max_size = int(_get_float("POSTGRES_POOL_MAX_SIZE", 5.0, minimum=1.0))
        if pool_max_size < pool_min_size:
            raise SettingsError("POSTGRES_POOL_MAX_SIZE must be >= POSTGRES_POOL_MIN_SIZE")

        port_default = _get_int("PORT", 8080, minimum=1) or 8080
        ingress_port = int(_get_float("INGRESS_PORT", float(port_default), minimum=1.0))
        app_public_url = _public_url_from_env()
        forum_status_source = ((_get_str("FORUM_STATUS_SOURCE", "hybrid") or "hybrid").lower())
        if forum_status_source not in {"tags", "title", "hybrid"}:
            raise SettingsError("FORUM_STATUS_SOURCE must be one of: tags, title, hybrid")

        strict_env_production_hygiene = _get_bool("STRICT_ENV_PRODUCTION_HYGIENE", True)
        backup_on_critical_changes = _get_bool("BACKUP_ON_CRITICAL_CHANGES", True)
        backup_dir = _runtime_path("BACKUP_DIR", data_dir / "backups")
        bridge_dlq_after_hours = int(_get_float("BRIDGE_DLQ_AFTER_HOURS", 24.0, minimum=1.0))
        job_dlq_after_hours = int(_get_float("JOB_DLQ_AFTER_HOURS", 24.0, minimum=1.0))
        bridge_max_attempts = int(_get_float("BRIDGE_MAX_ATTEMPTS", 8.0, minimum=1.0))
        bridge_retry_backoff_base_seconds = int(_get_float("BRIDGE_RETRY_BACKOFF_BASE_SECONDS", 15.0, minimum=1.0))
        bridge_retry_backoff_max_seconds = int(_get_float("BRIDGE_RETRY_BACKOFF_MAX_SECONDS", 900.0, minimum=5.0))
        bridge_destination_circuit_breaker_threshold = int(_get_float("BRIDGE_DESTINATION_CIRCUIT_BREAKER_THRESHOLD", 5.0, minimum=1.0))
        bridge_destination_circuit_open_seconds = int(_get_float("BRIDGE_DESTINATION_CIRCUIT_OPEN_SECONDS", 300.0, minimum=30.0))
        scheduler_max_attempts = int(_get_float("SCHEDULER_MAX_ATTEMPTS", 5.0, minimum=1.0))
        scheduler_retry_backoff_base_seconds = int(_get_float("SCHEDULER_RETRY_BACKOFF_BASE_SECONDS", 30.0, minimum=1.0))
        scheduler_retry_backoff_max_seconds = int(_get_float("SCHEDULER_RETRY_BACKOFF_MAX_SECONDS", 1800.0, minimum=5.0))
        drift_alert_cooldown_seconds = int(_get_float("DRIFT_ALERT_COOLDOWN_SECONDS", 1800.0, minimum=60.0))
        systemd_service_name = _get_str("SYSTEMD_SERVICE_NAME", "nmdiscordbot") or "nmdiscordbot"

        if strict_env_production_hygiene:
            placeholders = {"changeme", "change-me", "your-token", "example", "example-token", "example-secret", "token", "secret", "replace-me"}
            def _looks_placeholder(value: str) -> bool:
                val = (value or '').strip().lower()
                return bool(val) and any(p in val for p in placeholders)

            critical_pairs = {
                'DISCORD_TOKEN': token,
                'INGRESS_BEARER_TOKEN': _get_str('INGRESS_BEARER_TOKEN'),
                'INGRESS_HMAC_SECRET': _get_str('INGRESS_HMAC_SECRET'),
                'OUTBOUND_HMAC_SECRET': _get_str('OUTBOUND_HMAC_SECRET'),
                'OUTBOUND_BEARER_TOKEN': _get_str('OUTBOUND_BEARER_TOKEN'),
                'NEVERMINE_API_TOKEN': _get_str('NEVERMINE_API_TOKEN'),
            }
            for key, value in critical_pairs.items():
                if _looks_placeholder(value):
                    raise SettingsError(f"{key} contains a placeholder-like value and is not valid for production")
            if token.startswith('Bot ') or ' ' in token.strip():
                raise SettingsError('DISCORD_TOKEN must contain the raw token value without prefixes')
            if _get_bool('INGRESS_ENABLED', False) and _get_bool('INGRESS_STRICT_AUTH', True) and not (_get_str('INGRESS_BEARER_TOKEN') or _get_str('INGRESS_HMAC_SECRET')):
                raise SettingsError('Ingress strict auth enabled, but neither INGRESS_BEARER_TOKEN nor INGRESS_HMAC_SECRET is configured')
            if any(_get_str(name) for name in ('COMMUNITY_CORE_EVENT_URL','TELEGRAM_BRIDGE_URL','VK_BRIDGE_URL','WORKSPACE_BRIDGE_URL')) and not (_get_str('OUTBOUND_HMAC_SECRET') or _get_str('OUTBOUND_BEARER_TOKEN')):
                raise SettingsError('At least one bridge destination is configured, but neither OUTBOUND_HMAC_SECRET nor OUTBOUND_BEARER_TOKEN is set')

        settings = cls(
            discord_token=token,
            discord_guild_id=_get_int("DISCORD_GUILD_ID", None, minimum=1),
            discord_status_channel_id=_get_int("DISCORD_STATUS_CHANNEL_ID", None, minimum=1),
            discord_announcements_channel_id=_get_int("DISCORD_ANNOUNCEMENTS_CHANNEL_ID", None, minimum=1),
            discord_events_channel_id=_get_int("DISCORD_EVENTS_CHANNEL_ID", None, minimum=1),
            discord_audit_channel_id=_get_int("DISCORD_AUDIT_CHANNEL_ID", None, minimum=1),
            discord_security_audit_channel_id=_get_int("DISCORD_SECURITY_AUDIT_CHANNEL_ID", None, minimum=1),
            discord_business_audit_channel_id=_get_int("DISCORD_BUSINESS_AUDIT_CHANNEL_ID", None, minimum=1),
            discord_ops_audit_channel_id=_get_int("DISCORD_OPS_AUDIT_CHANNEL_ID", None, minimum=1),
            discord_start_here_channel_id=_get_int("DISCORD_START_HERE_CHANNEL_ID", None, minimum=1),
            discord_rules_channel_id=_get_int("DISCORD_RULES_CHANNEL_ID", None, minimum=1),
            discord_roles_channel_id=_get_int("DISCORD_ROLES_AND_ACCESS_CHANNEL_ID", None, minimum=1),
            discord_faq_channel_id=_get_int("DISCORD_FAQ_CHANNEL_ID", None, minimum=1),
            discord_devlog_channel_id=_get_int("DISCORD_DEVLOG_CHANNEL_ID", None, minimum=1),
            discord_world_signals_channel_id=_get_int("DISCORD_WORLD_SIGNALS_CHANNEL_ID", None, minimum=1),
            discord_reports_channel_id=_get_int("DISCORD_REPORTS_CHANNEL_ID", None, minimum=1),
            discord_bot_logs_channel_id=_get_int("DISCORD_BOT_LOGS_CHANNEL_ID", None, minimum=1),
            discord_stage_channel_id=_get_int("DISCORD_STAGE_CHANNEL_ID", None, minimum=1),
            discord_forum_suggestions_channel_id=_get_int("DISCORD_FORUM_SUGGESTIONS_CHANNEL_ID", None, minimum=1),
            discord_forum_bug_reports_channel_id=_get_int("DISCORD_FORUM_BUG_REPORTS_CHANNEL_ID", None, minimum=1),
            discord_forum_guild_recruitment_channel_id=_get_int("DISCORD_FORUM_GUILD_RECRUITMENT_CHANNEL_ID", None, minimum=1),
            discord_forum_help_channel_id=_get_int("DISCORD_FORUM_HELP_CHANNEL_ID", None, minimum=1),
            discord_forum_launcher_and_tech_channel_id=_get_int("DISCORD_FORUM_LAUNCHER_AND_TECH_CHANNEL_ID", None, minimum=1),
            discord_forum_account_help_channel_id=_get_int("DISCORD_FORUM_ACCOUNT_HELP_CHANNEL_ID", None, minimum=1),
            discord_forum_appeals_channel_id=_get_int("DISCORD_FORUM_APPEALS_CHANNEL_ID", None, minimum=1),
            visitor_role_id=_get_int("VISITOR_ROLE_ID", None, minimum=1),
            member_role_id=_get_int("MEMBER_ROLE_ID", None, minimum=1),
            guild_leader_role_id=_get_int("GUILD_LEADER_ROLE_ID", None, minimum=1),
            interest_role_news_id=_get_int("INTEREST_ROLE_NEWS_ID", None, minimum=1),
            interest_role_lore_id=_get_int("INTEREST_ROLE_LORE_ID", None, minimum=1),
            interest_role_gameplay_id=_get_int("INTEREST_ROLE_GAMEPLAY_ID", None, minimum=1),
            interest_role_events_id=_get_int("INTEREST_ROLE_EVENTS_ID", None, minimum=1),
            interest_role_guilds_id=_get_int("INTEREST_ROLE_GUILDS_ID", None, minimum=1),
            interest_role_media_id=_get_int("INTEREST_ROLE_MEDIA_ID", None, minimum=1),
            interest_role_devlogs_id=_get_int("INTEREST_ROLE_DEVLOGS_ID", None, minimum=1),
            discord_sync_commands_on_start=_get_bool("DISCORD_SYNC_COMMANDS_ON_START", False),
            discord_startup_validation_strict=_get_bool("DISCORD_STARTUP_VALIDATION_STRICT", True),
            command_prefix=command_prefix,
            command_surface_mode=(_get_str("COMMAND_SURFACE_MODE", "compat") or "compat").strip().lower(),
            log_level=log_level,
            log_json=_get_bool("LOG_JSON", False),
            bot_presence_text=_get_str("BOT_PRESENCE_TEXT", "NeverMine | /status") or "NeverMine | /status",
            bot_use_prefix_commands=_get_bool("BOT_USE_PREFIX_COMMANDS", True),
            allow_degraded_without_redis=_get_bool("ALLOW_DEGRADED_WITHOUT_REDIS", True),
            audit_payload_max_string_length=int(_get_float("AUDIT_PAYLOAD_MAX_STRING_LENGTH", 256.0, minimum=32.0)),
            audit_payload_max_collection_items=int(_get_float("AUDIT_PAYLOAD_MAX_COLLECTION_ITEMS", 25.0, minimum=1.0)),
            audit_payload_max_depth=int(_get_float("AUDIT_PAYLOAD_MAX_DEPTH", 5.0, minimum=1.0)),
            audit_payload_max_bytes=int(_get_float("AUDIT_PAYLOAD_MAX_BYTES", 8192.0, minimum=512.0)),
            sqlite_optimize_on_cleanup=_get_bool("SQLITE_OPTIMIZE_ON_CLEANUP", True),
            sqlite_analyze_on_cleanup=_get_bool("SQLITE_ANALYZE_ON_CLEANUP", True),
            sqlite_vacuum_min_interval_seconds=int(_get_float("SQLITE_VACUUM_MIN_INTERVAL_SECONDS", 86400.0, minimum=0.0)),
            nevermine_server_name=_get_str("NEVERMINE_SERVER_NAME", "NeverMine") or "NeverMine",
            nevermine_server_address=_get_str("NEVERMINE_SERVER_ADDRESS", "play.nevermine.ru") or "play.nevermine.ru",
            nevermine_website_url=_get_str("NEVERMINE_WEBSITE_URL"),
            nevermine_vk_url=_get_str("NEVERMINE_VK_URL"),
            nevermine_telegram_url=_get_str("NEVERMINE_TELEGRAM_URL"),
            nevermine_discord_invite_url=_get_str("NEVERMINE_DISCORD_INVITE_URL"),
            nevermine_api_base_url=_get_str("NEVERMINE_API_BASE_URL").rstrip("/"),
            nevermine_api_token=_get_str("NEVERMINE_API_TOKEN"),
            nevermine_status_endpoint=_get_str("NEVERMINE_STATUS_ENDPOINT", "/status") or "/status",
            nevermine_players_endpoint=_get_str("NEVERMINE_PLAYERS_ENDPOINT", "/players") or "/players",
            nevermine_announcements_endpoint=_get_str("NEVERMINE_ANNOUNCEMENTS_ENDPOINT", "/community/announcements") or "/community/announcements",
            nevermine_events_endpoint=_get_str("NEVERMINE_EVENTS_ENDPOINT", "/community/events") or "/community/events",
            nevermine_verify_start_endpoint=_get_str("NEVERMINE_VERIFY_START_ENDPOINT", "/community/verify/start") or "/community/verify/start",
            nevermine_verify_complete_endpoint=_get_str("NEVERMINE_VERIFY_COMPLETE_ENDPOINT", "/community/verify/complete") or "/community/verify/complete",
            nevermine_link_status_endpoint=_get_str("NEVERMINE_LINK_STATUS_ENDPOINT", "/community/link/status") or "/community/link/status",
            nevermine_link_unlink_endpoint=_get_str("NEVERMINE_LINK_UNLINK_ENDPOINT", "/community/link/unlink") or "/community/link/unlink",
            nevermine_request_timeout=_get_float("NEVERMINE_REQUEST_TIMEOUT", 8.0, minimum=1.0),
            nevermine_request_retries=int(_get_float("NEVERMINE_REQUEST_RETRIES", 3.0, minimum=0.0)),
            nevermine_request_retry_backoff_seconds=_get_float("NEVERMINE_REQUEST_RETRY_BACKOFF_SECONDS", 1.0, minimum=0.1),
            nevermine_request_retry_backoff_max_seconds=_get_float("NEVERMINE_REQUEST_RETRY_BACKOFF_MAX_SECONDS", 8.0, minimum=0.1),
            staff_role_ids=_get_csv_ints("STAFF_ROLE_IDS"),
            admin_user_ids=_get_csv_ints("ADMIN_USER_IDS"),
            moderation_role_ids=_get_csv_ints("MODERATION_ROLE_IDS"),
            support_role_ids=_get_csv_ints("SUPPORT_ROLE_IDS"),
            content_role_ids=_get_csv_ints("CONTENT_ROLE_IDS"),
            event_role_ids=_get_csv_ints("EVENT_ROLE_IDS"),
            community_manager_role_ids=_get_csv_ints("COMMUNITY_MANAGER_ROLE_IDS"),
            strict_runtime_precheck=_get_bool("STRICT_RUNTIME_PRECHECK", True),
            verified_role_id=_get_int("VERIFIED_ROLE_ID", None, minimum=1),
            relay_enabled=_get_bool("RELAY_ENABLED", True),
            relay_poll_interval_seconds=int(_get_float("RELAY_POLL_INTERVAL_SECONDS", 60.0, minimum=5.0)),
            relay_status_changes=_get_bool("RELAY_STATUS_CHANGES", True),
            relay_announcements=_get_bool("RELAY_ANNOUNCEMENTS", True),
            relay_events=_get_bool("RELAY_EVENTS", True),
            storage_backend=storage_backend,
            database_url=database_url,
            sqlite_path=sqlite_path,
            postgres_pool_min_size=pool_min_size,
            postgres_pool_max_size=pool_max_size,
            redis_url=_get_str("REDIS_URL"),
            redis_namespace=_get_str("REDIS_NAMESPACE", "nmdiscordbot") or "nmdiscordbot",
            redis_relay_dedupe_ttl_seconds=int(_get_float("REDIS_RELAY_DEDUPE_TTL_SECONDS", 604800.0, minimum=1.0)),
            redis_lock_ttl_seconds=int(_get_float("REDIS_LOCK_TTL_SECONDS", 30.0, minimum=1.0)),
            redis_command_cooldown_seconds=int(_get_float("REDIS_COMMAND_COOLDOWN_SECONDS", 5.0, minimum=1.0)),
            audit_relay_max_preview_length=int(_get_float("AUDIT_RELAY_MAX_PREVIEW_LENGTH", 320.0, minimum=64.0)),
            audit_log_retention_days=int(_get_float("AUDIT_LOG_RETENTION_DAYS", 90.0, minimum=1.0)),
            verification_session_retention_days=int(_get_float("VERIFICATION_SESSION_RETENTION_DAYS", 14.0, minimum=1.0)),
            relay_history_retention_days=int(_get_float("RELAY_HISTORY_RETENTION_DAYS", 30.0, minimum=1.0)),
            cleanup_interval_seconds=int(_get_float("CLEANUP_INTERVAL_SECONDS", 21600.0, minimum=60.0)),
            data_dir=data_dir,
            shared_dir=shared_dir,
            healthcheck_strict_storage=_get_bool("HEALTHCHECK_STRICT_STORAGE", True),
            healthcheck_strict_redis=_get_bool("HEALTHCHECK_STRICT_REDIS", False),
            community_core_event_url=_get_str("COMMUNITY_CORE_EVENT_URL"),
            telegram_bridge_url=_get_str("TELEGRAM_BRIDGE_URL"),
            vk_bridge_url=_get_str("VK_BRIDGE_URL"),
            workspace_bridge_url=_get_str("WORKSPACE_BRIDGE_URL"),
            outbound_hmac_secret=_get_str("OUTBOUND_HMAC_SECRET"),
            outbound_bearer_token=_get_str("OUTBOUND_BEARER_TOKEN"),
            outbound_key_id=_get_str("OUTBOUND_KEY_ID", "v1") or "v1",
            bridge_timeout_seconds=_get_float("BRIDGE_TIMEOUT_SECONDS", 5.0, minimum=1.0),
            bridge_event_ttl_seconds=int(_get_float("BRIDGE_EVENT_TTL_SECONDS", 300.0, minimum=30.0)),
            bridge_delivery_batch_size=int(_get_float("BRIDGE_DELIVERY_BATCH_SIZE", 25.0, minimum=1.0)),
            bridge_max_attempts=bridge_max_attempts,
            bridge_retry_backoff_base_seconds=bridge_retry_backoff_base_seconds,
            bridge_retry_backoff_max_seconds=bridge_retry_backoff_max_seconds,
            bridge_destination_circuit_breaker_threshold=bridge_destination_circuit_breaker_threshold,
            bridge_destination_circuit_open_seconds=bridge_destination_circuit_open_seconds,
            ingress_enabled=_get_bool("INGRESS_ENABLED", False),
            app_public_url=app_public_url,
            ingress_host=_get_str("INGRESS_HOST", "0.0.0.0") or "0.0.0.0",
            ingress_port=ingress_port,
            ingress_bearer_token=_get_str("INGRESS_BEARER_TOKEN"),
            ingress_hmac_secret=_get_str("INGRESS_HMAC_SECRET"),
            ingress_strict_auth=_get_bool("INGRESS_STRICT_AUTH", True),
            bridge_sync_announcements=_get_bool("BRIDGE_SYNC_ANNOUNCEMENTS", True),
            bridge_sync_events=_get_bool("BRIDGE_SYNC_EVENTS", True),
            bridge_sync_support=_get_bool("BRIDGE_SYNC_SUPPORT", True),
            bridge_sync_reports=_get_bool("BRIDGE_SYNC_REPORTS", True),
            bridge_sync_guild_recruitment=_get_bool("BRIDGE_SYNC_GUILD_RECRUITMENT", True),
            bridge_sync_identity=_get_bool("BRIDGE_SYNC_IDENTITY", True),
            panel_auto_reconcile_on_ready=_get_bool("PANEL_AUTO_RECONCILE_ON_READY", True),
            community_command_cooldown_seconds=int(_get_float("COMMUNITY_COMMAND_COOLDOWN_SECONDS", 20.0, minimum=1.0)),
            guild_recruit_command_cooldown_seconds=int(_get_float("GUILD_RECRUIT_COMMAND_COOLDOWN_SECONDS", 120.0, minimum=1.0)),
            scheduler_poll_interval_seconds=int(_get_float("SCHEDULER_POLL_INTERVAL_SECONDS", 30.0, minimum=5.0)),
            scheduler_max_attempts=scheduler_max_attempts,
            scheduler_retry_backoff_base_seconds=scheduler_retry_backoff_base_seconds,
            scheduler_retry_backoff_max_seconds=scheduler_retry_backoff_max_seconds,
            forum_auto_close_inactive_hours=int(_get_float("FORUM_AUTO_CLOSE_INACTIVE_HOURS", 168.0, minimum=1.0)),
            forum_recruitment_auto_close_hours=int(_get_float("FORUM_RECRUITMENT_AUTO_CLOSE_HOURS", 336.0, minimum=1.0)),
            forum_tag_bug_name=_get_str("FORUM_TAG_BUG_NAME", "баг") or "баг",
            forum_tag_support_name=_get_str("FORUM_TAG_SUPPORT_NAME", "поддержка") or "поддержка",
            forum_tag_suggestion_name=_get_str("FORUM_TAG_SUGGESTION_NAME", "предложение") or "предложение",
            forum_tag_appeal_name=_get_str("FORUM_TAG_APPEAL_NAME", "апелляция") or "апелляция",
            forum_tag_guild_recruitment_name=_get_str("FORUM_TAG_GUILD_RECRUITMENT_NAME", "набор-в-гильдию") or "набор-в-гильдию",
            forum_tag_status_open_name=_get_str("FORUM_TAG_STATUS_OPEN_NAME", "открыто") or "открыто",
            forum_tag_status_in_review_name=_get_str("FORUM_TAG_STATUS_IN_REVIEW_NAME", "на-рассмотрении") or "на-рассмотрении",
            forum_tag_status_resolved_name=_get_str("FORUM_TAG_STATUS_RESOLVED_NAME", "решено") or "решено",
            forum_tag_status_closed_name=_get_str("FORUM_TAG_STATUS_CLOSED_NAME", "закрыто") or "закрыто",
            discord_content_file_path=Path(_get_str("DISCORD_CONTENT_FILE_PATH", "./templates/content.json")).expanduser(),
            content_schema_version_required=int(_get_float("CONTENT_SCHEMA_VERSION_REQUIRED", 4.0, minimum=1.0)),
            bridge_payload_allowlist=_get_json_mapping("BRIDGE_PAYLOAD_ALLOWLIST_JSON"),
            bridge_event_rules=_get_json_mapping("BRIDGE_EVENT_RULES_JSON"),
            approval_required_for_announce=_get_bool("APPROVAL_REQUIRED_FOR_ANNOUNCE", False),
            approval_required_for_verify_unlink=_get_bool("APPROVAL_REQUIRED_FOR_VERIFY_UNLINK", False),
            forum_auto_create_tags=_get_bool("FORUM_AUTO_CREATE_TAGS", True),
            rules_version=_get_str("RULES_VERSION", "1") or "1",
            rules_reacceptance_enforcement_enabled=_get_bool("RULES_REACCEPTANCE_ENFORCEMENT_ENABLED", True),
            rules_reacceptance_grace_hours=int(_get_float("RULES_REACCEPTANCE_GRACE_HOURS", 72.0, minimum=1.0)),
            rules_reacceptance_reminder_hours=int(_get_float("RULES_REACCEPTANCE_REMINDER_HOURS", 24.0, minimum=1.0)),
            rules_reacceptance_check_interval_seconds=int(_get_float("RULES_REACCEPTANCE_CHECK_INTERVAL_SECONDS", 900.0, minimum=60.0)),
            support_escalation_hours=int(_get_float("SUPPORT_ESCALATION_HOURS", 24.0, minimum=1.0)),
            appeal_escalation_hours=int(_get_float("APPEAL_ESCALATION_HOURS", 24.0, minimum=1.0)),
            report_escalation_hours=int(_get_float("REPORT_ESCALATION_HOURS", 12.0, minimum=1.0)),
            guild_recruitment_expiry_warning_hours=int(_get_float("GUILD_RECRUITMENT_EXPIRY_WARNING_HOURS", 24.0, minimum=1.0)),
            forum_status_source=forum_status_source,
            discord_content_require_russian=_get_bool("DISCORD_CONTENT_REQUIRE_RUSSIAN", True),
            interest_role_ping_map=_get_json_mapping("INTEREST_ROLE_PING_MAP_JSON"),
            forum_duplicate_detection_hours=int(_get_float("FORUM_DUPLICATE_DETECTION_HOURS", 72.0, minimum=1.0)),
            topic_transcript_history_limit=int(_get_float("TOPIC_TRANSCRIPT_HISTORY_LIMIT", 200.0, minimum=10.0)),
            forum_attachment_policy=_get_json_nested_object("FORUM_ATTACHMENT_POLICY_JSON"),
            attachment_max_bytes_default=int(_get_float("ATTACHMENT_MAX_BYTES_DEFAULT", 8388608.0, minimum=1.0)),
            attachment_allowed_extensions_default=tuple(x.strip().lower() for x in (_get_str("ATTACHMENT_ALLOWED_EXTENSIONS_DEFAULT", "png,jpg,jpeg,webp,gif,txt,log,json,pdf,zip,mp4,webm").split(',')) if x.strip()),
            attachment_blocked_extensions_default=tuple(x.strip().lower() for x in (_get_str("ATTACHMENT_BLOCKED_EXTENSIONS_DEFAULT", "exe,bat,cmd,sh,ps1,msi,scr,jar").split(',')) if x.strip()),
            forum_policy_overrides={str(k): (v if isinstance(v, dict) else {}) for k, v in _get_json_object("FORUM_POLICY_OVERRIDES_JSON").items()},
            maintenance_mode_default=_get_bool("MAINTENANCE_MODE_DEFAULT", False),
            maintenance_mode_message=_get_str("MAINTENANCE_MODE_MESSAGE", "Сейчас включён режим технических работ. Создание новых тем временно недоступно. Попробуйте позже.") or "Сейчас включён режим технических работ. Создание новых тем временно недоступно. Попробуйте позже.",
            ingress_previous_hmac_secret=_get_str("INGRESS_PREVIOUS_HMAC_SECRET"),
            staff_scope_role_map=_get_json_mapping("STAFF_SCOPE_ROLE_MAP_JSON"),
            strict_env_production_hygiene=strict_env_production_hygiene,
            backup_on_critical_changes=backup_on_critical_changes,
            backup_dir=backup_dir,
            bridge_dlq_after_hours=bridge_dlq_after_hours,
            job_dlq_after_hours=job_dlq_after_hours,
            systemd_service_name=systemd_service_name,
            min_free_disk_mb=int(_get_float("MIN_FREE_DISK_MB", 256.0, minimum=64.0)),
            log_dir=_runtime_path("LOG_DIR", data_dir / "logs"),
            recovery_mode_default=_get_bool("RECOVERY_MODE_DEFAULT", False),
            metrics_enabled=_get_bool("METRICS_ENABLED", False),
            metrics_path=_get_str("METRICS_PATH", "/internal/metrics") or "/internal/metrics",
            metrics_require_auth=_get_bool("METRICS_REQUIRE_AUTH", True),
            metrics_bearer_token=_get_str("METRICS_BEARER_TOKEN"),
            metrics_allowed_ips=tuple(x.strip() for x in (_get_str("METRICS_ALLOWED_IPS", "127.0.0.1,::1").split(',')) if x.strip()),
            layout_schema_version_required=int(_get_float("LAYOUT_SCHEMA_VERSION_REQUIRED", 3.0, minimum=1.0)),
            alias_binding_version_required=int(_get_float("ALIAS_BINDING_VERSION_REQUIRED", 1.0, minimum=1.0)),
            permission_matrix_version_required=int(_get_float("PERMISSION_MATRIX_VERSION_REQUIRED", 1.0, minimum=1.0)),
            drift_monitor_enabled=_get_bool("DRIFT_MONITOR_ENABLED", True),
            drift_monitor_interval_seconds=int(_get_float("DRIFT_MONITOR_INTERVAL_SECONDS", 300.0, minimum=30.0)),
            drift_alert_cooldown_seconds=drift_alert_cooldown_seconds,
            max_topic_attachments=int(_get_float("MAX_TOPIC_ATTACHMENTS", 3.0, minimum=1.0)),
            attachment_total_max_bytes_default=int(_get_float("ATTACHMENT_TOTAL_MAX_BYTES_DEFAULT", 20971520.0, minimum=1.0)),
            telegram_event_semantic_aliases=_get_json_mapping("TELEGRAM_EVENT_SEMANTIC_ALIASES_JSON"),
        )
        return _normalize_and_validate_settings(settings)
