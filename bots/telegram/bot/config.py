from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv


_DEF_ROOT = Path(__file__).resolve().parent.parent
_ROLE_ORDER = {"user": 0, "mod": 1, "admin": 2, "owner": 3}
_ALLOWED_CHAT_SCOPES = {"all", "private", "groups"}
_ALLOWED_MODES = {"polling", "webhook"}
_ALLOWED_LOG_FORMATS = {"plain", "json"}
_ALLOWED_LANGUAGES = {"ru"}


class ConfigValidationError(RuntimeError):
    pass


def _read_secret_file(raw: str | None, *, name: str) -> str:
    if not raw or not raw.strip():
        return ""
    path = Path(raw.strip()).expanduser()
    if not path.is_absolute():
        path = (_DEF_ROOT / path).resolve()
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ConfigValidationError(f"{name} указывает на недоступный файл: {path}") from exc


def _env_or_file(name: str) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    return _read_secret_file(os.getenv(f"{name}_FILE"), name=f"{name}_FILE")


def _parse_int_set(raw: str | None, *, name: str) -> set[int]:
    if not raw:
        return set()
    values: set[int] = set()
    for item in raw.split(","):
        stripped = item.strip()
        if not stripped:
            continue
        try:
            values.add(int(stripped))
        except ValueError as exc:
            raise ConfigValidationError(f"{name} содержит нецелое значение: {stripped!r}") from exc
    return values


def _parse_str_set(raw: str | None) -> set[str]:
    if not raw:
        return set()
    return {item.strip() for item in raw.split(",") if item.strip()}


def _parse_int(raw: str | None, *, name: str, default: int) -> int:
    value = (raw or "").strip()
    if not value:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigValidationError(f"{name} должен быть integer") from exc


def _parse_float(raw: str | None, *, name: str, default: float) -> float:
    value = (raw or "").strip()
    if not value:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ConfigValidationError(f"{name} должен быть float") from exc


def _parse_bool(raw: str | None, *, default: bool = False) -> bool:
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _runtime_data_dir() -> Path:
    """Persistent per-bot runtime directory.

    BotHost mounts persistent bot storage at /app/data. DATA_DIR has priority
    because it is the platform-facing name; NMBOT_RUNTIME_DIR is kept as a
    backward-compatible alias for local deployments.
    """
    raw = os.getenv("DATA_DIR", "").strip() or os.getenv("NMBOT_RUNTIME_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    if Path("/app").exists():
        return Path("/app/data")
    return _DEF_ROOT


def _runtime_shared_dir() -> Path:
    """Shared storage directory for BotHost bots with common storage enabled."""
    raw = os.getenv("SHARED_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path("/app/shared")


def _runtime_shared_available() -> bool:
    shared_dir = _runtime_shared_dir()
    return shared_dir.exists() and shared_dir.is_dir()


def _public_base_url_from_env() -> str:
    for name in ("WEBHOOK_URL", "PUBLIC_HTTP_SERVER_URL", "BOT_PUBLIC_URL", "APP_URL"):
        value = os.getenv(name, "").strip()
        if value:
            return value.rstrip("/")
    domain = os.getenv("DOMAIN", "").strip().strip("/")
    if domain:
        if domain.startswith(("http://", "https://")):
            return domain.rstrip("/")
        return f"https://{domain}"
    return ""


def _nearest_existing_parent(path: Path) -> Path:
    current = path
    while not current.exists() and current != current.parent:
        current = current.parent
    return current


def _is_writable_target(path: Path, *, is_dir: bool = False) -> bool:
    target = path if is_dir else path.parent
    probe = _nearest_existing_parent(target)
    return os.access(probe, os.W_OK | os.X_OK)


def _can_prepare_writable_target(path: Path, *, is_dir: bool = False) -> bool:
    """Return True only when the target directory can actually be created and written.

    os.access() is not enough on BotHost-like mounts: the parent may appear
    traversable while mkdir/open still fails for the runtime user. This helper is
    intentionally side-effect light and removes its probe file immediately.
    """
    target_dir = path if is_dir else path.parent
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        probe = target_dir / f".nmbot-write-probe-{os.getpid()}"
        with probe.open("w", encoding="utf-8") as handle:
            handle.write("ok")
        probe.unlink(missing_ok=True)
        if not is_dir and path.exists() and not os.access(path, os.W_OK):
            return False
        return True
    except OSError:
        return False


def _fallback_runtime_dir() -> Path:
    raw = os.getenv("NMBOT_FALLBACK_RUNTIME_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (Path(tempfile.gettempdir()) / "nmtelegrambot").resolve()


def _resolve_runtime_path(raw: str, *, is_dir: bool = False) -> Path:
    configured = Path(raw).expanduser()
    if configured.is_absolute():
        return configured.resolve()
    # Runtime-mutating files must not be written into the Git checkout on BotHost.
    # Keep relative values ergonomic in .env, but anchor them under DATA_DIR.
    return (_runtime_data_dir() / configured).resolve()


def _resolve_log_file_path(raw: str) -> Path:
    path = _resolve_runtime_path(raw, is_dir=False)
    if _can_prepare_writable_target(path, is_dir=False):
        return path
    return (_fallback_runtime_dir() / "logs" / path.name).resolve()


def _resolve_templates_dir(raw: str) -> Path:
    configured = Path(raw).expanduser()
    if configured.is_absolute():
        return configured.resolve()

    shared_candidate = (_runtime_shared_dir() / configured).resolve()
    if _runtime_shared_available() and shared_candidate.exists():
        return shared_candidate

    bundled_candidate = (_DEF_ROOT / configured).resolve()
    if bundled_candidate.exists():
        return bundled_candidate

    return (_runtime_data_dir() / configured).resolve()


def _validate_url(name: str, value: str, *, required: bool = False) -> str:
    value = value.strip()
    if not value:
        if required:
            raise ConfigValidationError(f"{name} обязателен")
        return ""
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigValidationError(f"{name} должен быть корректным http(s) URL")
    return value


@dataclass(slots=True)
class BotConfig:
    telegram_bot_token: str
    telegram_bot_username: str
    telegram_owner_ids: set[int]
    telegram_admin_ids: set[int]
    telegram_mod_ids: set[int]
    telegram_allowed_chat_ids: set[int]
    telegram_chat_scope: str
    telegram_parse_mode: str
    bot_timezone: str
    bot_mode: str
    data_dir: Path
    shared_dir: Path
    shared_dir_available: bool
    public_http_server_url: str
    webhook_url: str
    webhook_listen: str
    webhook_port: int
    webhook_secret_token: str
    webhook_path_prefix: str
    auto_reconcile_webhook: bool
    health_http_listen: str
    health_http_port: int
    health_http_token: str
    health_http_minimal: bool
    delivery_max_concurrency: int
    delivery_max_per_minute: int
    operator_alert_cooldown_seconds: int
    log_level: str
    log_format: str
    log_file: Path
    sqlite_path: Path
    database_url: str
    instance_id: str
    request_timeout_seconds: float
    status_cache_seconds: float
    status_refresh_seconds: float
    status_retry_attempts: int
    status_retry_backoff_seconds: float
    command_cooldown_seconds: float
    status_command_cooldown_seconds: float
    broadcast_confirmation_seconds: int
    link_code_ttl_seconds: int
    cleanup_interval_seconds: int
    interaction_retention_days: int
    admin_action_retention_days: int
    runtime_state_retention_days: int
    dead_letter_retention_days: int
    link_history_retention_days: int
    security_history_retention_days: int
    scheduler_tick_seconds: int
    announcement_feed_interval_seconds: int
    delivery_retry_attempts: int
    delivery_retry_backoff_seconds: int
    leader_lock_ttl_seconds: int
    strict_api_schemas: bool
    template_strict_mode: bool
    server_status_url: str
    announcement_feed_url: str
    link_verify_url: str
    link_verify_auto_approve: bool
    security_status_url: str
    security_challenges_url: str
    security_2fa_action_url: str
    security_recovery_url: str
    security_sessions_url: str
    security_session_action_url: str
    server_api_bearer_token: str
    server_api_hmac_secret: str
    server_api_request_id_header: str
    templates_dir: Path
    artifact_root: Path
    public_site_url: str
    vk_url: str
    discord_url: str
    telegram_channel_url: str
    rules_url: str
    build_manifest_path: Path
    external_api_circuit_threshold: int
    external_api_circuit_reset_seconds: int
    backup_retention_days: int
    export_retention_days: int
    secret_sources: dict[str, str] = field(default_factory=dict)
    health_http_public: bool = False
    user_pref_default_timezone: str = "Europe/Berlin"
    strict_compatibility_gate: bool = False
    security_nonce_ttl_seconds: int = 600
    feed_nonce_ttl_seconds: int = 1800
    discord_bridge_url: str = ""
    discord_bridge_bearer_token: str = ""
    discord_bridge_hmac_secret: str = ""
    bridge_inbound_bearer_token: str = ""
    bridge_inbound_hmac_secret: str = ""
    bridge_ingress_strict_auth: bool = True
    bridge_target_chat_ids: set[int] = field(default_factory=set)
    bridge_target_scope: str = "all"
    bridge_target_tags: set[str] = field(default_factory=set)
    bridge_allowed_event_types: set[str] = field(default_factory=set)

    @property
    def links(self) -> dict[str, str]:
        return {
            "Сайт": self.public_site_url,
            "VK": self.vk_url,
            "Discord": self.discord_url,
            "Telegram": self.telegram_channel_url,
            "Правила": self.rules_url,
        }

    @property
    def admin_like_ids(self) -> set[int]:
        return set(self.telegram_owner_ids) | set(self.telegram_admin_ids)

    def role_for_user(self, user_id: int | None) -> str:
        if user_id is None:
            return "user"
        if user_id in self.telegram_owner_ids:
            return "owner"
        if user_id in self.telegram_admin_ids:
            return "admin"
        if user_id in self.telegram_mod_ids:
            return "mod"
        return "user"

    def has_role(self, user_id: int | None, required_role: str) -> bool:
        return _ROLE_ORDER[self.role_for_user(user_id)] >= _ROLE_ORDER[required_role]


def load_config(env_path: Path | None = None) -> BotConfig:
    env_file = env_path or (_DEF_ROOT / ".env")
    load_dotenv(env_file, override=_parse_bool(os.getenv("DOTENV_OVERRIDE"), default=False))

    token = _env_or_file("TELEGRAM_BOT_TOKEN")
    if not token or token == "replace_me":
        raise ConfigValidationError("TELEGRAM_BOT_TOKEN не задан в .env")

    chat_scope = (os.getenv("TELEGRAM_CHAT_SCOPE", "all").strip() or "all").lower()
    if chat_scope not in _ALLOWED_CHAT_SCOPES:
        raise ConfigValidationError("TELEGRAM_CHAT_SCOPE должен быть one of: all/private/groups")

    bot_mode = (os.getenv("BOT_MODE", "polling").strip() or "polling").lower()
    if bot_mode not in _ALLOWED_MODES:
        raise ConfigValidationError("BOT_MODE должен быть polling или webhook")

    log_format = (os.getenv("LOG_FORMAT", "plain").strip() or "plain").lower()
    if log_format not in _ALLOWED_LOG_FORMATS:
        raise ConfigValidationError("LOG_FORMAT должен быть plain или json")

    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        parsed = urlparse(database_url)
        if parsed.scheme == "sqlite":
            if not parsed.path:
                raise ConfigValidationError("DATABASE_URL=sqlite:// требует путь к файлу")
        elif parsed.scheme not in {"postgres", "postgresql"}:
            raise ConfigValidationError("DATABASE_URL должен быть sqlite://<path> или postgresql://user:pass@host:port/dbname")

    secret_sources = {
        'TELEGRAM_BOT_TOKEN': 'file' if os.getenv('TELEGRAM_BOT_TOKEN_FILE') and not os.getenv('TELEGRAM_BOT_TOKEN') else 'env',
        'WEBHOOK_SECRET_TOKEN': 'file' if os.getenv('WEBHOOK_SECRET_TOKEN_FILE') and not os.getenv('WEBHOOK_SECRET_TOKEN') else ('env' if os.getenv('WEBHOOK_SECRET_TOKEN') else 'unset'),
        'SERVER_API_BEARER_TOKEN': 'file' if os.getenv('SERVER_API_BEARER_TOKEN_FILE') and not os.getenv('SERVER_API_BEARER_TOKEN') else ('env' if os.getenv('SERVER_API_BEARER_TOKEN') else 'unset'),
        'SERVER_API_HMAC_SECRET': 'file' if os.getenv('SERVER_API_HMAC_SECRET_FILE') and not os.getenv('SERVER_API_HMAC_SECRET') else ('env' if os.getenv('SERVER_API_HMAC_SECRET') else 'unset'),
        'HEALTH_HTTP_TOKEN': 'file' if os.getenv('HEALTH_HTTP_TOKEN_FILE') and not os.getenv('HEALTH_HTTP_TOKEN') else ('env' if os.getenv('HEALTH_HTTP_TOKEN') else 'unset'),
        'DISCORD_BRIDGE_BEARER_TOKEN': 'file' if os.getenv('DISCORD_BRIDGE_BEARER_TOKEN_FILE') and not os.getenv('DISCORD_BRIDGE_BEARER_TOKEN') else ('env' if os.getenv('DISCORD_BRIDGE_BEARER_TOKEN') else 'unset'),
        'DISCORD_BRIDGE_HMAC_SECRET': 'file' if os.getenv('DISCORD_BRIDGE_HMAC_SECRET_FILE') and not os.getenv('DISCORD_BRIDGE_HMAC_SECRET') else ('env' if os.getenv('DISCORD_BRIDGE_HMAC_SECRET') else 'unset'),
        'BRIDGE_INBOUND_BEARER_TOKEN': 'file' if os.getenv('BRIDGE_INBOUND_BEARER_TOKEN_FILE') and not os.getenv('BRIDGE_INBOUND_BEARER_TOKEN') else ('env' if os.getenv('BRIDGE_INBOUND_BEARER_TOKEN') else 'unset'),
        'BRIDGE_INBOUND_HMAC_SECRET': 'file' if os.getenv('BRIDGE_INBOUND_HMAC_SECRET_FILE') and not os.getenv('BRIDGE_INBOUND_HMAC_SECRET') else ('env' if os.getenv('BRIDGE_INBOUND_HMAC_SECRET') else 'unset'),
        'REMOTE_LOGS_TOKEN': 'file' if os.getenv('REMOTE_LOGS_TOKEN_FILE') and not os.getenv('REMOTE_LOGS_TOKEN') else ('env' if os.getenv('REMOTE_LOGS_TOKEN') else 'unset'),
    }

    config = BotConfig(
        telegram_bot_token=token,
        telegram_bot_username=os.getenv("TELEGRAM_BOT_USERNAME", "").strip(),
        telegram_owner_ids=_parse_int_set(os.getenv("TELEGRAM_OWNER_IDS"), name="TELEGRAM_OWNER_IDS"),
        telegram_admin_ids=_parse_int_set(os.getenv("TELEGRAM_ADMIN_IDS"), name="TELEGRAM_ADMIN_IDS"),
        telegram_mod_ids=_parse_int_set(os.getenv("TELEGRAM_MOD_IDS"), name="TELEGRAM_MOD_IDS"),
        telegram_allowed_chat_ids=_parse_int_set(os.getenv("TELEGRAM_ALLOWED_CHAT_IDS"), name="TELEGRAM_ALLOWED_CHAT_IDS"),
        telegram_chat_scope=chat_scope,
        telegram_parse_mode=os.getenv("TELEGRAM_PARSE_MODE", "HTML").strip() or "HTML",
        bot_timezone=os.getenv("BOT_TIMEZONE", "Europe/Berlin").strip() or "Europe/Berlin",
        bot_mode=bot_mode,
        data_dir=_runtime_data_dir(),
        shared_dir=_runtime_shared_dir(),
        shared_dir_available=_runtime_shared_available(),
        public_http_server_url=_validate_url("PUBLIC_HTTP_SERVER_URL", _public_base_url_from_env(), required=False),
        webhook_url=_validate_url("WEBHOOK_URL", _public_base_url_from_env(), required=False),
        webhook_listen=os.getenv("WEBHOOK_LISTEN", "0.0.0.0").strip() or "0.0.0.0",
        webhook_port=_parse_int(os.getenv("WEBHOOK_PORT") or os.getenv("PORT"), name="WEBHOOK_PORT", default=8080),
        webhook_secret_token=_env_or_file("WEBHOOK_SECRET_TOKEN"),
        webhook_path_prefix=(os.getenv("WEBHOOK_PATH_PREFIX", "telegram").strip().strip("/") or "telegram"),
        auto_reconcile_webhook=_parse_bool(os.getenv("AUTO_RECONCILE_WEBHOOK"), default=True),
        health_http_listen=os.getenv("HEALTH_HTTP_LISTEN", "127.0.0.1").strip() or "127.0.0.1",
        health_http_port=_parse_int(os.getenv("HEALTH_HTTP_PORT"), name="HEALTH_HTTP_PORT", default=0),
        health_http_token=_env_or_file("HEALTH_HTTP_TOKEN"),
        health_http_minimal=_parse_bool(os.getenv("HEALTH_HTTP_MINIMAL"), default=False),
        health_http_public=_parse_bool(os.getenv("HEALTH_HTTP_PUBLIC"), default=False),
        delivery_max_concurrency=_parse_int(os.getenv("DELIVERY_MAX_CONCURRENCY"), name="DELIVERY_MAX_CONCURRENCY", default=5),
        delivery_max_per_minute=_parse_int(os.getenv("DELIVERY_MAX_PER_MINUTE"), name="DELIVERY_MAX_PER_MINUTE", default=120),
        operator_alert_cooldown_seconds=_parse_int(os.getenv("OPERATOR_ALERT_COOLDOWN_SECONDS"), name="OPERATOR_ALERT_COOLDOWN_SECONDS", default=300),
        log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO",
        log_format=log_format,
        log_file=_resolve_log_file_path(os.getenv("LOG_FILE", "logs/nmtelegrambot.log")),
        sqlite_path=_resolve_runtime_path(os.getenv("SQLITE_PATH", "storage/nmtelegrambot.db"), is_dir=False),
        database_url=database_url,
        instance_id=os.getenv("INSTANCE_ID", "").strip() or "nmtgbot-instance",
        request_timeout_seconds=_parse_float(os.getenv("REQUEST_TIMEOUT_SECONDS"), name="REQUEST_TIMEOUT_SECONDS", default=5.0),
        status_cache_seconds=_parse_float(os.getenv("STATUS_CACHE_SECONDS"), name="STATUS_CACHE_SECONDS", default=10.0),
        status_refresh_seconds=_parse_float(os.getenv("STATUS_REFRESH_SECONDS"), name="STATUS_REFRESH_SECONDS", default=30.0),
        status_retry_attempts=_parse_int(os.getenv("STATUS_RETRY_ATTEMPTS"), name="STATUS_RETRY_ATTEMPTS", default=2),
        status_retry_backoff_seconds=_parse_float(os.getenv("STATUS_RETRY_BACKOFF_SECONDS"), name="STATUS_RETRY_BACKOFF_SECONDS", default=0.75),
        command_cooldown_seconds=_parse_float(os.getenv("COMMAND_COOLDOWN_SECONDS"), name="COMMAND_COOLDOWN_SECONDS", default=2.0),
        status_command_cooldown_seconds=_parse_float(os.getenv("STATUS_COMMAND_COOLDOWN_SECONDS"), name="STATUS_COMMAND_COOLDOWN_SECONDS", default=8.0),
        broadcast_confirmation_seconds=_parse_int(os.getenv("BROADCAST_CONFIRMATION_SECONDS"), name="BROADCAST_CONFIRMATION_SECONDS", default=120),
        link_code_ttl_seconds=_parse_int(os.getenv("LINK_CODE_TTL_SECONDS"), name="LINK_CODE_TTL_SECONDS", default=900),
        cleanup_interval_seconds=_parse_int(os.getenv("CLEANUP_INTERVAL_SECONDS"), name="CLEANUP_INTERVAL_SECONDS", default=300),
        interaction_retention_days=_parse_int(os.getenv("INTERACTION_RETENTION_DAYS"), name="INTERACTION_RETENTION_DAYS", default=30),
        admin_action_retention_days=_parse_int(os.getenv("ADMIN_ACTION_RETENTION_DAYS"), name="ADMIN_ACTION_RETENTION_DAYS", default=90),
        runtime_state_retention_days=_parse_int(os.getenv("RUNTIME_STATE_RETENTION_DAYS"), name="RUNTIME_STATE_RETENTION_DAYS", default=90),
        dead_letter_retention_days=_parse_int(os.getenv("DEAD_LETTER_RETENTION_DAYS"), name="DEAD_LETTER_RETENTION_DAYS", default=30),
        link_history_retention_days=_parse_int(os.getenv("LINK_HISTORY_RETENTION_DAYS"), name="LINK_HISTORY_RETENTION_DAYS", default=90),
        security_history_retention_days=_parse_int(os.getenv("SECURITY_HISTORY_RETENTION_DAYS"), name="SECURITY_HISTORY_RETENTION_DAYS", default=30),
        scheduler_tick_seconds=_parse_int(os.getenv("SCHEDULER_TICK_SECONDS"), name="SCHEDULER_TICK_SECONDS", default=10),
        announcement_feed_interval_seconds=_parse_int(os.getenv("ANNOUNCEMENT_FEED_INTERVAL_SECONDS"), name="ANNOUNCEMENT_FEED_INTERVAL_SECONDS", default=30),
        delivery_retry_attempts=_parse_int(os.getenv("DELIVERY_RETRY_ATTEMPTS"), name="DELIVERY_RETRY_ATTEMPTS", default=3),
        delivery_retry_backoff_seconds=_parse_int(os.getenv("DELIVERY_RETRY_BACKOFF_SECONDS"), name="DELIVERY_RETRY_BACKOFF_SECONDS", default=60),
        leader_lock_ttl_seconds=_parse_int(os.getenv("LEADER_LOCK_TTL_SECONDS"), name="LEADER_LOCK_TTL_SECONDS", default=60),
        strict_api_schemas=_parse_bool(os.getenv("STRICT_API_SCHEMAS"), default=True),
        template_strict_mode=_parse_bool(os.getenv("TEMPLATE_STRICT_MODE"), default=True),
        server_status_url=_validate_url("SERVER_STATUS_URL", os.getenv("SERVER_STATUS_URL", ""), required=False),
        announcement_feed_url=_validate_url("ANNOUNCEMENT_FEED_URL", os.getenv("ANNOUNCEMENT_FEED_URL", ""), required=False),
        link_verify_url=_validate_url("LINK_VERIFY_URL", os.getenv("LINK_VERIFY_URL", ""), required=False),
        link_verify_auto_approve=_parse_bool(os.getenv("LINK_VERIFY_AUTO_APPROVE"), default=True),
        security_status_url=_validate_url("SECURITY_STATUS_URL", os.getenv("SECURITY_STATUS_URL", ""), required=False),
        security_challenges_url=_validate_url("SECURITY_CHALLENGES_URL", os.getenv("SECURITY_CHALLENGES_URL", ""), required=False),
        security_2fa_action_url=_validate_url("SECURITY_2FA_ACTION_URL", os.getenv("SECURITY_2FA_ACTION_URL", ""), required=False),
        security_recovery_url=_validate_url("SECURITY_RECOVERY_URL", os.getenv("SECURITY_RECOVERY_URL", ""), required=False),
        security_sessions_url=_validate_url("SECURITY_SESSIONS_URL", os.getenv("SECURITY_SESSIONS_URL", ""), required=False),
        security_session_action_url=_validate_url("SECURITY_SESSION_ACTION_URL", os.getenv("SECURITY_SESSION_ACTION_URL", ""), required=False),
        server_api_bearer_token=_env_or_file("SERVER_API_BEARER_TOKEN"),
        server_api_hmac_secret=_env_or_file("SERVER_API_HMAC_SECRET"),
        server_api_request_id_header=os.getenv("SERVER_API_REQUEST_ID_HEADER", "X-Request-ID").strip() or "X-Request-ID",
        templates_dir=_resolve_templates_dir(os.getenv("TEMPLATES_DIR", "templates")),
        artifact_root=_resolve_runtime_path(os.getenv("ARTIFACT_ROOT", "artifacts"), is_dir=True),
        public_site_url=_validate_url("PUBLIC_SITE_URL", os.getenv("PUBLIC_SITE_URL", ""), required=False),
        vk_url=_validate_url("VK_URL", os.getenv("VK_URL", ""), required=False),
        discord_url=_validate_url("DISCORD_URL", os.getenv("DISCORD_URL", ""), required=False),
        telegram_channel_url=_validate_url("TELEGRAM_CHANNEL_URL", os.getenv("TELEGRAM_CHANNEL_URL", ""), required=False),
        rules_url=_validate_url("RULES_URL", os.getenv("RULES_URL", ""), required=False),
        secret_sources=secret_sources,
        user_pref_default_timezone=os.getenv("USER_PREF_DEFAULT_TIMEZONE", "Europe/Berlin").strip() or "Europe/Berlin",
        strict_compatibility_gate=_parse_bool(os.getenv("STRICT_COMPATIBILITY_GATE"), default=False),
        security_nonce_ttl_seconds=_parse_int(os.getenv("SECURITY_NONCE_TTL_SECONDS"), name="SECURITY_NONCE_TTL_SECONDS", default=600),
        feed_nonce_ttl_seconds=_parse_int(os.getenv("FEED_NONCE_TTL_SECONDS"), name="FEED_NONCE_TTL_SECONDS", default=1800),
        discord_bridge_url=_validate_url("DISCORD_BRIDGE_URL", os.getenv("DISCORD_BRIDGE_URL", ""), required=False),
        discord_bridge_bearer_token=_env_or_file("DISCORD_BRIDGE_BEARER_TOKEN"),
        discord_bridge_hmac_secret=_env_or_file("DISCORD_BRIDGE_HMAC_SECRET"),
        bridge_inbound_bearer_token=_env_or_file("BRIDGE_INBOUND_BEARER_TOKEN"),
        bridge_inbound_hmac_secret=_env_or_file("BRIDGE_INBOUND_HMAC_SECRET"),
        bridge_ingress_strict_auth=_parse_bool(os.getenv("BRIDGE_INGRESS_STRICT_AUTH"), default=True),
        bridge_target_chat_ids=_parse_int_set(os.getenv("BRIDGE_TARGET_CHAT_IDS"), name="BRIDGE_TARGET_CHAT_IDS"),
        bridge_target_scope=(os.getenv("BRIDGE_TARGET_SCOPE", "all").strip() or "all").lower(),
        bridge_target_tags=_parse_str_set(os.getenv("BRIDGE_TARGET_TAGS")),
        bridge_allowed_event_types=_parse_str_set(os.getenv("BRIDGE_ALLOWED_EVENT_TYPES")),
        build_manifest_path=_DEF_ROOT / "build_info.json",
        external_api_circuit_threshold=_parse_int(os.getenv("EXTERNAL_API_CIRCUIT_THRESHOLD"), name="EXTERNAL_API_CIRCUIT_THRESHOLD", default=4),
        external_api_circuit_reset_seconds=_parse_int(os.getenv("EXTERNAL_API_CIRCUIT_RESET_SECONDS"), name="EXTERNAL_API_CIRCUIT_RESET_SECONDS", default=120),
        backup_retention_days=_parse_int(os.getenv("BACKUP_RETENTION_DAYS"), name="BACKUP_RETENTION_DAYS", default=30),
        export_retention_days=_parse_int(os.getenv("EXPORT_RETENTION_DAYS"), name="EXPORT_RETENTION_DAYS", default=30),
    )
    validate_config(config)
    return config


def validate_config(config: BotConfig) -> None:
    errors: list[str] = []
    warnings: list[str] = []
    if config.bot_mode == "webhook" and not config.webhook_url:
        errors.append("WEBHOOK_URL обязателен для BOT_MODE=webhook")
    if config.webhook_port <= 0:
        errors.append("WEBHOOK_PORT должен быть > 0")
    if config.command_cooldown_seconds < 0 or config.status_command_cooldown_seconds < 0:
        errors.append("cooldown значения не могут быть < 0")
    if config.health_http_port < 0:
        errors.append("HEALTH_HTTP_PORT не может быть < 0")
    if config.delivery_max_concurrency <= 0:
        errors.append("DELIVERY_MAX_CONCURRENCY должен быть > 0")
    if config.delivery_max_per_minute <= 0:
        errors.append("DELIVERY_MAX_PER_MINUTE должен быть > 0")
    if config.status_retry_attempts < 0:
        errors.append("STATUS_RETRY_ATTEMPTS не может быть < 0")
    if config.delivery_retry_attempts < 0:
        errors.append("DELIVERY_RETRY_ATTEMPTS не может быть < 0")
    if config.leader_lock_ttl_seconds <= 0:
        errors.append("LEADER_LOCK_TTL_SECONDS должен быть > 0")
    if config.external_api_circuit_threshold <= 0:
        errors.append("EXTERNAL_API_CIRCUIT_THRESHOLD должен быть > 0")
    if config.external_api_circuit_reset_seconds <= 0:
        errors.append("EXTERNAL_API_CIRCUIT_RESET_SECONDS должен быть > 0")
    if not config.telegram_owner_ids:
        errors.append("TELEGRAM_OWNER_IDS должен содержать хотя бы один user id")
    if config.bot_mode == "polling" and config.webhook_url:
        warnings.append("WEBHOOK_URL задан, но BOT_MODE=polling")
    if config.webhook_url and not config.webhook_url.startswith("https://"):
        warnings.append("WEBHOOK_URL должен быть HTTPS для Telegram webhook/BotHost")
    if config.bot_mode == "webhook" and config.webhook_listen in {"127.0.0.1", "localhost"} and config.health_http_port <= 0:
        warnings.append("WEBHOOK_LISTEN=127.0.0.1: для BotHost обычно нужен WEBHOOK_LISTEN=0.0.0.0")
    platform_port = os.getenv("PORT", "").strip()
    if platform_port and str(config.webhook_port) != platform_port:
        warnings.append(f"PORT={platform_port}, но WEBHOOK_PORT={config.webhook_port}; BotHost проксирует на PORT")
    if config.security_sessions_url and not config.security_status_url:
        warnings.append("SECURITY_SESSIONS_URL задан без SECURITY_STATUS_URL")
    if config.health_http_public and not config.health_http_token:
        warnings.append("HEALTH_HTTP_PUBLIC включён без HEALTH_HTTP_TOKEN")
    if config.bridge_target_scope not in {"all", "private", "groups", "current"}:
        errors.append("BRIDGE_TARGET_SCOPE должен быть one of: all/private/groups/current")
    if (config.bridge_inbound_bearer_token or config.bridge_inbound_hmac_secret) and config.health_http_port <= 0:
        warnings.append("Bridge inbound configured, but HEALTH_HTTP_PORT=0")
    if config.discord_bridge_url and not (config.discord_bridge_bearer_token or config.discord_bridge_hmac_secret or config.server_api_bearer_token or config.server_api_hmac_secret):
        warnings.append("DISCORD_BRIDGE_URL задан без DISCORD_BRIDGE_* секрета; будет использован legacy EXTERNAL_ADMIN/INBOUND secret fallback если он есть")
    if warnings:
        os.environ["NMBOT_CONFIG_WARNINGS"] = " | ".join(warnings)
    if errors:
        raise ConfigValidationError("; ".join(errors))
