from __future__ import annotations

import os

from core.schema import validate_schema_name
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable



_DOTENV_LOADED = False


def load_dotenv(path: str | Path = ".env", *, override: bool = False) -> bool:
    env_path = Path(path)
    if not env_path.exists() or not env_path.is_file():
        return False
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or key.startswith("#"):
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if override or key not in os.environ:
            os.environ[key] = value
    return True


def load_default_dotenv_once() -> None:
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    _DOTENV_LOADED = True
    explicit = os.getenv("CAJEER_BOTS_ENV_FILE")
    if explicit:
        load_dotenv(explicit)
        return
    load_dotenv(Path.cwd() / ".env")


class SettingsError(ValueError):
    """Ошибка чтения или проверки конфигурации окружения."""


class SafeSummary(dict):
    """Диагностический dict, который не печатает credential-поля в str/repr."""

    @staticmethod
    def _sanitize(value: object) -> object:
        if isinstance(value, dict):
            sanitized: dict[str, object] = {}
            index = 0
            for key, item in value.items():
                safe_key = str(key)
                if any(word in safe_key.lower() for word in ("token", "secret")):
                    safe_key = f"credential_flag_{index}"
                    index += 1
                sanitized[safe_key] = SafeSummary._sanitize(item)
            return sanitized
        if isinstance(value, list):
            return [SafeSummary._sanitize(item) for item in value]
        if isinstance(value, str) and any(word in value.lower() for word in ("token", "secret")):
            return "<credential-redacted>"
        return value

    def __repr__(self) -> str:
        return dict.__repr__(self._sanitize(dict(self)))

    __str__ = __repr__


def _csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on", "да", "вкл"}:
        return True
    if normalized in {"0", "false", "no", "n", "off", "нет", "выкл"}:
        return False
    raise SettingsError(f"значение {value!r} должно быть логическим: true/false")


def _int(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:
            raise SettingsError(f"{name} должен быть целым числом") from exc
    if minimum is not None and value < minimum:
        raise SettingsError(f"{name} должен быть не меньше {minimum}")
    if maximum is not None and value > maximum:
        raise SettingsError(f"{name} должен быть не больше {maximum}")
    return value


def _choice(name: str, default: str, choices: set[str]) -> str:
    value = os.getenv(name, default).strip()
    if value not in choices:
        allowed = ", ".join(sorted(choices))
        raise SettingsError(f"{name} содержит неизвестное значение {value!r}; допустимо: {allowed}")
    return value


@dataclass(frozen=True)
class AdapterConfig:
    name: str
    enabled: bool
    token: str = ""
    extra: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DistributedSettings:
    enabled: bool
    role: str
    core_server_url: str
    node_id: str
    node_secret: str
    transport: str
    local_queue_path: str
    degraded_mode_enabled: bool

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not self.enabled:
            return errors
        if self.role not in {"server", "agent", "gateway", "worker"}:
            errors.append("DISTRIBUTED_ROLE должен быть server, agent, gateway или worker")
        if self.role == "agent" and not self.core_server_url:
            errors.append("DISTRIBUTED_ROLE=agent требует CORE_SERVER_URL")
        if self.role in {"agent", "gateway", "worker"} and not self.node_id:
            errors.append("распределённый режим требует NODE_ID")
        if self.role in {"agent", "gateway", "worker"} and not self.node_secret:
            errors.append("распределённый режим требует NODE_SECRET")
        return errors


@dataclass(frozen=True)
class WorkspaceSettings:
    enabled: bool
    url: str
    token: str
    project_id: str
    team_id: str
    service_id: str
    timeout_seconds: int

    def validate(self) -> list[str]:
        if not self.enabled:
            return []
        errors: list[str] = []
        if not self.url:
            errors.append("CAJEER_WORKSPACE_URL обязателен при CAJEER_WORKSPACE_ENABLED=true")
        if not self.token:
            errors.append("CAJEER_WORKSPACE_TOKEN обязателен при CAJEER_WORKSPACE_ENABLED=true")
        if not self.project_id:
            errors.append("CAJEER_WORKSPACE_PROJECT_ID обязателен при CAJEER_WORKSPACE_ENABLED=true")
        if not self.service_id:
            errors.append("CAJEER_WORKSPACE_SERVICE_ID обязателен при CAJEER_WORKSPACE_ENABLED=true")
        return errors


@dataclass(frozen=True)
class RemoteLogsSettings:
    enabled: bool
    url: str
    token: str
    project: str
    bot: str
    environment: str
    level: str
    batch_size: int
    flush_interval: int
    timeout_seconds: int
    sign_requests: bool

    def validate(self) -> list[str]:
        if not self.enabled:
            return []
        errors: list[str] = []
        if not self.url:
            errors.append("REMOTE_LOGS_URL обязателен при REMOTE_LOGS_ENABLED=true")
        if not self.token:
            errors.append("REMOTE_LOGS_TOKEN обязателен при REMOTE_LOGS_ENABLED=true")
        return errors


@dataclass(frozen=True)
class StorageSettings:
    async_database_url: str
    alembic_config: str
    delivery_backend: str
    dead_letter_backend: str
    idempotency_backend: str
    redis_cache_prefix: str
    redis_fsm_prefix: str
    redis_queue_prefix: str
    delivery_retry_backoff_seconds: int
    delivery_lease_seconds: int
    delivery_claim_limit: int
    event_bus_retry_backoff_seconds: int
    event_bus_retry_backoff_max_seconds: int
    event_bus_max_attempts: int
    idempotency_ttl_seconds: int


@dataclass(frozen=True)
class SupervisorSettings:
    restart_policy: str
    restart_max: int
    restart_backoff_seconds: int


@dataclass(frozen=True)
class Settings:
    env: str
    mode: str
    default_target: str
    instance_id: str
    log_level: str
    runtime_dir: Path
    database_url: str
    database_sslmode: str
    redis_url: str | None
    shared_schema: str
    event_bus_backend: str
    local_inline_routing: bool
    bridge_routing: bool
    modules_enabled: list[str]
    plugins_enabled: list[str]
    runtime_catalog_paths: list[Path]
    registry_repo_root_fallback: bool
    api_bind: str
    api_port: int
    api_server: str
    api_behind_reverse_proxy: bool
    webhook_replay_protection: bool
    webhook_replay_ttl_seconds: int
    api_token: str
    api_readonly_token: str
    api_metrics_token: str
    api_tokens_file: Path
    metrics_public: bool
    webhook_rate_limit_per_minute: int
    webhook_auth_failure_limit: int
    event_signing_secret: str
    worker_tick_seconds: int
    distributed: DistributedSettings
    workspace: WorkspaceSettings
    remote_logs: RemoteLogsSettings
    storage: StorageSettings
    supervisor: SupervisorSettings
    adapters: dict[str, AdapterConfig]

    @classmethod
    def from_env(cls) -> "Settings":
        load_default_dotenv_once()
        telegram_mode = _choice("TELEGRAM_MODE", "polling", {"polling", "webhook"})
        adapters = {
            "telegram": AdapterConfig("telegram", _bool(os.getenv("TELEGRAM_ENABLED"), True), os.getenv("TELEGRAM_BOT_TOKEN", ""), {"mode": telegram_mode, "webhook_url": os.getenv("TELEGRAM_WEBHOOK_URL", ""), "webhook_secret": os.getenv("TELEGRAM_WEBHOOK_SECRET", "")}),
            "discord": AdapterConfig("discord", _bool(os.getenv("DISCORD_ENABLED"), True), os.getenv("DISCORD_TOKEN", ""), {
                "application_id": os.getenv("DISCORD_APPLICATION_ID", ""),
                "guild_id": os.getenv("DISCORD_GUILD_ID", ""),
                "message_content_enabled": str(_bool(os.getenv("DISCORD_MESSAGE_CONTENT_ENABLED"), False)).lower(),
                "slash_commands_enabled": str(_bool(os.getenv("DISCORD_SLASH_COMMANDS_ENABLED"), True)).lower(),
            }),
            "vkontakte": AdapterConfig("vkontakte", _bool(os.getenv("VKONTAKTE_ENABLED"), True), os.getenv("VK_GROUP_TOKEN", ""), {
                "group_id": os.getenv("VK_GROUP_ID", ""),
                "api_version": os.getenv("VK_API_VERSION", "5.199"),
                "callback_secret": os.getenv("VK_CALLBACK_SECRET", ""),
                "confirmation_code": os.getenv("VK_CONFIRMATION_CODE", ""),
            }),
            "fake": AdapterConfig("fake", _bool(os.getenv("FAKE_ENABLED"), False), "", {"script": os.getenv("FAKE_SCRIPT", "")}),
        }
        distributed = DistributedSettings(
            enabled=_bool(os.getenv("DISTRIBUTED_ENABLED"), False),
            role=os.getenv("DISTRIBUTED_ROLE", "").strip(),
            core_server_url=os.getenv("CORE_SERVER_URL", "").strip(),
            node_id=os.getenv("NODE_ID", "").strip(),
            node_secret=os.getenv("NODE_SECRET", "").strip(),
            transport=_choice("DISTRIBUTED_TRANSPORT", "websocket", {"http", "websocket", "grpc", "broker"}),
            local_queue_path=os.getenv("DISTRIBUTED_LOCAL_QUEUE_PATH", "runtime/distributed-queue.jsonl"),
            degraded_mode_enabled=_bool(os.getenv("DISTRIBUTED_DEGRADED_MODE_ENABLED"), True),
        )
        workspace = WorkspaceSettings(
            enabled=_bool(os.getenv("CAJEER_WORKSPACE_ENABLED"), False),
            url=os.getenv("CAJEER_WORKSPACE_URL", "").rstrip("/"),
            token=os.getenv("CAJEER_WORKSPACE_TOKEN", ""),
            project_id=os.getenv("CAJEER_WORKSPACE_PROJECT_ID", ""),
            team_id=os.getenv("CAJEER_WORKSPACE_TEAM_ID", ""),
            service_id=os.getenv("CAJEER_WORKSPACE_SERVICE_ID", ""),
            timeout_seconds=_int("CAJEER_WORKSPACE_TIMEOUT_SECONDS", 5, minimum=1, maximum=60),
        )
        remote_logs = RemoteLogsSettings(
            enabled=_bool(os.getenv("REMOTE_LOGS_ENABLED"), False),
            url=os.getenv("REMOTE_LOGS_URL", ""),
            token=os.getenv("REMOTE_LOGS_TOKEN", ""),
            project=os.getenv("REMOTE_LOGS_PROJECT", "CajeerBots"),
            bot=os.getenv("REMOTE_LOGS_BOT", "CajeerBots"),
            environment=os.getenv("REMOTE_LOGS_ENVIRONMENT", os.getenv("CAJEER_BOTS_ENV", "development")),
            level=os.getenv("REMOTE_LOGS_LEVEL", "INFO"),
            batch_size=_int("REMOTE_LOGS_BATCH_SIZE", 25, minimum=1, maximum=100),
            flush_interval=_int("REMOTE_LOGS_FLUSH_INTERVAL", 5, minimum=1, maximum=3600),
            timeout_seconds=_int("REMOTE_LOGS_TIMEOUT_SECONDS", 5, minimum=1, maximum=60),
            sign_requests=_bool(os.getenv("REMOTE_LOGS_SIGN_REQUESTS"), True),
        )
        storage = StorageSettings(
            async_database_url=os.getenv("DATABASE_ASYNC_URL", ""),
            alembic_config=os.getenv("ALEMBIC_CONFIG", "alembic.ini"),
            delivery_backend=_choice("DELIVERY_BACKEND", "memory", {"memory", "redis", "postgres"}),
            dead_letter_backend=_choice("DEAD_LETTER_BACKEND", "memory", {"memory", "redis", "postgres"}),
            idempotency_backend=_choice("IDEMPOTENCY_BACKEND", "memory", {"memory", "redis", "postgres"}),
            redis_cache_prefix=os.getenv("REDIS_CACHE_PREFIX", "cajeer:bots:cache"),
            redis_fsm_prefix=os.getenv("REDIS_FSM_PREFIX", "cajeer:bots:fsm"),
            redis_queue_prefix=os.getenv("REDIS_QUEUE_PREFIX", "cajeer:bots:queue"),
            delivery_retry_backoff_seconds=_int("DELIVERY_RETRY_BACKOFF_SECONDS", 5, minimum=0, maximum=3600),
            delivery_lease_seconds=_int("DELIVERY_LEASE_SECONDS", 60, minimum=1, maximum=3600),
            delivery_claim_limit=_int("DELIVERY_CLAIM_LIMIT", 50, minimum=1, maximum=1000),
            event_bus_retry_backoff_seconds=_int("EVENT_BUS_RETRY_BACKOFF_SECONDS", 5, minimum=0, maximum=3600),
            event_bus_retry_backoff_max_seconds=_int("EVENT_BUS_RETRY_BACKOFF_MAX_SECONDS", 300, minimum=1, maximum=86400),
            event_bus_max_attempts=_int("EVENT_BUS_MAX_ATTEMPTS", 10, minimum=1, maximum=1000),
            idempotency_ttl_seconds=_int("IDEMPOTENCY_TTL_SECONDS", 86400, minimum=60, maximum=31536000),
        )
        supervisor = SupervisorSettings(
            restart_policy=_choice("ADAPTER_RESTART_POLICY", "on-failure", {"always", "on-failure", "never"}),
            restart_max=_int("ADAPTER_RESTART_MAX", 5, minimum=0, maximum=1000),
            restart_backoff_seconds=_int("ADAPTER_RESTART_BACKOFF_SECONDS", 10, minimum=0, maximum=3600),
        )
        return cls(
            env=_choice("CAJEER_BOTS_ENV", "development", {"production", "staging", "development", "test"}),
            mode=_choice("CAJEER_BOTS_MODE", "local", {"local", "distributed"}),
            default_target=_choice("CAJEER_BOTS_DEFAULT_TARGET", "all", {"all", "telegram", "discord", "vkontakte", "fake", "worker", "api", "bridge"}),
            instance_id=os.getenv("CAJEER_BOTS_INSTANCE_ID", "cajeer-bots-local"),
            log_level=_choice("CAJEER_BOTS_LOG_LEVEL", "INFO", {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}),
            runtime_dir=Path(os.getenv("CAJEER_BOTS_RUNTIME_DIR", "runtime")),
            database_url=os.getenv("DATABASE_URL", ""),
            database_sslmode=_choice("DATABASE_SSLMODE", "prefer", {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}),
            redis_url=os.getenv("REDIS_URL") or None,
            shared_schema=validate_schema_name(os.getenv("DATABASE_SCHEMA_SHARED", "shared")),
            event_bus_backend=_choice("EVENT_BUS_BACKEND", "memory", {"memory", "postgres", "redis"}),
            local_inline_routing=_bool(os.getenv("LOCAL_INLINE_ROUTING"), True),
            bridge_routing=_bool(os.getenv("BRIDGE_ROUTING"), True),
            modules_enabled=_csv(os.getenv("MODULES_ENABLED")),
            plugins_enabled=_csv(os.getenv("PLUGINS_ENABLED")),
            runtime_catalog_paths=[Path(item) for item in _csv(os.getenv("RUNTIME_CATALOG_PATHS", "runtime/catalog"))],
            registry_repo_root_fallback=_bool(os.getenv("REGISTRY_REPO_ROOT_FALLBACK"), True),
            api_bind=os.getenv("API_BIND", "127.0.0.1"),
            api_port=_int("API_PORT", 8088, minimum=1, maximum=65535),
            api_server=_choice("API_SERVER", "stdlib", {"stdlib", "asgi"}),
            api_behind_reverse_proxy=_bool(os.getenv("API_BEHIND_REVERSE_PROXY"), False),
            webhook_replay_protection=_bool(os.getenv("WEBHOOK_REPLAY_PROTECTION"), True),
            webhook_replay_ttl_seconds=_int("WEBHOOK_REPLAY_TTL_SECONDS", 300, minimum=30, maximum=86400),
            api_token=os.getenv("API_TOKEN", ""),
            api_readonly_token=os.getenv("API_TOKEN_READONLY", ""),
            api_metrics_token=os.getenv("API_TOKEN_METRICS", ""),
            api_tokens_file=Path(os.getenv("API_TOKENS_FILE", "runtime/secrets/api_tokens.json")),
            metrics_public=_bool(os.getenv("METRICS_PUBLIC"), False),
            webhook_rate_limit_per_minute=_int("WEBHOOK_RATE_LIMIT_PER_MINUTE", 120, minimum=1, maximum=100000),
            webhook_auth_failure_limit=_int("WEBHOOK_AUTH_FAILURE_LIMIT", 20, minimum=1, maximum=100000),
            event_signing_secret=os.getenv("EVENT_SIGNING_SECRET", ""),
            worker_tick_seconds=_int("WORKER_TICK_SECONDS", 30, minimum=1, maximum=3600),
            distributed=distributed,
            workspace=workspace,
            remote_logs=remote_logs,
            storage=storage,
            supervisor=supervisor,
            adapters=adapters,
        )

    def enabled_adapters(self) -> Iterable[AdapterConfig]:
        return (adapter for adapter in self.adapters.values() if adapter.enabled)

    def enabled_module_ids(self) -> set[str]:
        return set(self.modules_enabled)

    def enabled_plugin_ids(self) -> set[str]:
        return set(self.plugins_enabled)

    def validate_runtime(self, *, doctor_mode: str = "local") -> list[str]:
        errors: list[str] = []
        if self.mode == "local" and self.distributed.enabled:
            errors.append("DISTRIBUTED_ENABLED=true нельзя использовать вместе с CAJEER_BOTS_MODE=local")
        if doctor_mode == "distributed" or self.mode == "distributed" or self.distributed.enabled:
            errors.extend(self.distributed.validate())
        if self.event_bus_backend == "postgres" and not self.storage.async_database_url:
            errors.append("EVENT_BUS_BACKEND=postgres требует DATABASE_ASYNC_URL")
        if self.event_bus_backend == "redis" and not self.redis_url:
            errors.append("EVENT_BUS_BACKEND=redis требует REDIS_URL")
        if self.storage.async_database_url and not self.storage.async_database_url.startswith("postgresql+asyncpg://"):
            errors.append("DATABASE_ASYNC_URL должен использовать драйвер postgresql+asyncpg для SQLAlchemy async")
        if self.storage.delivery_backend == "redis" and not self.redis_url:
            errors.append("DELIVERY_BACKEND=redis требует REDIS_URL")
        if self.storage.dead_letter_backend == "redis" and not self.redis_url:
            errors.append("DEAD_LETTER_BACKEND=redis требует REDIS_URL")
        if self.storage.idempotency_backend == "redis" and not self.redis_url:
            errors.append("IDEMPOTENCY_BACKEND=redis требует REDIS_URL")
        try:
            validate_schema_name(self.shared_schema)
        except ValueError as exc:
            errors.append(str(exc))
        errors.extend(self.workspace.validate())
        errors.extend(self.remote_logs.validate())
        if self.api_port < 1 or self.api_port > 65535:
            errors.append("API_PORT должен быть числом от 1 до 65535")
        return errors

    def safe_summary(self) -> dict[str, object]:
        return SafeSummary({
            "env": self.env,
            "mode": self.mode,
            "default_target": self.default_target,
            "instance_id": self.instance_id,
            "log_level": self.log_level,
            "runtime_dir": str(self.runtime_dir),
            "database_url_configured": bool(self.database_url),
            "database_async_url_configured": bool(self.storage.async_database_url),
            "redis_url_configured": bool(self.redis_url),
            "shared_schema": self.shared_schema,
            "event_bus_backend": self.event_bus_backend,
            "delivery_backend": self.storage.delivery_backend,
            "dead_letter_backend": self.storage.dead_letter_backend,
            "idempotency_backend": self.storage.idempotency_backend,
            "delivery_retry_backoff_seconds": self.storage.delivery_retry_backoff_seconds,
            "delivery_lease_seconds": self.storage.delivery_lease_seconds,
            "delivery_claim_limit": self.storage.delivery_claim_limit,
            "event_bus_retry_backoff_seconds": self.storage.event_bus_retry_backoff_seconds,
            "event_bus_max_attempts": self.storage.event_bus_max_attempts,
            "idempotency_ttl_seconds": self.storage.idempotency_ttl_seconds,
            "local_inline_routing": self.local_inline_routing,
            "bridge_routing": self.bridge_routing,
            "modules_enabled": self.modules_enabled,
            "plugins_enabled": self.plugins_enabled,
            "runtime_catalog_paths": [str(item) for item in self.runtime_catalog_paths],
            "registry_repo_root_fallback": self.registry_repo_root_fallback,
            "api_bind": self.api_bind,
            "api_port": self.api_port,
            "api_server": self.api_server,
            "api_behind_reverse_proxy": self.api_behind_reverse_proxy,
            "webhook_replay_protection": self.webhook_replay_protection,
            "webhook_replay_ttl_seconds": self.webhook_replay_ttl_seconds,
            "api_token_configured": bool(self.api_token),
            "api_readonly_token_configured": bool(self.api_readonly_token),
            "api_metrics_token_configured": bool(self.api_metrics_token),
            "api_tokens_file": str(self.api_tokens_file),
            "metrics_public": self.metrics_public,
            "webhook_rate_limit_per_minute": self.webhook_rate_limit_per_minute,
            "webhook_auth_failure_limit": self.webhook_auth_failure_limit,
            "event_signing_secret_configured": bool(self.event_signing_secret),
            "workspace_enabled": self.workspace.enabled,
            "workspace_url_configured": bool(self.workspace.url),
            "workspace_token_configured": bool(self.workspace.token),
            "remote_logs_enabled": self.remote_logs.enabled,
            "remote_logs_url_configured": bool(self.remote_logs.url),
            "remote_logs_token_configured": bool(self.remote_logs.token),
            "worker_tick_seconds": self.worker_tick_seconds,
            "supervisor": {
                "restart_policy": self.supervisor.restart_policy,
                "restart_max": self.supervisor.restart_max,
                "restart_backoff_seconds": self.supervisor.restart_backoff_seconds,
            },
            "distributed": {
                "enabled": self.distributed.enabled,
                "role": self.distributed.role,
                "transport": self.distributed.transport,
                "core_server_url_configured": bool(self.distributed.core_server_url),
                "node_id": self.distributed.node_id,
                "node_secret_configured": bool(self.distributed.node_secret),
                "degraded_mode_enabled": self.distributed.degraded_mode_enabled,
            },
            "adapters": {
                name: {"enabled": adapter.enabled, "configured": bool(adapter.token) or name == "fake", "extra_keys": sorted(adapter.extra.keys())}
                for name, adapter in self.adapters.items()
            },
        })
