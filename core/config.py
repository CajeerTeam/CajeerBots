from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


class SettingsError(ValueError):
    """Ошибка чтения или проверки конфигурации окружения."""


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
    api_bind: str
    api_port: int
    api_token: str
    api_readonly_token: str
    api_metrics_token: str
    metrics_public: bool
    event_signing_secret: str
    remote_logs_enabled: bool
    remote_logs_url: str
    remote_logs_token: str
    worker_tick_seconds: int
    distributed: DistributedSettings
    adapters: dict[str, AdapterConfig]

    @classmethod
    def from_env(cls) -> "Settings":
        telegram_mode = _choice("TELEGRAM_MODE", "polling", {"polling", "webhook"})
        adapters = {
            "telegram": AdapterConfig(
                "telegram",
                _bool(os.getenv("TELEGRAM_ENABLED"), True),
                os.getenv("TELEGRAM_BOT_TOKEN", ""),
                {
                    "mode": telegram_mode,
                    "webhook_url": os.getenv("TELEGRAM_WEBHOOK_URL", ""),
                    "webhook_secret": os.getenv("TELEGRAM_WEBHOOK_SECRET", ""),
                },
            ),
            "discord": AdapterConfig(
                "discord",
                _bool(os.getenv("DISCORD_ENABLED"), True),
                os.getenv("DISCORD_TOKEN", ""),
                {
                    "application_id": os.getenv("DISCORD_APPLICATION_ID", ""),
                    "guild_id": os.getenv("DISCORD_GUILD_ID", ""),
                },
            ),
            "vkontakte": AdapterConfig(
                "vkontakte",
                _bool(os.getenv("VKONTAKTE_ENABLED"), True),
                os.getenv("VK_GROUP_TOKEN", ""),
                {
                    "group_id": os.getenv("VK_GROUP_ID", ""),
                    "api_version": os.getenv("VK_API_VERSION", "5.199"),
                },
            ),
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
        return cls(
            env=_choice("CAJEER_BOTS_ENV", "production", {"production", "staging", "development", "test"}),
            mode=_choice("CAJEER_BOTS_MODE", "local", {"local", "distributed"}),
            default_target=_choice(
                "CAJEER_BOTS_DEFAULT_TARGET",
                "all",
                {"all", "telegram", "discord", "vkontakte", "worker", "api", "bridge"},
            ),
            instance_id=os.getenv("CAJEER_BOTS_INSTANCE_ID", "cajeer-bots-local"),
            log_level=_choice("CAJEER_BOTS_LOG_LEVEL", "INFO", {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}),
            runtime_dir=Path(os.getenv("CAJEER_BOTS_RUNTIME_DIR", "runtime")),
            database_url=os.getenv("DATABASE_URL", ""),
            database_sslmode=_choice("DATABASE_SSLMODE", "prefer", {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}),
            redis_url=os.getenv("REDIS_URL") or None,
            shared_schema=os.getenv("DATABASE_SCHEMA_SHARED", "shared"),
            event_bus_backend=_choice("EVENT_BUS_BACKEND", "memory", {"memory", "postgres", "redis"}),
            local_inline_routing=_bool(os.getenv("LOCAL_INLINE_ROUTING"), True),
            bridge_routing=_bool(os.getenv("BRIDGE_ROUTING"), True),
            modules_enabled=_csv(os.getenv("MODULES_ENABLED")),
            plugins_enabled=_csv(os.getenv("PLUGINS_ENABLED")),
            api_bind=os.getenv("API_BIND", "127.0.0.1"),
            api_port=_int("API_PORT", 8088, minimum=1, maximum=65535),
            api_token=os.getenv("API_TOKEN", ""),
            api_readonly_token=os.getenv("API_TOKEN_READONLY", ""),
            api_metrics_token=os.getenv("API_TOKEN_METRICS", ""),
            metrics_public=_bool(os.getenv("METRICS_PUBLIC"), False),
            event_signing_secret=os.getenv("EVENT_SIGNING_SECRET", ""),
            remote_logs_enabled=_bool(os.getenv("REMOTE_LOGS_ENABLED"), False),
            remote_logs_url=os.getenv("REMOTE_LOGS_URL", ""),
            remote_logs_token=os.getenv("REMOTE_LOGS_TOKEN", ""),
            worker_tick_seconds=_int("WORKER_TICK_SECONDS", 30, minimum=1, maximum=3600),
            distributed=distributed,
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
        if self.event_bus_backend == "postgres" and not self.database_url:
            errors.append("EVENT_BUS_BACKEND=postgres требует DATABASE_URL")
        if self.event_bus_backend == "redis" and not self.redis_url:
            errors.append("EVENT_BUS_BACKEND=redis требует REDIS_URL")
        if self.api_port < 1 or self.api_port > 65535:
            errors.append("API_PORT должен быть числом от 1 до 65535")
        return errors

    def safe_summary(self) -> dict[str, object]:
        return {
            "env": self.env,
            "mode": self.mode,
            "default_target": self.default_target,
            "instance_id": self.instance_id,
            "log_level": self.log_level,
            "runtime_dir": str(self.runtime_dir),
            "database_url_configured": bool(self.database_url),
            "redis_url_configured": bool(self.redis_url),
            "shared_schema": self.shared_schema,
            "event_bus_backend": self.event_bus_backend,
            "local_inline_routing": self.local_inline_routing,
            "bridge_routing": self.bridge_routing,
            "modules_enabled": self.modules_enabled,
            "plugins_enabled": self.plugins_enabled,
            "api_bind": self.api_bind,
            "api_port": self.api_port,
            "api_token_configured": bool(self.api_token),
            "api_readonly_token_configured": bool(self.api_readonly_token),
            "api_metrics_token_configured": bool(self.api_metrics_token),
            "metrics_public": self.metrics_public,
            "event_signing_secret_configured": bool(self.event_signing_secret),
            "remote_logs_enabled": self.remote_logs_enabled,
            "remote_logs_url_configured": bool(self.remote_logs_url),
            "worker_tick_seconds": self.worker_tick_seconds,
            "distributed": {
                "enabled": self.distributed.enabled,
                "role": self.distributed.role,
                "core_server_url_configured": bool(self.distributed.core_server_url),
                "node_id_configured": bool(self.distributed.node_id),
                "node_secret_configured": bool(self.distributed.node_secret),
                "transport": self.distributed.transport,
                "degraded_mode_enabled": self.distributed.degraded_mode_enabled,
            },
            "adapters": {
                name: {
                    "enabled": adapter.enabled,
                    "token_configured": bool(adapter.token),
                    "extra": {key: bool(value) for key, value in adapter.extra.items()},
                }
                for name, adapter in self.adapters.items()
            },
        }
