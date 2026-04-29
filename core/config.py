from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable


def _csv(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class AdapterConfig:
    name: str
    enabled: bool
    token: str = ""
    extra: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Settings:
    env: str
    mode: str
    instance_id: str
    log_level: str
    runtime_dir: Path
    database_url: str
    database_sslmode: str
    redis_url: str | None
    shared_schema: str
    modules_enabled: list[str]
    plugins_enabled: list[str]
    api_bind: str
    api_port: int
    api_token: str
    event_signing_secret: str
    remote_logs_enabled: bool
    remote_logs_url: str
    remote_logs_token: str
    adapters: dict[str, AdapterConfig]

    @classmethod
    def from_env(cls) -> "Settings":
        adapters = {
            "telegram": AdapterConfig(
                "telegram",
                _bool(os.getenv("TELEGRAM_ENABLED"), True),
                os.getenv("TELEGRAM_BOT_TOKEN", ""),
                {
                    "mode": os.getenv("TELEGRAM_MODE", "polling"),
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
        return cls(
            env=os.getenv("CAJEER_BOTS_ENV", "production"),
            mode=os.getenv("CAJEER_BOTS_MODE", "all"),
            instance_id=os.getenv("CAJEER_BOTS_INSTANCE_ID", "cajeer-bots-local"),
            log_level=os.getenv("CAJEER_BOTS_LOG_LEVEL", "INFO"),
            runtime_dir=Path(os.getenv("CAJEER_BOTS_RUNTIME_DIR", "runtime")),
            database_url=os.getenv("DATABASE_URL", ""),
            database_sslmode=os.getenv("DATABASE_SSLMODE", "prefer"),
            redis_url=os.getenv("REDIS_URL") or None,
            shared_schema=os.getenv("DATABASE_SCHEMA_SHARED", "shared"),
            modules_enabled=_csv(os.getenv("MODULES_ENABLED")),
            plugins_enabled=_csv(os.getenv("PLUGINS_ENABLED")),
            api_bind=os.getenv("API_BIND", "127.0.0.1"),
            api_port=int(os.getenv("API_PORT", "8088")),
            api_token=os.getenv("API_TOKEN", ""),
            event_signing_secret=os.getenv("EVENT_SIGNING_SECRET", ""),
            remote_logs_enabled=_bool(os.getenv("REMOTE_LOGS_ENABLED"), False),
            remote_logs_url=os.getenv("REMOTE_LOGS_URL", ""),
            remote_logs_token=os.getenv("REMOTE_LOGS_TOKEN", ""),
            adapters=adapters,
        )

    def enabled_adapters(self) -> Iterable[AdapterConfig]:
        return (adapter for adapter in self.adapters.values() if adapter.enabled)

    def enabled_module_ids(self) -> set[str]:
        return set(self.modules_enabled)

    def enabled_plugin_ids(self) -> set[str]:
        return set(self.plugins_enabled)

    def safe_summary(self) -> dict[str, object]:
        return {
            "env": self.env,
            "mode": self.mode,
            "instance_id": self.instance_id,
            "log_level": self.log_level,
            "runtime_dir": str(self.runtime_dir),
            "database_url_configured": bool(self.database_url),
            "redis_url_configured": bool(self.redis_url),
            "shared_schema": self.shared_schema,
            "modules_enabled": self.modules_enabled,
            "plugins_enabled": self.plugins_enabled,
            "api_bind": self.api_bind,
            "api_port": self.api_port,
            "api_token_configured": bool(self.api_token),
            "event_signing_secret_configured": bool(self.event_signing_secret),
            "remote_logs_enabled": self.remote_logs_enabled,
            "remote_logs_url_configured": bool(self.remote_logs_url),
            "adapters": {
                name: {
                    "enabled": adapter.enabled,
                    "token_configured": bool(adapter.token),
                    "extra": {key: bool(value) for key, value in adapter.extra.items()},
                }
                for name, adapter in self.adapters.items()
            },
        }
