from __future__ import annotations

import asyncio
import logging
import signal
import time
from pathlib import Path
from typing import Iterable

from core.adapters.base import AdapterContext, AdapterHealth, BotAdapter
from core.adapters.discord import DiscordAdapter
from core.adapters.telegram import TelegramAdapter
from core.adapters.vkontakte import VkontakteAdapter
from core.bridge import BridgeService
from core.commands import CommandRegistry, build_default_commands
from core.compatibility import check_compatibility
from core.config import Settings
from core.db import Database
from core.dead_letters import DeadLetterQueue
from core.delivery import DeliveryService
from core.event_bus import build_event_bus
from core.events import EVENT_CONTRACT_VERSION
from core.idempotency import IdempotencyStore
from core.registry import Registry
from core.router import EventRouter
from core.worker import WorkerService

logger = logging.getLogger(__name__)

ADAPTER_CLASSES: dict[str, type[BotAdapter]] = {
    "telegram": TelegramAdapter,
    "discord": DiscordAdapter,
    "vkontakte": VkontakteAdapter,
}

PLACEHOLDER_SECRETS = {
    "change-me",
    "change-me-admin-token",
    "change-me-long-random-secret",
    "",
}

FORBIDDEN_TERMS = ["Never" + "Mine", "cajeer" + "_bots", "cajeer" + "_core", "nm" + "bot"]
TEXT_EXTENSIONS = {".py", ".md", ".json", ".yaml", ".yml", ".toml", ".sh", ".service", ".conf", ".example"}


class Runtime:
    def __init__(self, settings: Settings, project_root: Path | None = None) -> None:
        self.settings = settings
        self.project_root = project_root or Path.cwd()
        self.registry = Registry(self.project_root)
        self.adapters: list[BotAdapter] = []
        self.event_bus = build_event_bus(settings)
        self.dead_letters = DeadLetterQueue()
        self.delivery = DeliveryService()
        self.idempotency = IdempotencyStore()
        self.commands: CommandRegistry = build_default_commands(self)
        self.router = EventRouter(self.commands)
        self.bridge = BridgeService(self)
        self.worker = WorkerService(self)
        self._stop_event: asyncio.Event | None = None
        self.version = self._read_version()
        self.event_contract_version = EVENT_CONTRACT_VERSION
        self.started_at = time.time()

    def _read_version(self) -> str:
        path = self.project_root / "VERSION"
        if path.exists():
            return path.read_text(encoding="utf-8").strip()
        return "0.0.0"

    def build_adapter(self, name: str) -> BotAdapter:
        context = AdapterContext(self.event_bus, self.router, self.dead_letters)
        return ADAPTER_CLASSES[name](self.settings, self.settings.adapters[name], context=context)

    def selected_adapters(self, mode: str) -> list[str]:
        if mode == "all":
            return [adapter.name for adapter in self.settings.enabled_adapters()]
        if mode in ADAPTER_CLASSES:
            return [mode]
        return []

    def adapter_health_snapshot(self) -> list[AdapterHealth]:
        return [
            AdapterHealth(
                name=adapter.name,
                enabled=adapter.config.enabled,
                configured=bool(adapter.config.token),
                state=adapter.status.state,
                capabilities=adapter.capabilities.names(),
                started_at=adapter.status.started_at,
                last_event_at=adapter.status.last_event_at,
                last_error=adapter.status.last_error,
                processed_events=adapter.status.processed_events,
                failed_events=adapter.status.failed_events,
            )
            for adapter in self.adapters
        ]

    async def run(self, mode: str) -> None:
        logger.info("запуск Cajeer Bots, режим=%s", mode)
        self.settings.runtime_dir.mkdir(parents=True, exist_ok=True)

        if mode == "api":
            return await self.run_api()
        if mode == "worker":
            return await self.run_worker()
        if mode == "bridge":
            return await self.run_bridge()

        names = self.selected_adapters(mode)
        if not names:
            raise ValueError(f"неподдерживаемый режим: {mode}")
        self.adapters = [self.build_adapter(name) for name in names]
        await self._run_supervised(self.adapters)

    async def _run_supervised(self, adapters: Iterable[BotAdapter]) -> None:
        self._stop_event = asyncio.Event()
        self._install_signal_handlers(self._stop_event)
        tasks = [asyncio.create_task(adapter.start(), name=f"adapter:{adapter.name}") for adapter in adapters]
        stop_task = asyncio.create_task(self._stop_event.wait(), name="runtime:stop")
        try:
            done, _ = await asyncio.wait([*tasks, stop_task], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stop_task:
                    exc = task.exception()
                    if exc:
                        logger.error("адаптер завершился с ошибкой: %s", exc)
                        self._stop_event.set()
            if stop_task in done:
                logger.info("получен сигнал остановки")
        finally:
            await self._stop_adapters(tasks)
            stop_task.cancel()

    async def _stop_adapters(self, tasks: list[asyncio.Task[None]]) -> None:
        for adapter in self.adapters:
            await adapter.stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _install_signal_handlers(self, stop_event: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except (NotImplementedError, RuntimeError):
                pass

    async def run_api(self) -> None:
        from core.api import ApiServer

        logger.info("режим API запущен на %s:%s", self.settings.api_bind, self.settings.api_port)
        stop_event = asyncio.Event()
        self._install_signal_handlers(stop_event)
        server = ApiServer(self)
        server.start_in_thread()
        try:
            await stop_event.wait()
        finally:
            server.stop()

    async def run_worker(self) -> None:
        stop_event = asyncio.Event()
        self._install_signal_handlers(stop_event)
        await self.worker.run(stop_event)

    async def run_bridge(self) -> None:
        stop_event = asyncio.Event()
        self._install_signal_handlers(stop_event)
        await self.bridge.run(stop_event)

    def readiness_snapshot(self) -> dict[str, object]:
        problems: list[str] = []
        problems.extend(self.registry.validate(settings=self.settings))
        compat = check_compatibility(self.project_root, self.version)
        problems.extend(compat.errors)
        if self.settings.api_token in PLACEHOLDER_SECRETS:
            problems.append("API_TOKEN содержит демонстрационное значение")
        if self.settings.event_signing_secret in PLACEHOLDER_SECRETS:
            problems.append("EVENT_SIGNING_SECRET содержит демонстрационное значение")
        return {
            "ok": not problems,
            "problems": problems,
            "event_bus": self.event_bus.metrics().to_dict(),
            "registry": {
                "adapters": len(self.registry.adapters()),
                "modules": len(self.registry.modules()),
                "plugins": len(self.registry.plugins()),
            },
        }

    def metrics_text(self) -> str:
        metrics = self.event_bus.metrics()
        uptime = max(0, int(time.time() - self.started_at))
        lines = [
            "# HELP cajeerbots_runtime_uptime_seconds Время работы процесса Cajeer Bots.",
            "# TYPE cajeerbots_runtime_uptime_seconds gauge",
            f"cajeerbots_runtime_uptime_seconds {uptime}",
            "# HELP cajeerbots_events_total Количество опубликованных событий.",
            "# TYPE cajeerbots_events_total counter",
            f'cajeerbots_events_total{{backend="{metrics.backend}"}} {metrics.published}',
            "# HELP cajeerbots_events_failed_total Количество ошибок публикации событий.",
            "# TYPE cajeerbots_events_failed_total counter",
            f'cajeerbots_events_failed_total{{backend="{metrics.backend}"}} {metrics.failed}',
            "# HELP cajeerbots_registry_modules_total Количество зарегистрированных модулей.",
            "# TYPE cajeerbots_registry_modules_total gauge",
            f"cajeerbots_registry_modules_total {len(self.registry.modules())}",
            "# HELP cajeerbots_registry_plugins_total Количество зарегистрированных плагинов.",
            "# TYPE cajeerbots_registry_plugins_total gauge",
            f"cajeerbots_registry_plugins_total {len(self.registry.plugins())}",
            "# HELP cajeerbots_dead_letters_total Количество dead letter событий.",
            "# TYPE cajeerbots_dead_letters_total gauge",
            f"cajeerbots_dead_letters_total {self.dead_letters.count()}",
        ]
        for adapter in self.adapter_health_snapshot():
            state_value = 1 if adapter.state == "running" else 0
            lines.append(f'cajeerbots_adapter_state{{adapter="{adapter.name}",state="{adapter.state}"}} {state_value}')
            lines.append(f'cajeerbots_adapter_events_total{{adapter="{adapter.name}"}} {adapter.processed_events}')
            lines.append(f'cajeerbots_adapter_events_failed_total{{adapter="{adapter.name}"}} {adapter.failed_events}')
        return "\n".join(lines) + "\n"

    def doctor(self, offline: bool = False) -> list[str]:
        problems: list[str] = []
        warnings: list[str] = []
        if not self.settings.event_signing_secret:
            problems.append("EVENT_SIGNING_SECRET не задан")
        if self.settings.event_signing_secret in PLACEHOLDER_SECRETS:
            problems.append("EVENT_SIGNING_SECRET содержит демонстрационное значение")
        if self.settings.api_token in PLACEHOLDER_SECRETS:
            problems.append("API_TOKEN содержит демонстрационное значение")
        if not (self.project_root / "core").is_dir():
            problems.append("каталог core не найден")
        if not (self.project_root / "bots").is_dir():
            problems.append("каталог bots не найден")
        if (self.project_root / "migrations").exists():
            problems.append("каталог migrations не должен входить в проект")
        if self.settings.event_bus_backend == "postgres" and not self.settings.database_url:
            problems.append("EVENT_BUS_BACKEND=postgres требует DATABASE_URL")
        if self.settings.event_bus_backend == "redis" and not self.settings.redis_url:
            problems.append("EVENT_BUS_BACKEND=redis требует REDIS_URL")
        problems.extend(self.registry.validate(settings=self.settings))
        problems.extend(self._check_executable_bits())
        problems.extend(self._check_forbidden_terms())
        compat = check_compatibility(self.project_root, self.version)
        problems.extend(compat.errors)
        warnings.extend(compat.warnings)
        for warning in warnings:
            logger.warning(warning)
        if not offline:
            if not self.settings.database_url:
                problems.append("DATABASE_URL не задан")
            else:
                try:
                    Database(self.settings.database_url, self.settings.database_sslmode).ping()
                except Exception as exc:
                    problems.append(f"проверка PostgreSQL завершилась ошибкой: {exc}")
            for name, adapter in self.settings.adapters.items():
                if adapter.enabled and not adapter.token:
                    problems.append(f"адаптер {name} включён, но его токен не задан")
        return problems

    def _check_executable_bits(self) -> list[str]:
        errors: list[str] = []
        for path in [
            self.project_root / "run.sh",
            self.project_root / "install.sh",
            self.project_root / "setup_wizard.py",
            *(self.project_root / "scripts").glob("*.sh"),
        ]:
            if path.exists() and not path.stat().st_mode & 0o111:
                errors.append(f"файл должен быть исполняемым: {path.relative_to(self.project_root)}")
        return errors

    def _check_forbidden_terms(self) -> list[str]:
        errors: list[str] = []
        ignored_dirs = {".git", "dist", "runtime", "__pycache__", ".pytest_cache"}
        for path in self.project_root.rglob("*"):
            if not path.is_file() or any(part in ignored_dirs for part in path.parts):
                continue
            if path.suffix not in TEXT_EXTENSIONS and path.name not in {"Dockerfile", "Makefile", ".env.example"}:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for term in FORBIDDEN_TERMS:
                if term in text:
                    errors.append(f"запрещённый термин {term!r} найден в {path.relative_to(self.project_root)}")
        return errors
