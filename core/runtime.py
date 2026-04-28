from __future__ import annotations
import asyncio, logging
from pathlib import Path
from cajeer_bots.adapters.base import BotAdapter
from cajeer_bots.adapters.discord import DiscordAdapter
from cajeer_bots.adapters.telegram import TelegramAdapter
from cajeer_bots.adapters.vkontakte import VkontakteAdapter
from cajeer_bots.config import Settings
from cajeer_bots.db import Database, MigrationManager
from cajeer_bots.registry import Registry

logger = logging.getLogger(__name__)
ADAPTER_CLASSES: dict[str, type[BotAdapter]] = {
    "telegram": TelegramAdapter,
    "discord": DiscordAdapter,
    "vkontakte": VkontakteAdapter,
}

class Runtime:
    def __init__(self, settings: Settings, project_root: Path | None = None) -> None:
        self.settings = settings
        self.project_root = project_root or Path.cwd()
        self.registry = Registry(self.project_root)
        self.adapters: list[BotAdapter] = []

    def build_adapter(self, name: str) -> BotAdapter:
        return ADAPTER_CLASSES[name](self.settings, self.settings.adapters[name])

    def selected_adapters(self, mode: str) -> list[str]:
        if mode == "all":
            return [adapter.name for adapter in self.settings.enabled_adapters()]
        if mode in ADAPTER_CLASSES:
            return [mode]
        return []

    async def run(self, mode: str) -> None:
        logger.info("Cajeer Bots runtime starting mode=%s", mode)
        self.settings.runtime_dir.mkdir(parents=True, exist_ok=True)
        if mode == "api":
            return await self.run_api()
        if mode == "worker":
            return await self.run_worker()
        if mode == "bridge":
            return await self.run_bridge()
        names = self.selected_adapters(mode)
        if not names:
            raise ValueError(f"Unsupported mode: {mode}")
        self.adapters = [self.build_adapter(name) for name in names]
        await asyncio.gather(*(adapter.start() for adapter in self.adapters))

    async def run_api(self) -> None:
        logger.info("API mode placeholder started on %s:%s", self.settings.api_bind, self.settings.api_port)
        while True:
            await asyncio.sleep(10)

    async def run_worker(self) -> None:
        logger.info("Worker mode placeholder started")
        while True:
            await asyncio.sleep(10)

    async def run_bridge(self) -> None:
        logger.info("Bridge mode placeholder started")
        while True:
            await asyncio.sleep(10)

    def doctor(self, offline: bool = False) -> list[str]:
        problems: list[str] = []
        if not self.settings.event_signing_secret:
            problems.append("EVENT_SIGNING_SECRET is empty")
        problems.extend(self.registry.validate())
        if not offline:
            if not self.settings.database_url:
                problems.append("DATABASE_URL is empty")
            else:
                try:
                    Database(self.settings.database_url, self.settings.database_sslmode).ping()
                except Exception as exc:
                    problems.append(f"PostgreSQL check failed: {exc}")
        return problems

    def migrate(self) -> int:
        return MigrationManager(Database(self.settings.database_url, self.settings.database_sslmode), self.project_root / "migrations").apply_all()
