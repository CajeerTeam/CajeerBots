from __future__ import annotations

import abc
import asyncio
import logging
from dataclasses import dataclass

from core.config import AdapterConfig, Settings
from core.events import CajeerEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AdapterCapabilities:
    messages_send: bool = True
    messages_receive: bool = True
    files_receive: bool = False
    roles: bool = False
    reactions: bool = False
    webhooks: bool = False


class BotAdapter(abc.ABC):
    name: str
    capabilities: AdapterCapabilities = AdapterCapabilities()

    def __init__(self, settings: Settings, config: AdapterConfig) -> None:
        self.settings = settings
        self.config = config
        self._stopping = asyncio.Event()

    @abc.abstractmethod
    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        self._stopping.set()

    async def publish_event(self, event: CajeerEvent) -> None:
        logger.info("%s опубликовал событие: %s", self.name, event.to_json())

    async def send_message(self, target: str, text: str) -> None:
        logger.info("%s отправляет сообщение target=%s text=%s", self.name, target, text)
