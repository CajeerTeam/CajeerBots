from __future__ import annotations

import abc
import asyncio
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from core.config import AdapterConfig, Settings
from core.events import CajeerEvent

logger = logging.getLogger(__name__)

AdapterState = Literal["created", "starting", "running", "degraded", "stopping", "stopped", "failed"]


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class AdapterCapabilities:
    messages_send: bool = True
    messages_receive: bool = True
    files_receive: bool = False
    roles: bool = False
    reactions: bool = False
    webhooks: bool = False

    def names(self) -> list[str]:
        values: list[str] = []
        if self.messages_send:
            values.append("messages.send")
        if self.messages_receive:
            values.append("messages.receive")
        if self.files_receive:
            values.append("files.receive")
        if self.roles:
            values.append("roles")
        if self.reactions:
            values.append("reactions")
        if self.webhooks:
            values.append("webhooks")
        values.extend(["health", "events.publish"])
        return sorted(set(values))


@dataclass
class AdapterHealth:
    name: str
    enabled: bool
    configured: bool
    state: AdapterState
    capabilities: list[str]
    started_at: str | None = None
    last_event_at: str | None = None
    last_error: str | None = None
    processed_events: int = 0
    failed_events: int = 0

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass
class AdapterRuntimeStatus:
    state: AdapterState = "created"
    started_at: str | None = None
    last_event_at: str | None = None
    last_error: str | None = None
    processed_events: int = 0
    failed_events: int = 0
    history: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class AdapterContext:
    event_bus: Any
    router: Any
    dead_letters: Any | None = None


class BotAdapter(abc.ABC):
    name: str
    capabilities: AdapterCapabilities = AdapterCapabilities()

    def __init__(self, settings: Settings, config: AdapterConfig, context: AdapterContext | None = None) -> None:
        self.settings = settings
        self.config = config
        self.context = context
        self._stopping = asyncio.Event()
        self.status = AdapterRuntimeStatus()

    def set_context(self, context: AdapterContext) -> None:
        self.context = context

    def set_state(self, state: AdapterState, *, error: str | None = None) -> None:
        self.status.state = state
        if state == "running" and self.status.started_at is None:
            self.status.started_at = _utc_iso()
        if error:
            self.status.last_error = error
        self.status.history.append((state, _utc_iso()))
        self.status.history = self.status.history[-25:]

    async def start(self) -> None:
        self.set_state("starting")
        try:
            await self.on_start()
            self.set_state("running")
            await self.run_loop()
            if self.status.state not in {"failed", "stopping"}:
                self.set_state("stopped")
        except asyncio.CancelledError:
            self.set_state("stopping")
            raise
        except Exception as exc:
            self.status.failed_events += 1
            self.set_state("failed", error=str(exc))
            raise
        finally:
            await self.on_stop()
            if self.status.state != "failed":
                self.set_state("stopped")

    async def on_start(self) -> None:
        """Хук запуска адаптера."""

    async def run_loop(self) -> None:
        while not self._stopping.is_set():
            await asyncio.sleep(5)

    async def on_stop(self) -> None:
        """Хук остановки адаптера."""

    async def stop(self) -> None:
        self.set_state("stopping")
        self._stopping.set()

    async def publish_event(self, event: CajeerEvent) -> None:
        logger.info("%s опубликовал событие: %s", self.name, event.to_json())
        try:
            if self.context is not None:
                await self.context.event_bus.publish(event)
                result = await self.context.router.route(event)
                if not result.handled:
                    logger.info("событие опубликовано без финального обработчика: %s", result.to_dict())
            self.status.processed_events += 1
            self.status.last_event_at = _utc_iso()
        except Exception as exc:
            self.status.failed_events += 1
            self.status.last_error = str(exc)
            if self.context is not None and self.context.dead_letters is not None:
                self.context.dead_letters.add(event, str(exc))
            raise

    async def send_message(self, target: str, text: str) -> None:
        logger.info("%s отправляет сообщение target=%s text=%s", self.name, target, text)
        self.status.processed_events += 1
        self.status.last_event_at = _utc_iso()

    async def health(self) -> AdapterHealth:
        return AdapterHealth(
            name=self.name,
            enabled=self.config.enabled,
            configured=bool(self.config.token),
            state=self.status.state,
            capabilities=self.capabilities.names(),
            started_at=self.status.started_at,
            last_event_at=self.status.last_event_at,
            last_error=self.status.last_error,
            processed_events=self.status.processed_events,
            failed_events=self.status.failed_events,
        )
