from __future__ import annotations

import abc
import asyncio
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from core.config import AdapterConfig, Settings
from core.events import CajeerEvent, command_event_from_message, extract_command

logger = logging.getLogger(__name__)

AdapterState = Literal["created", "starting", "running", "degraded", "stopping", "stopped", "failed"]


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class SendResult:
    ok: bool = True
    platform_message_id: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class AdapterCapabilities:
    messages_send: bool = True
    messages_receive: bool = True
    files_receive: bool = False
    roles: bool = False
    reactions: bool = False
    webhooks: bool = False
    slash_commands: bool = False
    headless_send: bool = False

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
        if self.slash_commands:
            values.append("slash_commands")
        if self.headless_send:
            values.append("headless_send")
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
    restart_count: int = 0

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
    restart_count: int = 0
    history: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class AdapterContext:
    event_bus: Any
    router: Any
    dead_letters: Any | None = None
    delivery: Any | None = None
    audit: Any | None = None
    workspace: Any | None = None
    remote_logs: Any | None = None
    rate_limiter: Any | None = None
    idempotency: Any | None = None
    inline_routing: bool = True


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
        delivery_task: asyncio.Task[None] | None = None
        try:
            await self.on_start()
            self.set_state("running")
            delivery_task = asyncio.create_task(self._delivery_sender_loop(), name=f"delivery:{self.name}")
            await self.run_loop()
            if self.status.state not in {"failed", "stopping"}:
                self.set_state("stopped")
        except asyncio.CancelledError:
            self.set_state("stopping")
            raise
        except Exception as exc:
            self.status.failed_events += 1
            self.set_state("failed", error=str(exc))
            await self.report_lifecycle("adapter.failed", {"error": str(exc)})
            raise
        finally:
            if delivery_task is not None:
                delivery_task.cancel()
                await asyncio.gather(delivery_task, return_exceptions=True)
            await self.on_stop()
            if self.status.state != "failed":
                self.set_state("stopped")

    async def _delivery_sender_loop(self) -> None:
        while not self._stopping.is_set():
            if self.context is not None and self.context.delivery is not None:
                await self.context.delivery.process_for_adapter(self)
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

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

    async def report_lifecycle(self, event_type: str, payload: dict[str, object] | None = None) -> None:
        event = CajeerEvent.create(source=self.name, type=event_type, payload=payload or {})
        await self.publish_event(event)
        if self.context is not None and self.context.workspace is not None:
            await self.context.workspace.report_event(event)
        if self.context is not None and self.context.remote_logs is not None:
            await self.context.remote_logs.emit_event(event, level="INFO")

    async def publish_event(self, event: CajeerEvent) -> None:
        logger.info("%s опубликовал событие: %s", self.name, event.to_json())
        try:
            if self.context is not None:
                await self.context.event_bus.publish(event)
                if self.context.inline_routing:
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

    def _platform_idempotency_key(self, event: CajeerEvent) -> str | None:
        raw = event.payload.get("raw") if isinstance(event.payload, dict) else None
        raw = raw if isinstance(raw, dict) else {}
        if event.source == "telegram":
            update = raw.get("update") if isinstance(raw.get("update"), dict) else {}
            update_id = update.get("update_id") if isinstance(update, dict) else None
            return f"telegram:update:{update_id}" if update_id is not None else None
        if event.source == "discord":
            for key in ("interaction_id", "message_id"):
                value = raw.get(key)
                if value:
                    return f"discord:{key}:{value}"
        if event.source == "vkontakte":
            callback = raw.get("callback") if isinstance(raw.get("callback"), dict) else raw.get("update")
            if isinstance(callback, dict):
                value = callback.get("event_id") or callback.get("id") or callback.get("object_id")
                if value:
                    return f"vk:event:{value}"
                return "vk:event:" + ":".join(str(callback.get(k, "")) for k in ("group_id", "type"))
        return None

    async def handle_incoming_message(self, event: CajeerEvent, *, bot_username: str | None = None) -> None:
        if self.context is not None and self.context.idempotency is not None:
            key = self._platform_idempotency_key(event)
            if key:
                try:
                    already = await self.context.idempotency.seen_async(key)
                except AttributeError:
                    already = self.context.idempotency.seen(key)
                if already:
                    logger.info("%s пропустил дубль platform update: %s", self.name, key)
                    return
        await self.publish_event(event)
        command = extract_command(str(event.payload.get("text") or ""), bot_username=bot_username)
        if command is not None:
            name, args = command
            await self.publish_event(command_event_from_message(event, name, args))
    async def send_message(self, target: str, text: str) -> SendResult:
        logger.info("%s отправляет сообщение target=%s text=%s", self.name, target, text)
        self.status.processed_events += 1
        self.status.last_event_at = _utc_iso()
        return SendResult(ok=True, raw={"target": target})

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
            restart_count=self.status.restart_count,
        )
