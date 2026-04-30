from __future__ import annotations

import asyncio
import logging
import signal
import time
from pathlib import Path
from typing import Iterable

from core.adapters.base import AdapterContext, AdapterHealth, BotAdapter
from core.adapters.discord import DiscordAdapter
from core.adapters.fake import FakeAdapter
from core.adapters.telegram import TelegramAdapter
from core.adapters.vkontakte import VkontakteAdapter
from core.audit import build_audit_log
from core.bridge import BridgeService
from core.commands import CommandRegistry, build_default_commands
from core.contracts import API_CONTRACT_VERSION, DB_CONTRACT_VERSION, EVENT_CONTRACT_VERSION_ID, LOGS_CONTRACT_VERSION, WORKSPACE_CONTRACT_VERSION
from core.compatibility import check_compatibility
from core.config import Settings
from core.db import Database
from core.dead_letters import build_dead_letter_queue
from core.delivery import build_delivery_service
from core.event_bus import build_event_bus
from core.events import EVENT_CONTRACT_VERSION, command_event_from_message, extract_command
from core.idempotency import build_idempotency_store
from core.integrations.logs import CajeerLogsClient
from core.integrations.workspace import WorkspaceClient
from core.modules.runtime import ComponentManager
from core.permissions_fix import executable_paths
from core.registry import Registry
from core.router import EventRouter
from core.worker import WorkerService
from core.rate_limits import build_rate_limiter
from core.updater import UpdateManager
from core.token_registry import ApiTokenRegistry
from core.rbac_store import HybridRbacStore

logger = logging.getLogger(__name__)

ADAPTER_CLASSES: dict[str, type[BotAdapter]] = {
    "telegram": TelegramAdapter,
    "discord": DiscordAdapter,
    "vkontakte": VkontakteAdapter,
    "fake": FakeAdapter,
}

PLACEHOLDER_SECRETS = {
    "change-me",
    "change-me-admin-token",
    "change-me-readonly-token",
    "change-me-metrics-token",
    "change-me-long-random-secret",
    "",
}

FORBIDDEN_TERMS = ["Never" + "Mine", "cajeer" + "_bots", "cajeer" + "_core", "nm" + "bot"]
TEXT_EXTENSIONS = {".py", ".md", ".json", ".yaml", ".yml", ".toml", ".sh", ".service", ".conf", ".example", ".ini"}


class Runtime:
    def __init__(self, settings: Settings, project_root: Path | None = None) -> None:
        self.settings = settings
        self.project_root = project_root or Path.cwd()
        self.registry = Registry(self.project_root, settings=settings)
        self.adapters: list[BotAdapter] = []
        self.event_bus = build_event_bus(settings)
        self.dead_letters = build_dead_letter_queue(settings)
        self.delivery = build_delivery_service(settings)
        self.idempotency = build_idempotency_store(settings)
        self.commands: CommandRegistry = build_default_commands(self)
        self.audit = build_audit_log(settings)
        self.components = ComponentManager(self, self.registry)
        self.router = EventRouter(self.commands, self.idempotency, components=self.components)
        self.bridge = BridgeService(self)
        self.worker = WorkerService(self)
        self.updater = UpdateManager(self)
        self.token_registry = ApiTokenRegistry(settings.api_tokens_file)
        self.rbac_store = HybridRbacStore(settings.runtime_dir / "secrets" / "rbac_cache.json")
        self.rate_limiter = build_rate_limiter(settings)
        self._stop_event: asyncio.Event | None = None
        self.version = self._read_version()
        self.event_contract_version = EVENT_CONTRACT_VERSION_ID
        self.db_contract_version = DB_CONTRACT_VERSION
        self.workspace_contract_version = WORKSPACE_CONTRACT_VERSION
        self.logs_contract_version = LOGS_CONTRACT_VERSION
        self.api_contract_version = API_CONTRACT_VERSION
        self.workspace = WorkspaceClient(settings.workspace, settings.instance_id, self.version)
        self.remote_logs = CajeerLogsClient(settings.remote_logs, settings.instance_id, settings.runtime_dir / "logs-buffer")
        self.started_at = time.time()

    def _read_version(self) -> str:
        path = self.project_root / "VERSION"
        if path.exists():
            return path.read_text(encoding="utf-8").strip()
        return "0.0.0"

    def build_adapter(self, name: str) -> BotAdapter:
        context = AdapterContext(
            self.event_bus,
            self.router,
            self.dead_letters,
            delivery=self.delivery,
            audit=self.audit,
            workspace=self.workspace,
            remote_logs=self.remote_logs,
            rate_limiter=self.rate_limiter,
            idempotency=self.idempotency,
            inline_routing=self.settings.local_inline_routing,
        )
        return ADAPTER_CLASSES[name](self.settings, self.settings.adapters[name], context=context)

    def selected_adapters(self, target: str) -> list[str]:
        if target == "all":
            return [adapter.name for adapter in self.settings.enabled_adapters() if adapter.name in ADAPTER_CLASSES]
        if target in ADAPTER_CLASSES:
            return [target]
        return []

    def adapter_map(self) -> dict[str, BotAdapter]:
        return {adapter.name: adapter for adapter in self.adapters}

    def make_system_event(self, event_type: str, payload: dict[str, object] | None = None):
        from core.events import CajeerEvent

        return CajeerEvent.create(source="system", type=event_type, payload=payload or {})

    def _idempotency_key_for_event(self, event) -> str | None:
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

    async def _already_processed_platform_event(self, event) -> bool:
        key = self._idempotency_key_for_event(event)
        if not key:
            return False
        try:
            return await self.idempotency.seen_async(key)
        except AttributeError:
            return self.idempotency.seen(key)

    def adapter_health_snapshot(self) -> list[AdapterHealth]:
        return [
            AdapterHealth(
                name=adapter.name,
                enabled=adapter.config.enabled,
                configured=bool(adapter.config.token) or adapter.name == "fake",
                state=adapter.status.state,
                capabilities=adapter.capabilities.names(),
                started_at=adapter.status.started_at,
                last_event_at=adapter.status.last_event_at,
                last_error=adapter.status.last_error,
                processed_events=adapter.status.processed_events,
                failed_events=adapter.status.failed_events,
                restart_count=adapter.status.restart_count,
            )
            for adapter in self.adapters
        ]

    async def run(self, target: str | None = None) -> None:
        target = target or self.settings.default_target
        logger.info("запуск Cajeer Bots, режим=%s, цель=%s", self.settings.mode, target)
        self.settings.runtime_dir.mkdir(parents=True, exist_ok=True)
        await self.components.start()

        if self.settings.mode == "distributed" and target not in {"api", "worker", "bridge"}:
            logger.info("распределённый режим включён; локальный запуск адаптеров используется только для agent-узла")

        try:
            if target == "api":
                return await self.run_api()
            if target == "worker":
                return await self.run_worker()
            if target == "bridge":
                return await self.run_bridge()

            names = self.selected_adapters(target)
            if not names:
                raise ValueError(f"неподдерживаемая цель запуска: {target}")
            self.adapters = [self.build_adapter(name) for name in names]
            await self._run_supervised(self.adapters)
        finally:
            await self.components.stop()

    async def _adapter_supervisor(self, adapter: BotAdapter) -> None:
        restarts = 0
        policy = self.settings.supervisor.restart_policy
        max_restarts = self.settings.supervisor.restart_max
        while not (self._stop_event and self._stop_event.is_set()):
            try:
                await adapter.start()
                if policy != "always":
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                adapter.status.restart_count = restarts
                logger.error("адаптер %s завершился с ошибкой: %s", adapter.name, exc)
                self.audit.write(
                    actor_type="system",
                    actor_id="runtime",
                    action="adapter.failed",
                    resource=adapter.name,
                    result="error",
                    message=str(exc),
                )
                if policy == "never" or (policy == "on-failure" and restarts >= max_restarts):
                    if self._stop_event is not None:
                        self._stop_event.set()
                    return
                restarts += 1
                adapter.status.restart_count = restarts
                adapter.set_state("degraded", error=str(exc))
                await asyncio.sleep(self.settings.supervisor.restart_backoff_seconds)

    async def _run_supervised(self, adapters: Iterable[BotAdapter]) -> None:
        self._stop_event = asyncio.Event()
        self._install_signal_handlers(self._stop_event)
        tasks = [asyncio.create_task(self._adapter_supervisor(adapter), name=f"adapter:{adapter.name}") for adapter in adapters]
        stop_task = asyncio.create_task(self._stop_event.wait(), name="runtime:stop")
        heartbeat_task = asyncio.create_task(self._workspace_heartbeat_loop(), name="workspace:heartbeat")
        try:
            done, _ = await asyncio.wait([*tasks, stop_task], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not stop_task:
                    exc = task.exception()
                    if exc:
                        logger.error("supervisor завершился с ошибкой: %s", exc)
                        self._stop_event.set()
            if stop_task in done:
                logger.info("получен сигнал остановки")
        finally:
            heartbeat_task.cancel()
            await self._stop_adapters(tasks)
            stop_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)

    async def _workspace_heartbeat_loop(self) -> None:
        while not (self._stop_event and self._stop_event.is_set()):
            await self.workspace.heartbeat(self.readiness_snapshot())
            await asyncio.sleep(30)

    async def _stop_adapters(self, tasks: list[asyncio.Task[None]]) -> None:
        for adapter in self.adapters:
            await adapter.stop()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def request_stop(self) -> None:
        if self._stop_event is not None:
            self._stop_event.set()
        self.audit.write(actor_type="api", actor_id="admin", action="runtime.stop", resource="runtime")

    def _install_signal_handlers(self, stop_event: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except (NotImplementedError, RuntimeError):
                pass

    async def ingest_incoming_event(self, event, *, bot_username: str | None = None) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        if await self._already_processed_platform_event(event):
            return [{"handled": True, "handler": "idempotency", "details": {"skipped": True, "source": event.source}}]
        await self.event_bus.publish(event)
        results.append((await self.router.route(event)).to_dict())
        command = extract_command(str(event.payload.get("text") or ""), bot_username=bot_username)
        if command is not None:
            name, args = command
            command_event = command_event_from_message(event, name, args)
            await self.event_bus.publish(command_event)
            results.append((await self.router.route(command_event)).to_dict())
        return results
    async def run_api(self) -> None:
        logger.info("режим API запущен на %s:%s", self.settings.api_bind, self.settings.api_port)
        stop_event = asyncio.Event()
        self._stop_event = stop_event
        self._install_signal_handlers(stop_event)
        if getattr(self.settings, "api_server", "stdlib") == "asgi":
            try:
                import uvicorn  # type: ignore
            except ImportError as exc:  # pragma: no cover - зависит от optional dependency
                raise RuntimeError("API_SERVER=asgi требует пакет uvicorn: pip install cajeer-bots[api]") from exc
            from core.asgi import create_app

            config = uvicorn.Config(create_app(self), host=self.settings.api_bind, port=self.settings.api_port, log_level=self.settings.log_level.lower())
            server = uvicorn.Server(config)
            task = asyncio.create_task(server.serve(), name="api:asgi")
            try:
                await stop_event.wait()
            finally:
                server.should_exit = True
                await asyncio.gather(task, return_exceptions=True)
            return

        from core.api import ApiServer

        server = ApiServer(self, loop=asyncio.get_running_loop())
        server.start_in_thread()
        try:
            await stop_event.wait()
        finally:
            server.stop()

    async def run_worker(self) -> None:
        stop_event = asyncio.Event()
        self._stop_event = stop_event
        self._install_signal_handlers(stop_event)
        await self.worker.run(stop_event)

    async def run_bridge(self) -> None:
        stop_event = asyncio.Event()
        self._stop_event = stop_event
        self._install_signal_handlers(stop_event)
        await self.bridge.run(stop_event)

    def dependencies_snapshot(self) -> dict[str, object]:
        return {
            "database_required": self.settings.event_bus_backend == "postgres" or self.settings.storage.delivery_backend == "postgres" or self.settings.storage.dead_letter_backend == "postgres" or self.settings.storage.idempotency_backend == "postgres",
            "database_configured": bool(self.settings.storage.async_database_url or self.settings.database_url),
            "redis_required": self.settings.event_bus_backend == "redis"
            or self.settings.storage.delivery_backend == "redis"
            or self.settings.storage.dead_letter_backend == "redis"
            or self.settings.storage.idempotency_backend == "redis",
            "redis_configured": bool(self.settings.redis_url),
            "event_bus_backend": self.settings.event_bus_backend,
            "delivery_backend": self.settings.storage.delivery_backend,
            "dead_letter_backend": self.settings.storage.dead_letter_backend,
            "idempotency_backend": self.settings.storage.idempotency_backend,
            "enabled_adapters": [adapter.name for adapter in self.settings.enabled_adapters()],
            "distributed_enabled": self.settings.distributed.enabled,
            "workspace_enabled": self.settings.workspace.enabled,
            "remote_logs_enabled": self.settings.remote_logs.enabled,
            "audit_backend": getattr(self.audit, "backend", "memory"),
            "delivery_runtime_backend": getattr(self.delivery, "backend", "memory"),
            "dead_letter_runtime_backend": getattr(self.dead_letters, "backend", "memory"),
            "idempotency_runtime_backend": getattr(self.idempotency, "backend", "memory"),
            "event_contract_version": self.event_contract_version,
            "db_contract_version": self.db_contract_version,
            "workspace_contract_version": self.workspace_contract_version,
            "logs_contract_version": self.logs_contract_version,
            "update_source": self.updater.source,
            "update_channel": self.updater.channel,
            "rate_limiter": self.rate_limiter.__class__.__name__,
            "api_token_registry_configured": self.settings.api_tokens_file.exists(),
        }

    def _production_security_problems(self) -> list[str]:
        if self.settings.env != "production":
            return []
        problems: list[str] = []
        if self.settings.api_readonly_token in PLACEHOLDER_SECRETS:
            problems.append("API_TOKEN_READONLY содержит демонстрационное значение")
        if self.settings.api_metrics_token in PLACEHOLDER_SECRETS:
            problems.append("API_TOKEN_METRICS содержит демонстрационное значение")
        telegram = self.settings.adapters.get("telegram")
        if telegram and telegram.enabled and telegram.extra.get("mode") == "webhook" and not telegram.extra.get("webhook_secret"):
            problems.append("TELEGRAM_WEBHOOK_SECRET обязателен для production webhook-режима")
        vkontakte = self.settings.adapters.get("vkontakte")
        if vkontakte and vkontakte.enabled and not vkontakte.extra.get("callback_secret"):
            problems.append("VK_CALLBACK_SECRET обязателен для production VK Callback API")
        return problems

    def readiness_snapshot(self) -> dict[str, object]:
        problems: list[str] = []
        problems.extend(self.settings.validate_runtime(doctor_mode=self.settings.mode))
        problems.extend(self.registry.validate(settings=self.settings))
        compat = check_compatibility(self.project_root, self.version, registry=self.registry)
        problems.extend(compat.errors)
        if self.settings.api_token in PLACEHOLDER_SECRETS:
            problems.append("API_TOKEN содержит демонстрационное значение")
        problems.extend(self._production_security_problems())
        if self.settings.event_signing_secret in PLACEHOLDER_SECRETS:
            problems.append("EVENT_SIGNING_SECRET содержит демонстрационное значение")
        if self.settings.event_bus_backend == "postgres" and not self.settings.storage.async_database_url:
            problems.append("DATABASE_ASYNC_URL требуется для EVENT_BUS_BACKEND=postgres")
        if self.settings.event_bus_backend == "redis" and not self.settings.redis_url:
            problems.append("Redis требуется для EVENT_BUS_BACKEND=redis")
        dependency_checks = self.dependency_health_snapshot()
        for check in dependency_checks.get("checks", []):
            if isinstance(check, dict) and not check.get("ok"):
                problems.append(str(check.get("message") or check.get("name")))
        return {
            "ok": not problems,
            "problems": problems,
            "dependencies": self.dependencies_snapshot(),
            "event_bus": self.event_bus.metrics().to_dict(),
            "registry": {
                "adapters": len(self.registry.adapters()),
                "modules": len(self.registry.modules()),
                "plugins": len(self.registry.plugins()),
                "load_order": [item.to_dict() for item in self.registry.load_order()],
            },
            "components": self.components.snapshot(),
            "dependency_checks": dependency_checks,
        }

    def dependency_health_snapshot(self) -> dict[str, object]:
        checks: list[dict[str, object]] = []
        redis_required = (
            self.settings.event_bus_backend == "redis"
            or self.settings.storage.delivery_backend == "redis"
            or self.settings.storage.dead_letter_backend == "redis"
            or self.settings.storage.idempotency_backend == "redis"
        )
        postgres_required = (
            self.settings.event_bus_backend == "postgres"
            or self.settings.storage.delivery_backend == "postgres"
            or self.settings.storage.dead_letter_backend == "postgres"
            or self.settings.storage.idempotency_backend == "postgres"
        )
        if redis_required:
            if not self.settings.redis_url:
                checks.append({"name": "redis", "ok": False, "message": "REDIS_URL не задан"})
            else:
                try:
                    import redis
                    client = redis.Redis.from_url(self.settings.redis_url, socket_connect_timeout=1, socket_timeout=1)
                    client.ping()
                    checks.append({"name": "redis", "ok": True, "message": "pong"})
                except Exception as exc:
                    checks.append({"name": "redis", "ok": False, "message": str(exc)})
        if postgres_required:
            if not self.settings.storage.async_database_url:
                checks.append({"name": "postgres", "ok": False, "message": "DATABASE_ASYNC_URL не задан"})
            else:
                try:
                    from core.db_async import check_schema
                    problems = asyncio.run(check_schema(self.settings.storage.async_database_url, self.settings.shared_schema))
                    checks.append({"name": "postgres", "ok": not problems, "message": "; ".join(problems[:5]) if problems else "schema ok"})
                except RuntimeError:
                    checks.append({"name": "postgres", "ok": True, "message": "async check skipped inside active loop"})
                except Exception as exc:
                    checks.append({"name": "postgres", "ok": False, "message": str(exc)})
        checks.append({"name": "event_bus", "ok": True, "message": self.event_bus.metrics().backend})
        checks.append({"name": "delivery", "ok": True, "message": getattr(self.delivery, "backend", "memory")})
        checks.append({"name": "workspace", "ok": not self.settings.workspace.enabled or bool(self.settings.workspace.url), "message": "enabled" if self.settings.workspace.enabled else "disabled"})
        checks.append({"name": "remote_logs", "ok": not self.settings.remote_logs.enabled or bool(self.settings.remote_logs.url), "message": "enabled" if self.settings.remote_logs.enabled else "disabled"})
        return {"checks": checks}

    async def dependency_health_snapshot_async(self) -> dict[str, object]:
        checks: list[dict[str, object]] = []
        redis_required = (
            self.settings.event_bus_backend == "redis"
            or self.settings.storage.delivery_backend == "redis"
            or self.settings.storage.dead_letter_backend == "redis"
            or self.settings.storage.idempotency_backend == "redis"
        )
        postgres_required = (
            self.settings.event_bus_backend == "postgres"
            or self.settings.storage.delivery_backend == "postgres"
            or self.settings.storage.dead_letter_backend == "postgres"
            or self.settings.storage.idempotency_backend == "postgres"
        )
        if redis_required:
            if not self.settings.redis_url:
                checks.append({"name": "redis", "ok": False, "message": "REDIS_URL не задан"})
            else:
                try:
                    from redis.asyncio import Redis  # type: ignore
                    client = Redis.from_url(self.settings.redis_url, socket_connect_timeout=1, socket_timeout=1)
                    await client.ping()
                    await client.aclose()
                    checks.append({"name": "redis", "ok": True, "message": "pong"})
                except Exception as exc:
                    checks.append({"name": "redis", "ok": False, "message": str(exc)})
        if postgres_required:
            if not self.settings.storage.async_database_url:
                checks.append({"name": "postgres", "ok": False, "message": "DATABASE_ASYNC_URL не задан"})
            else:
                try:
                    from core.db_async import check_schema
                    problems = await check_schema(self.settings.storage.async_database_url, self.settings.shared_schema)
                    checks.append({"name": "postgres", "ok": not problems, "message": "; ".join(problems[:5]) if problems else "schema ok"})
                except Exception as exc:
                    checks.append({"name": "postgres", "ok": False, "message": str(exc)})
        checks.append({"name": "event_bus", "ok": True, "message": self.event_bus.metrics().backend})
        checks.append({"name": "delivery", "ok": True, "message": getattr(self.delivery, "backend", "memory")})
        checks.append({"name": "workspace", "ok": not self.settings.workspace.enabled or bool(self.settings.workspace.url), "message": "enabled" if self.settings.workspace.enabled else "disabled"})
        checks.append({"name": "remote_logs", "ok": not self.settings.remote_logs.enabled or bool(self.settings.remote_logs.url), "message": "enabled" if self.settings.remote_logs.enabled else "disabled"})
        return {"checks": checks}

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
            "# HELP cajeerbots_delivery_failed_total Количество ошибок доставки.",
            "# TYPE cajeerbots_delivery_failed_total counter",
            f"cajeerbots_delivery_failed_total {self.delivery.failed_total}",
            "# HELP cajeerbots_outbound_trace_failed_total Количество ошибок записи outbound trace.",
            "# TYPE cajeerbots_outbound_trace_failed_total counter",
            f"cajeerbots_outbound_trace_failed_total {getattr(self.delivery, 'outbound_trace_failed_total', 0)}",
            "# HELP cajeerbots_audit_records_total Количество audit-записей в runtime.",
            "# TYPE cajeerbots_audit_records_total gauge",
            f"cajeerbots_audit_records_total {len(self.audit.snapshot())}",
        ]
        for adapter in self.adapter_health_snapshot():
            state_value = 1 if adapter.state == "running" else 0
            lines.append(f'cajeerbots_adapter_state{{adapter="{adapter.name}",state="{adapter.state}"}} {state_value}')
            lines.append(f'cajeerbots_adapter_events_total{{adapter="{adapter.name}"}} {adapter.processed_events}')
            lines.append(f'cajeerbots_adapter_events_failed_total{{adapter="{adapter.name}"}} {adapter.failed_events}')
            lines.append(f'cajeerbots_adapter_restarts_total{{adapter="{adapter.name}"}} {adapter.restart_count}')
        return "\n".join(lines) + "\n"

    def doctor(self, offline: bool = False, *, doctor_mode: str = "local", profile: str | None = None) -> list[str]:
        profile = profile or ("production" if self.settings.env == "production" else "dev")
        problems: list[str] = []
        warnings: list[str] = []
        problems.extend(self.settings.validate_runtime(doctor_mode=doctor_mode))

        strict_secrets = profile in {"staging", "production", "release-artifact"}
        if strict_secrets:
            if not self.settings.event_signing_secret:
                problems.append("EVENT_SIGNING_SECRET не задан")
            if self.settings.event_signing_secret in PLACEHOLDER_SECRETS:
                problems.append("EVENT_SIGNING_SECRET содержит демонстрационное значение")
            if self.settings.api_token in PLACEHOLDER_SECRETS:
                problems.append("API_TOKEN содержит демонстрационное значение")
        elif not self.settings.event_signing_secret:
            warnings.append("EVENT_SIGNING_SECRET не задан; допустимо только для dev-профиля")

        if profile == "production":
            problems.extend(self._production_security_problems())
            if self.settings.api_bind in {"0.0.0.0", "::"} and not self.settings.metrics_public:
                warnings.append("API_BIND открыт на все интерфейсы; убедитесь, что API закрыт reverse proxy/TLS/allowlist")
        if profile == "release-artifact":
            for required in ["README.md", "LICENSE", "VERSION", "pyproject.toml", ".env.example", "Dockerfile", "docker-compose.yml", "alembic.ini"]:
                if not (self.project_root / required).exists():
                    problems.append(f"release artifact неполный: отсутствует {required}")
            if (self.project_root / ".env").exists():
                problems.append("release artifact не должен содержать .env")

        if not (self.project_root / "core").is_dir():
            problems.append("каталог core не найден")
        if not (self.project_root / "bots").is_dir():
            problems.append("каталог bots не найден")
        if (self.project_root / "migrations").exists():
            problems.append("каталог migrations не должен входить в проект; используйте alembic/")
        problems.extend(self.registry.validate(settings=self.settings))
        problems.extend(self._check_executable_bits())
        problems.extend(self._check_forbidden_terms())
        compat = check_compatibility(self.project_root, self.version, registry=self.registry)
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
                if name == "fake":
                    continue
                if adapter.enabled and not adapter.token:
                    problems.append(f"адаптер {name} включён, но его токен не задан")
        return problems

    def _check_executable_bits(self) -> list[str]:
        errors: list[str] = []
        for path in executable_paths(self.project_root):
            if path.exists() and not path.stat().st_mode & 0o111:
                errors.append(f"файл должен быть исполняемым: {path.relative_to(self.project_root)}")
        return errors

    def _check_forbidden_terms(self) -> list[str]:
        errors: list[str] = []
        ignored_dirs = {".git", "dist", "runtime", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache"}
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
