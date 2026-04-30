from __future__ import annotations

import importlib
import importlib.util
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from core.events import CajeerEvent
from core.registry import Manifest, Registry
from core.sdk.permissions import PermissionSet
from core.sdk.plugins import PluginContext, PluginPermissionError, RegisteredPluginRoute

logger = logging.getLogger(__name__)


class RuntimeComponent(Protocol):
    id: str

    async def on_start(self, context: "ComponentContext") -> None: ...

    async def on_event(self, event: CajeerEvent, context: "ComponentContext") -> dict[str, object] | None: ...

    async def on_command(self, command: str, event: CajeerEvent, context: "ComponentContext") -> dict[str, object] | None: ...

    async def on_stop(self, context: "ComponentContext") -> None: ...


@dataclass
class ComponentContext(PluginContext):
    runtime: Any
    manifest: Manifest
    logger: logging.Logger
    permissions: PermissionSet | None = None


@dataclass
class LoadedComponent:
    manifest: Manifest
    instance: Any
    failed: bool = False
    last_error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.manifest.id,
            "type": self.manifest.type,
            "entrypoint": self.manifest.entrypoint,
            "failed": self.failed,
            "last_error": self.last_error,
            "catalog": self.manifest.catalog,
            "permissions": list(self.manifest.permissions),
        }


@dataclass
class ComponentManager:
    runtime: Any
    registry: Registry
    loaded: list[LoadedComponent] = field(default_factory=list)

    def _load_from_file(self, manifest: Manifest, module_name: str, attr: str) -> Any | None:
        module_path = manifest.path / (module_name.replace(".", "/") + ".py")
        if not module_path.is_absolute():
            module_path = self.registry.project_root / module_path
        if not module_path.exists() and module_name == "runtime":
            module_path = manifest.path / "runtime.py"
            if not module_path.is_absolute():
                module_path = self.registry.project_root / module_path
        if not module_path.exists():
            return None
        unique_name = f"cajeer_runtime_{manifest.type}_{manifest.id}_{uuid4().hex}"
        spec = importlib.util.spec_from_file_location(unique_name, module_path)
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        sys.modules[unique_name] = module
        spec.loader.exec_module(module)
        cls = getattr(module, attr)
        return cls()

    def _load_entrypoint(self, manifest: Manifest) -> Any | None:
        if not manifest.entrypoint:
            return None
        module_name, _, attr = manifest.entrypoint.partition(":")
        try:
            module = importlib.import_module(module_name)
            cls = getattr(module, attr)
            return cls()
        except ModuleNotFoundError:
            loaded = self._load_from_file(manifest, module_name, attr)
            if loaded is not None:
                return loaded
            raise

    def _context(self, manifest: Manifest) -> ComponentContext:
        return ComponentContext(
            self.runtime,
            manifest,
            logging.getLogger(f"component.{manifest.id}"),
            permissions=PermissionSet.from_iterable(tuple(manifest.permissions)),
        )

    def _require_plugin_permission(self, context: ComponentContext, permission: str) -> None:
        if context.manifest.type != "plugin":
            return
        context.require(permission)

    async def start(self) -> None:
        for manifest in self.registry.load_order():
            if manifest.type == "module" and manifest.id not in self.runtime.settings.modules_enabled:
                continue
            if manifest.type == "plugin" and manifest.id not in self.runtime.settings.plugins_enabled:
                continue
            instance = self._load_entrypoint(manifest)
            if instance is None:
                continue
            component = LoadedComponent(manifest, instance)
            self.loaded.append(component)
            context = self._context(manifest)
            for hook_name in ("on_enable", "on_start"):
                hook = getattr(instance, hook_name, None)
                if hook is not None:
                    try:
                        result = hook(context)
                        if hasattr(result, "__await__"):
                            await result
                    except Exception as exc:  # noqa: BLE001
                        component.failed = True
                        component.last_error = str(exc)
                        logger.exception("компонент %s не запущен на hook %s", manifest.key(), hook_name)
                        break
            route_hook = getattr(instance, "register_api_routes", None)
            if route_hook is not None and not component.failed:
                try:
                    routes = route_hook(context)
                    if routes:
                        self._require_plugin_permission(context, "api.route.register")
                    if routes and hasattr(self.runtime, "plugin_routes"):
                        for route in routes:
                            self.runtime.plugin_routes.append(RegisteredPluginRoute(manifest.id, route, instance, context))
                except Exception as exc:  # noqa: BLE001
                    component.failed = True
                    component.last_error = str(exc)
                    logger.exception("компонент %s не зарегистрировал API routes", manifest.key())
            scheduled_hook = getattr(instance, "register_scheduled_jobs", None)
            if scheduled_hook is not None and not component.failed:
                try:
                    jobs = scheduled_hook(context)
                    if jobs:
                        self._require_plugin_permission(context, "scheduler.jobs.register")
                    for job in jobs or []:
                        if not isinstance(job, dict):
                            raise TypeError("scheduled job должен быть dict")
                        normalized = {**job, "plugin_id": manifest.id}
                        if hasattr(self.runtime, "register_plugin_scheduled_job"):
                            await self.runtime.register_plugin_scheduled_job(manifest, normalized)
                        elif hasattr(self.runtime, "plugin_scheduled_jobs"):
                            self.runtime.plugin_scheduled_jobs.append(normalized)
                except Exception as exc:  # noqa: BLE001
                    component.failed = True
                    component.last_error = str(exc)
                    logger.exception("компонент %s не зарегистрировал scheduled jobs", manifest.key())

    async def stop(self) -> None:
        for component in reversed(self.loaded):
            context = self._context(component.manifest)
            for hook_name in ("on_stop", "on_disable"):
                hook = getattr(component.instance, hook_name, None)
                if hook is not None:
                    try:
                        result = hook(context)
                        if hasattr(result, "__await__"):
                            await result
                    except Exception as exc:  # noqa: BLE001
                        component.failed = True
                        component.last_error = str(exc)
                        logger.warning("ошибка остановки компонента %s на hook %s: %s", component.manifest.key(), hook_name, exc)

    async def route_event(self, event: CajeerEvent) -> dict[str, object] | None:
        for component in self.loaded:
            if component.failed:
                continue
            hook = getattr(component.instance, "on_event", None)
            if hook is None:
                continue
            context = self._context(component.manifest)
            if component.manifest.type == "plugin" and not context.has_permission("events.read"):
                logger.debug("компонент %s пропущен: нет permission events.read", component.manifest.key())
                continue
            result = await hook(event, context)
            if result:
                return result
        return None

    async def route_command(self, command: str, event: CajeerEvent) -> dict[str, object] | None:
        for component in self.loaded:
            if component.failed:
                continue
            hook = getattr(component.instance, "on_command", None)
            if hook is None:
                continue
            context = self._context(component.manifest)
            if component.manifest.type == "plugin" and not context.has_permission("events.read"):
                logger.debug("компонент %s пропущен: нет permission events.read", component.manifest.key())
                continue
            result = await hook(command, event, context)
            if result:
                return result
        return None

    def snapshot(self) -> list[dict[str, object]]:
        return [item.to_dict() for item in self.loaded]
