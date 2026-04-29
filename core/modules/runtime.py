from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

from core.events import CajeerEvent
from core.registry import Manifest, Registry

logger = logging.getLogger(__name__)


class RuntimeComponent(Protocol):
    id: str

    async def on_start(self, context: "ComponentContext") -> None: ...

    async def on_event(self, event: CajeerEvent, context: "ComponentContext") -> dict[str, object] | None: ...

    async def on_command(self, command: str, event: CajeerEvent, context: "ComponentContext") -> dict[str, object] | None: ...

    async def on_stop(self, context: "ComponentContext") -> None: ...


@dataclass
class ComponentContext:
    runtime: Any
    manifest: Manifest
    logger: logging.Logger


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
        }


@dataclass
class ComponentManager:
    runtime: Any
    registry: Registry
    loaded: list[LoadedComponent] = field(default_factory=list)

    def _load_entrypoint(self, manifest: Manifest) -> Any | None:
        if not manifest.entrypoint:
            return None
        module_name, _, attr = manifest.entrypoint.partition(":")
        module = importlib.import_module(module_name)
        cls = getattr(module, attr)
        return cls()

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
            hook = getattr(instance, "on_start", None)
            if hook is not None:
                try:
                    await hook(ComponentContext(self.runtime, manifest, logging.getLogger(f"component.{manifest.id}")))
                except Exception as exc:  # noqa: BLE001
                    component.failed = True
                    component.last_error = str(exc)
                    logger.exception("компонент %s не запущен", manifest.key())

    async def stop(self) -> None:
        for component in reversed(self.loaded):
            hook = getattr(component.instance, "on_stop", None)
            if hook is not None:
                try:
                    await hook(ComponentContext(self.runtime, component.manifest, logging.getLogger(f"component.{component.manifest.id}")))
                except Exception as exc:  # noqa: BLE001
                    component.failed = True
                    component.last_error = str(exc)
                    logger.warning("ошибка остановки компонента %s: %s", component.manifest.key(), exc)

    async def route_event(self, event: CajeerEvent) -> dict[str, object] | None:
        for component in self.loaded:
            if component.failed:
                continue
            hook = getattr(component.instance, "on_event", None)
            if hook is None:
                continue
            result = await hook(event, ComponentContext(self.runtime, component.manifest, logging.getLogger(f"component.{component.manifest.id}")))
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
            result = await hook(command, event, ComponentContext(self.runtime, component.manifest, logging.getLogger(f"component.{component.manifest.id}")))
            if result:
                return result
        return None

    def snapshot(self) -> list[dict[str, object]]:
        return [item.to_dict() for item in self.loaded]
