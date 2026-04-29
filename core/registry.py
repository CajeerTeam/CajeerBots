from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from core.config import Settings

ManifestKind = Literal["module", "plugin", "adapter"]

KNOWN_CAPABILITIES = {
    "messages.receive",
    "messages.send",
    "files.receive",
    "roles",
    "reactions",
    "webhooks",
    "health",
    "events.publish",
}

DEPENDENCY_TYPES = {"module", "plugin", "adapter"}


@dataclass(frozen=True)
class Dependency:
    type: str
    id: str

    @classmethod
    def parse(cls, raw: str) -> "Dependency":
        if ":" not in raw:
            # Совместимость со старыми manifest: bridge == module:bridge.
            return cls("module", raw)
        kind, component_id = raw.split(":", 1)
        return cls(kind.strip(), component_id.strip())

    def normalized(self) -> str:
        return f"{self.type}:{self.id}"


@dataclass(frozen=True)
class Manifest:
    id: str
    name: str
    version: str
    type: ManifestKind
    path: Path
    description: str = ""
    requires: tuple[str, ...] = ()
    adapters: tuple[str, ...] = ()
    capabilities: tuple[str, ...] = ()
    enabled_by_default: bool = False
    settings_schema: dict[str, object] | None = None

    def dependencies(self) -> list[Dependency]:
        return [Dependency.parse(item) for item in self.requires]

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["path"] = str(self.path)
        data["requires"] = list(self.requires)
        data["adapters"] = list(self.adapters)
        data["capabilities"] = list(self.capabilities)
        return data


class Registry:
    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root

    def _load(self, path: Path) -> Manifest:
        data = json.loads(path.read_text(encoding="utf-8"))
        return Manifest(
            id=data["id"],
            name=data.get("name", data["id"]),
            version=data.get("version", "0.0.0"),
            type=data.get("type", "module"),
            path=path.parent,
            description=data.get("description", ""),
            requires=tuple(data.get("requires", [])),
            adapters=tuple(data.get("adapters", [])),
            capabilities=tuple(data.get("capabilities", [])),
            enabled_by_default=bool(data.get("enabled_by_default", False)),
            settings_schema=data.get("settings_schema"),
        )

    def modules(self) -> list[Manifest]:
        return [self._load(path) for path in sorted((self.project_root / "modules").glob("*/module.json"))]

    def plugins(self) -> list[Manifest]:
        return [self._load(path) for path in sorted((self.project_root / "plugins").glob("*/plugin.json"))]

    def adapters(self) -> list[Manifest]:
        return [self._load(path) for path in sorted((self.project_root / "bots").glob("*/adapter.json"))]

    def all(self) -> list[Manifest]:
        return [*self.adapters(), *self.modules(), *self.plugins()]

    def validate(self, settings: Settings | None = None) -> list[str]:
        errors: list[str] = []
        manifests = self.all()
        adapters = {manifest.id: manifest for manifest in manifests if manifest.type == "adapter"}
        modules = {manifest.id: manifest for manifest in manifests if manifest.type == "module"}
        plugins = {manifest.id: manifest for manifest in manifests if manifest.type == "plugin"}
        by_type = {"adapter": adapters, "module": modules, "plugin": plugins}
        seen: set[str] = set()

        for manifest in manifests:
            key = f"{manifest.type}:{manifest.id}"
            if key in seen:
                errors.append(f"дублирующийся идентификатор manifest: {key}")
            seen.add(key)
            if manifest.type not in {"module", "plugin", "adapter"}:
                errors.append(f"неизвестный тип manifest у {manifest.id}: {manifest.type}")
            if not manifest.version:
                errors.append(f"у manifest {key} пустая версия")
            if not manifest.name:
                errors.append(f"у manifest {key} пустое название")
            if manifest.type == "adapter":
                unknown_capabilities = sorted(set(manifest.capabilities) - KNOWN_CAPABILITIES)
                if unknown_capabilities:
                    errors.append(f"у адаптера {manifest.id} неизвестные capabilities: {', '.join(unknown_capabilities)}")
            if manifest.type in {"module", "plugin"}:
                for adapter_id in manifest.adapters:
                    if adapter_id not in adapters:
                        errors.append(f"{manifest.type} {manifest.id} ссылается на отсутствующий адаптер {adapter_id}")
                for dependency in manifest.dependencies():
                    if dependency.type not in DEPENDENCY_TYPES:
                        errors.append(f"{manifest.type} {manifest.id} содержит неизвестный тип зависимости {dependency.type}")
                        continue
                    if dependency.id not in by_type[dependency.type]:
                        errors.append(
                            f"{manifest.type} {manifest.id} зависит от отсутствующего компонента {dependency.normalized()}"
                        )
                if manifest.settings_schema is not None and not isinstance(manifest.settings_schema, dict):
                    errors.append(f"settings_schema у {manifest.type} {manifest.id} должен быть объектом")

        if settings is not None:
            for module_id in settings.modules_enabled:
                if module_id not in modules:
                    errors.append(f"включённый модуль не найден: {module_id}")
            for plugin_id in settings.plugins_enabled:
                if plugin_id not in plugins:
                    errors.append(f"включённый плагин не найден: {plugin_id}")
            enabled_by_type = {
                "module": set(settings.modules_enabled),
                "plugin": set(settings.plugins_enabled),
                "adapter": {name for name, adapter in settings.adapters.items() if adapter.enabled},
            }
            for manifest in [*(modules.get(item) for item in settings.modules_enabled), *(plugins.get(item) for item in settings.plugins_enabled)]:
                if manifest is None:
                    continue
                for dependency in manifest.dependencies():
                    if dependency.id not in enabled_by_type.get(dependency.type, set()):
                        errors.append(
                            f"компонент {manifest.type}:{manifest.id} требует включить {dependency.normalized()}"
                        )
        return errors
