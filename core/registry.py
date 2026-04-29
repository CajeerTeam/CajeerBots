from __future__ import annotations

import importlib.resources as resources
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Literal

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
    entrypoint: str | None = None
    catalog: str = "repo"

    def dependencies(self) -> list[Dependency]:
        return [Dependency.parse(item) for item in self.requires]

    def key(self) -> str:
        return f"{self.type}:{self.id}"

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["path"] = str(self.path)
        data["requires"] = list(self.requires)
        data["adapters"] = list(self.adapters)
        data["capabilities"] = list(self.capabilities)
        return data


class Registry:
    """Registry with package-data catalog, runtime catalog and repo-root fallback.

    B + C model:
    - built-in bots/modules/core plugins are packaged with the wheel;
    - business/custom plugins and modules are loaded from runtime catalog paths;
    - development may still read repo-root folders directly.
    """

    def __init__(self, project_root: Path, settings: Settings | None = None) -> None:
        self.project_root = project_root
        self.settings = settings

    def _load_json(self, path: Path, *, catalog: str) -> Manifest:
        data = json.loads(path.read_text(encoding="utf-8"))
        return self._manifest_from_data(data, path.parent, catalog=catalog)

    def _manifest_from_data(self, data: dict[str, object], path: Path, *, catalog: str) -> Manifest:
        return Manifest(
            id=str(data["id"]),
            name=str(data.get("name") or data["id"]),
            version=str(data.get("version") or "0.0.0"),
            type=data.get("type", "module"),  # type: ignore[arg-type]
            path=path,
            description=str(data.get("description") or ""),
            requires=tuple(str(item) for item in data.get("requires", []) or []),
            adapters=tuple(str(item) for item in data.get("adapters", []) or []),
            capabilities=tuple(str(item) for item in data.get("capabilities", []) or []),
            enabled_by_default=bool(data.get("enabled_by_default", False)),
            settings_schema=data.get("settings_schema") if isinstance(data.get("settings_schema"), dict) else None,
            entrypoint=str(data.get("entrypoint") or "") or None,
            catalog=catalog,
        )

    def _repo_manifests(self, base: str, pattern: str, *, catalog: str) -> list[Manifest]:
        folder = self.project_root / base
        if not folder.exists():
            return []
        return [self._load_json(path, catalog=catalog) for path in sorted(folder.glob(pattern))]

    def _package_manifests(self, package: str, marker: str, *, kind: str) -> list[Manifest]:
        result: list[Manifest] = []
        try:
            base = resources.files(package)
        except ModuleNotFoundError:
            return result
        for child in base.iterdir():
            if not child.is_dir():
                continue
            candidate = child / marker
            if not candidate.is_file():
                continue
            data = json.loads(candidate.read_text(encoding="utf-8"))
            # resources.abc.Traversable may not be pathlib.Path inside zip wheels; use a logical path.
            result.append(self._manifest_from_data(data, Path(package.replace(".", "/")) / child.name, catalog=kind))
        return result

    def _runtime_catalog_manifests(self, kind: ManifestKind, marker: str) -> list[Manifest]:
        settings = self.settings
        paths = settings.runtime_catalog_paths if settings is not None else [Path("runtime/catalog")]
        result: list[Manifest] = []
        for catalog_root in paths:
            root = catalog_root if catalog_root.is_absolute() else self.project_root / catalog_root
            for path in sorted((root / f"{kind}s").glob(f"*/{marker}")):
                result.append(self._load_json(path, catalog="runtime"))
            # compatibility layout: runtime/catalog/<id>/plugin.json or module.json
            for path in sorted(root.glob(f"*/{marker}")):
                result.append(self._load_json(path, catalog="runtime"))
        return result

    def _dedupe(self, manifests: Iterable[Manifest]) -> list[Manifest]:
        by_key: dict[str, Manifest] = {}
        priority = {"package": 0, "repo": 1, "runtime": 2}
        for manifest in manifests:
            key = manifest.key()
            current = by_key.get(key)
            if current is None or priority.get(manifest.catalog, 0) >= priority.get(current.catalog, 0):
                by_key[key] = manifest
        return sorted(by_key.values(), key=lambda item: item.key())

    def modules(self) -> list[Manifest]:
        items = [*self._package_manifests("modules", "module.json", kind="package")]
        if self.settings is None or self.settings.registry_repo_root_fallback:
            items.extend(self._repo_manifests("modules", "*/module.json", catalog="repo"))
        items.extend(self._runtime_catalog_manifests("module", "module.json"))
        return self._dedupe(items)

    def plugins(self) -> list[Manifest]:
        items = [*self._package_manifests("plugins", "plugin.json", kind="package")]
        if self.settings is None or self.settings.registry_repo_root_fallback:
            items.extend(self._repo_manifests("plugins", "*/plugin.json", catalog="repo"))
        items.extend(self._runtime_catalog_manifests("plugin", "plugin.json"))
        return self._dedupe(items)

    def adapters(self) -> list[Manifest]:
        items = [*self._package_manifests("bots", "adapter.json", kind="package")]
        if self.settings is None or self.settings.registry_repo_root_fallback:
            items.extend(self._repo_manifests("bots", "*/adapter.json", catalog="repo"))
        return self._dedupe(items)

    def all(self) -> list[Manifest]:
        return [*self.adapters(), *self.modules(), *self.plugins()]

    def _by_type(self) -> dict[str, dict[str, Manifest]]:
        manifests = self.all()
        return {
            "adapter": {manifest.id: manifest for manifest in manifests if manifest.type == "adapter"},
            "module": {manifest.id: manifest for manifest in manifests if manifest.type == "module"},
            "plugin": {manifest.id: manifest for manifest in manifests if manifest.type == "plugin"},
        }

    def load_order(self) -> list[Manifest]:
        by_type = self._by_type()
        candidates = {
            **{f"module:{k}": v for k, v in by_type["module"].items()},
            **{f"plugin:{k}": v for k, v in by_type["plugin"].items()},
        }
        result: list[Manifest] = []
        temporary: set[str] = set()
        permanent: set[str] = set()

        def visit(key: str) -> None:
            if key in permanent:
                return
            if key in temporary:
                raise ValueError(f"циклическая зависимость manifest: {key}")
            manifest = candidates.get(key)
            if manifest is None:
                return
            temporary.add(key)
            for dependency in manifest.dependencies():
                if dependency.type in {"module", "plugin"}:
                    visit(dependency.normalized())
            temporary.remove(key)
            permanent.add(key)
            result.append(manifest)

        for key in sorted(candidates):
            visit(key)
        return result

    def validate(self, settings: Settings | None = None) -> list[str]:
        errors: list[str] = []
        manifests = self.all()
        by_type = self._by_type()
        adapters = by_type["adapter"]
        modules = by_type["module"]
        plugins = by_type["plugin"]
        seen: set[str] = set()

        for manifest in manifests:
            key = manifest.key()
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
                for raw_dependency in manifest.requires:
                    if ":" not in raw_dependency:
                        errors.append(f"{manifest.type} {manifest.id} содержит неявную зависимость {raw_dependency!r}; используйте module:<id>, plugin:<id> или adapter:<id>")
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
                if manifest.entrypoint is not None and ":" not in manifest.entrypoint:
                    errors.append(f"entrypoint у {manifest.type}:{manifest.id} должен иметь формат module.path:ClassName")

        try:
            self.load_order()
        except ValueError as exc:
            errors.append(str(exc))

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
