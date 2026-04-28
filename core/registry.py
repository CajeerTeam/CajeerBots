from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

ManifestKind = Literal["module", "plugin", "adapter"]


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
        )

    def modules(self) -> list[Manifest]:
        return [self._load(path) for path in sorted((self.project_root / "modules").glob("*/module.json"))]

    def plugins(self) -> list[Manifest]:
        return [self._load(path) for path in sorted((self.project_root / "plugins").glob("*/plugin.json"))]

    def adapters(self) -> list[Manifest]:
        return [self._load(path) for path in sorted((self.project_root / "bots").glob("*/adapter.json"))]

    def all(self) -> list[Manifest]:
        return [*self.adapters(), *self.modules(), *self.plugins()]

    def validate(self) -> list[str]:
        errors: list[str] = []
        seen: set[str] = set()
        for manifest in self.all():
            key = f"{manifest.type}:{manifest.id}"
            if key in seen:
                errors.append(f"дублирующийся идентификатор manifest: {key}")
            seen.add(key)
            if not manifest.version:
                errors.append(f"у manifest {key} пустая версия")
            if manifest.type in {"module", "plugin", "adapter"} and not manifest.name:
                errors.append(f"у manifest {key} пустое название")
        return errors
