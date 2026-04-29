from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from core.events import EVENT_CONTRACT_VERSION
from core.registry import Registry


@dataclass(frozen=True)
class CompatibilityReport:
    errors: list[str]
    warnings: list[str]

    @property
    def ok(self) -> bool:
        return not self.errors


def _parse_inline_map(value: str) -> dict[str, str]:
    value = value.strip()
    if not (value.startswith("{") and value.endswith("}")):
        return {}
    result: dict[str, str] = {}
    body = value[1:-1].strip()
    for part in body.split(","):
        if ":" not in part:
            continue
        key, raw = part.split(":", 1)
        result[key.strip()] = raw.strip().strip('"')
    return result


def _read_simple_yaml(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, result)]
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        line = raw_line.strip()
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip().strip('"')
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if not value:
            child: dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = _parse_inline_map(value) or value
    return result


def _version_tuple(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in re.findall(r"\d+", version)[:3] or [0])


def _satisfies(version: str, spec: object) -> bool:
    if isinstance(spec, dict):
        min_v = str(spec.get("min", "")).strip()
        max_v = str(spec.get("max", "")).strip()
        if min_v and _version_tuple(version) < _version_tuple(min_v):
            return False
        if max_v and max_v.endswith(".x"):
            prefix = max_v[:-2]
            return version.startswith(prefix + ".") or version == prefix
        if max_v and _version_tuple(version) > _version_tuple(max_v):
            return False
        return True
    text = str(spec).strip()
    if not text:
        return True
    if text.startswith(">="):
        return _version_tuple(version) >= _version_tuple(text[2:])
    if text.endswith(".x"):
        prefix = text[:-2]
        return version.startswith(prefix + ".") or version == prefix
    return version == text


def check_compatibility(project_root: Path, platform_version: str, registry: Registry | None = None) -> CompatibilityReport:
    path = project_root / "compatibility.yaml"
    errors: list[str] = []
    warnings: list[str] = []
    if not path.exists():
        return CompatibilityReport(["compatibility.yaml не найден"], warnings)

    data = _read_simple_yaml(path)
    expected_platform = str(data.get("platform", "")).strip()
    if expected_platform and expected_platform != platform_version:
        warnings.append(f"версия платформы {platform_version} отличается от compatibility.yaml ({expected_platform})")

    raw_event_contract = str(data.get("event_contract", "")).strip()
    if raw_event_contract:
        try:
            expected_event_contract = int(raw_event_contract)
        except ValueError:
            errors.append("event_contract в compatibility.yaml должен быть числом")
        else:
            if expected_event_contract != EVENT_CONTRACT_VERSION:
                errors.append(
                    f"версия контракта событий {EVENT_CONTRACT_VERSION} не совпадает с compatibility.yaml ({expected_event_contract})"
                )

    if str(data.get("db_contract", "")).strip() != "external":
        warnings.append("db_contract должен быть external, так как миграции не входят в проект")

    py_spec = str(data.get("python", "")).strip()
    if py_spec.startswith(">=") and _version_tuple(".".join(map(str, sys.version_info[:3]))) < _version_tuple(py_spec[2:]):
        errors.append(f"текущая версия Python не соответствует compatibility.yaml: {py_spec}")

    for section_name in ("adapters", "modules", "plugins"):
        if section_name not in data:
            errors.append(f"секция {section_name} отсутствует в compatibility.yaml")

    if registry is not None:
        sections = {
            "adapter": data.get("adapters", {}),
            "module": data.get("modules", {}),
            "plugin": data.get("plugins", {}),
        }
        for manifest in registry.all():
            section = sections.get(manifest.type, {})
            if not isinstance(section, dict) or manifest.id not in section:
                errors.append(f"{manifest.type}:{manifest.id} отсутствует в compatibility.yaml")
                continue
            if not _satisfies(manifest.version, section[manifest.id]):
                errors.append(f"версия {manifest.type}:{manifest.id}={manifest.version} не соответствует compatibility.yaml")
    return CompatibilityReport(errors, warnings)
