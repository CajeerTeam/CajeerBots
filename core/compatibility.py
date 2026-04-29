from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from core.events import EVENT_CONTRACT_VERSION


@dataclass(frozen=True)
class CompatibilityReport:
    errors: list[str]
    warnings: list[str]

    @property
    def ok(self) -> bool:
        return not self.errors


def _read_simple_yaml(path: Path) -> dict[str, object]:
    """Минимальный YAML-reader для compatibility.yaml без обязательной зависимости от PyYAML."""
    result: dict[str, object] = {}
    stack: list[tuple[int, dict[str, object]]] = [(-1, result)]
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
            child: dict[str, object] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = value
    return result


def check_compatibility(project_root: Path, platform_version: str) -> CompatibilityReport:
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

    return CompatibilityReport(errors, warnings)
