from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

MANIFEST_ID_RE = re.compile(r"^[a-z][a-z0-9_\-]{1,63}$")
SEMVER_LIKE_RE = re.compile(r"^\d+\.\d+\.\d+(?:[-+][A-Za-z0-9_.-]+)?$")

KNOWN_MANIFEST_TYPES = {"module", "plugin", "adapter"}
KNOWN_PERMISSIONS = {
    "events.read",
    "events.publish",
    "delivery.enqueue",
    "delivery.send",
    "storage.read",
    "storage.write",
    "api.route.register",
    "scheduler.jobs.register",
    "audit.write",
    "config.read",
}
KNOWN_LIFECYCLE_HOOKS = {
    "on_install",
    "on_enable",
    "on_disable",
    "on_upgrade",
    "on_uninstall",
    "on_start",
    "on_stop",
    "register_commands",
    "register_event_handlers",
    "register_api_routes",
    "register_scheduled_jobs",
}


def _as_list(value: Any, field: str, errors: list[str]) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        errors.append(f"{field} должен быть массивом")
        return []
    return value


def validate_manifest_data(data: dict[str, Any], *, expected_type: str | None = None, path: str | Path = "<memory>") -> list[str]:
    errors: list[str] = []
    path_text = str(path)
    manifest_id = str(data.get("id") or "")
    manifest_type = str(data.get("type") or expected_type or "")
    version = str(data.get("version") or "")

    if not manifest_id or not MANIFEST_ID_RE.match(manifest_id):
        errors.append(f"{path_text}: id должен соответствовать {MANIFEST_ID_RE.pattern}")
    if manifest_type not in KNOWN_MANIFEST_TYPES:
        errors.append(f"{path_text}: type должен быть одним из {', '.join(sorted(KNOWN_MANIFEST_TYPES))}")
    if expected_type and manifest_type != expected_type:
        errors.append(f"{path_text}: ожидался type={expected_type}, получен {manifest_type or '<empty>'}")
    if not version or not SEMVER_LIKE_RE.match(version):
        errors.append(f"{path_text}: version должен быть semver-like, например 1.0.0")
    if not str(data.get("name") or "").strip():
        errors.append(f"{path_text}: name обязателен")

    for field in ("requires", "adapters", "capabilities", "permissions", "migrations"):
        for item in _as_list(data.get(field), field, errors):
            if not isinstance(item, str) or not item.strip():
                errors.append(f"{path_text}: {field} должен содержать только непустые строки")

    permissions = set(str(item) for item in _as_list(data.get("permissions"), "permissions", errors))
    unknown_permissions = sorted(permissions - KNOWN_PERMISSIONS)
    if unknown_permissions:
        errors.append(f"{path_text}: неизвестные permissions: {', '.join(unknown_permissions)}")

    lifecycle = data.get("lifecycle") or {}
    if lifecycle and not isinstance(lifecycle, dict):
        errors.append(f"{path_text}: lifecycle должен быть объектом")
    elif isinstance(lifecycle, dict):
        unknown_hooks = sorted(set(str(item) for item in lifecycle) - KNOWN_LIFECYCLE_HOOKS)
        if unknown_hooks:
            errors.append(f"{path_text}: неизвестные lifecycle hooks: {', '.join(unknown_hooks)}")
        for hook, value in lifecycle.items():
            if not isinstance(value, str) or not value.strip():
                errors.append(f"{path_text}: lifecycle.{hook} должен быть строкой с entrypoint")

    compatibility = data.get("compatibility") or data.get("requires_contracts") or {}
    if compatibility and not isinstance(compatibility, dict):
        errors.append(f"{path_text}: compatibility должен быть объектом")
    elif isinstance(compatibility, dict):
        for key in ("platform", "db_contract", "event_contract"):
            if key in compatibility and not str(compatibility.get(key) or "").strip():
                errors.append(f"{path_text}: compatibility.{key} не должен быть пустым")

    return errors


def validate_manifest_file(path: str | Path, *, expected_type: str | None = None) -> list[str]:
    path = Path(path)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return [f"{path}: невозможно прочитать JSON manifest: {exc}"]
    if not isinstance(data, dict):
        return [f"{path}: manifest должен быть JSON-объектом"]
    return validate_manifest_data(data, expected_type=expected_type, path=path)
