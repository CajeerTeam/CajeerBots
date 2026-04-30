#!/usr/bin/env python3
from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.plugin_policy import validate_plugin_import_policy

ERRORS: list[str] = []


def imports(path: Path) -> set[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError as exc:
        ERRORS.append(f"{path.relative_to(ROOT)}: синтаксическая ошибка: {exc}")
        return set()
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module)
    return result


def has_import(path: Path, forbidden: str) -> bool:
    for module in imports(path):
        if module == forbidden or module.startswith(forbidden + "."):
            return True
    return False


RULES = [
    ("core", "modules", "core не должен импортировать modules"),
    ("core", "plugins", "core не должен импортировать plugins"),
    ("core", "bots", "core не должен импортировать bots.* напрямую; используйте adapter/webhook registry"),
    ("core/adapters", "modules", "adapters не должны импортировать modules"),
    ("core/adapters", "plugins", "adapters не должны импортировать plugins"),
]

for source, forbidden, message in RULES:
    source_dir = ROOT / source
    if not source_dir.exists():
        continue
    for path in source_dir.rglob("*.py"):
        if path.name == "__init__.py":
            continue
        if has_import(path, forbidden):
            ERRORS.append(f"{path.relative_to(ROOT)}: {message}")

runtime_path = ROOT / "core" / "runtime.py"
if runtime_path.exists():
    text = runtime_path.read_text(encoding="utf-8")
    forbidden_runtime_imports = (
        "from core.adapters.telegram import",
        "from core.adapters.discord import",
        "from core.adapters.vkontakte import",
        "from core.adapters.fake import",
        "ADAPTER_CLASSES",
    )
    for marker in forbidden_runtime_imports:
        if marker in text:
            ERRORS.append(f"core/runtime.py: runtime не должен импортировать concrete adapter classes напрямую ({marker})")

for plugin_manifest in sorted((ROOT / "plugins").glob("*/plugin.json")):
    result = validate_plugin_import_policy(plugin_manifest.parent)
    for error in result.errors:
        ERRORS.append(f"plugins/{plugin_manifest.parent.name}/{error}")

if ERRORS:
    for error in ERRORS:
        print(f"- {error}")
    raise SystemExit(1)
print("Архитектурные правила: ok")
