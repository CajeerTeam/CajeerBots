from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ERRORS: list[str] = []

RULES = [
    ("core", "modules", "core не должен импортировать modules"),
    ("core", "plugins", "core не должен импортировать plugins"),
    ("core/adapters", "modules", "adapters не должны импортировать modules"),
    ("core/adapters", "plugins", "adapters не должны импортировать plugins"),
]


def imports(path: Path) -> set[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError as exc:
        ERRORS.append(f"{path.relative_to(ROOT)}: синтаксическая ошибка: {exc}")
        return set()
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module.split(".", 1)[0])
    return result


for source, forbidden, message in RULES:
    source_dir = ROOT / source
    if not source_dir.exists():
        continue
    forbidden_top = forbidden.split("/", 1)[0]
    for path in source_dir.rglob("*.py"):
        if path.name == "__init__.py":
            continue
        if forbidden_top in imports(path):
            ERRORS.append(f"{path.relative_to(ROOT)}: {message}")

if ERRORS:
    for error in ERRORS:
        print(f"- {error}")
    raise SystemExit(1)
print("Архитектурные правила: ok")
