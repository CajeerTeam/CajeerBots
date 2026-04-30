from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

ALLOWED_PLUGIN_IMPORTS = {
    "core.sdk",
    "core.sdk.events",
    "core.sdk.modules",
    "core.sdk.permissions",
    "core.sdk.plugins",
    "core.sdk.runtime",
    "core.sdk.storage",
}
ALLOWED_TOP_LEVEL = {
    "__future__",
    "asyncio",
    "collections",
    "dataclasses",
    "datetime",
    "decimal",
    "enum",
    "functools",
    "hashlib",
    "hmac",
    "inspect",
    "itertools",
    "json",
    "logging",
    "math",
    "pathlib",
    "random",
    "re",
    "secrets",
    "string",
    "time",
    "typing",
    "uuid",
}


@dataclass
class PluginImportPolicyResult:
    ok: bool
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {"ok": self.ok, "errors": list(self.errors)}


def _imported_modules(path: Path) -> list[tuple[int, str]]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError as exc:
        return [(exc.lineno or 0, f"<syntax-error:{exc}>")]
    modules: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.append((node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append((node.lineno, node.module))
    return modules


def validate_plugin_import_policy(plugin_root: Path) -> PluginImportPolicyResult:
    errors: list[str] = []
    if not plugin_root.exists():
        return PluginImportPolicyResult(False, [f"plugin path не найден: {plugin_root}"])
    for path in sorted(plugin_root.rglob("*.py")):
        rel = path.relative_to(plugin_root)
        for line, module in _imported_modules(path):
            top = module.split(".", 1)[0]
            if module.startswith("core."):
                if module not in ALLOWED_PLUGIN_IMPORTS and not any(module.startswith(item + ".") for item in ALLOWED_PLUGIN_IMPORTS):
                    errors.append(f"{rel}:{line}: plugin может импортировать только core.sdk.*, найдено {module}")
            elif top == "core":
                errors.append(f"{rel}:{line}: прямой импорт core запрещён; используйте core.sdk")
            elif top in {"bots", "modules", "distributed"}:
                errors.append(f"{rel}:{line}: импорт {top}.* запрещён для переносимых plugin packages")
    return PluginImportPolicyResult(not errors, errors)
