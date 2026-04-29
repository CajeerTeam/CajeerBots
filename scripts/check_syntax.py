from __future__ import annotations

from pathlib import Path

ROOTS = ["core", "bots", "modules", "plugins", "distributed", "tests"]

for root_name in ROOTS:
    root = Path(root_name)
    if not root.exists():
        continue
    for path in root.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        compile(source, str(path), "exec")
print("Синтаксическая проверка Python: успешно")
