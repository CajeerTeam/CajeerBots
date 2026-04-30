#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

from core.permissions_fix import fix_permissions
from core.secrets import generate_env_block


def main() -> int:
    root = Path(__file__).resolve().parent
    env = root / ".env"
    if not env.exists():
        base = (root / ".env.example").read_text(encoding="utf-8")
        env.write_text(base.rstrip() + "\n\n# Сгенерированные секреты\n" + generate_env_block(), encoding="utf-8")
        print("Создан файл .env из .env.example и сгенерированы секреты")
    for relative in ["runtime", "runtime/catalog", "runtime/catalog/modules", "runtime/catalog/plugins"]:
        (root / relative).mkdir(parents=True, exist_ok=True)
    changed = fix_permissions(root)
    if changed:
        print("Исправлены права:", ", ".join(changed))
    print("Начальная настройка Cajeer Bots завершена")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
