#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.permissions_fix import fix_permissions  # noqa: E402
from core.secrets import generate_env_block  # noqa: E402


def main() -> int:
    root = PROJECT_ROOT
    env = root / ".env"
    if not env.exists():
        base = (root / ".env.example").read_text(encoding="utf-8")
        env.write_text(base.rstrip() + "\n\n# Сгенерированные секреты\n" + generate_env_block(), encoding="utf-8")
        print("Создан файл .env из .env.example и сгенерированы секреты")
    for relative in ["runtime", "runtime/catalog", "runtime/catalog/modules", "runtime/catalog/plugins", "runtime/tmp", "runtime/secrets"]:
        (root / relative).mkdir(parents=True, exist_ok=True)
    changed = fix_permissions(root)
    if changed:
        print("Исправлены права:", ", ".join(changed))
    print("Начальная настройка Cajeer Bots завершена")
    print("Дальше: scripts/install.sh, python -m core doctor --offline, python -m core rbac bootstrap-owner ...")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
