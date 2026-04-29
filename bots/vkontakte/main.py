from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.cli import main as _main  # noqa: E402

CORE_COMMANDS = {"run", "doctor", "modules", "plugins", "adapters", "commands", "migrate", "db-status", "distributed"}


def main() -> int:
    args = sys.argv[1:]
    if args and args[0] in CORE_COMMANDS:
        return _main(args)
    return _main(["run", "vkontakte", *args])


if __name__ == "__main__":
    raise SystemExit(main())
