from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class JsonFileStateStore:
    """Small persistent state backend for distributed control-plane dev/prod-lite.

    It keeps node registry and command leases durable across server restarts.
    Larger installations should replace it with Redis/PostgreSQL.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"nodes": {}, "commands": {}, "leases": {}}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {"nodes": {}, "commands": {}, "leases": {}}
        except json.JSONDecodeError:
            return {"nodes": {}, "commands": {}, "leases": {}}

    def save(self, data: dict[str, Any]) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(self.path)
