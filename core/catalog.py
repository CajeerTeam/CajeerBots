from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class CatalogEntry:
    id: str
    version: str
    source: str
    sha256: str = ""
    signature: str = ""
    installed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class RuntimeCatalogLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.entries: list[CatalogEntry] = []
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self.entries = []
            return
        data = json.loads(self.path.read_text(encoding="utf-8"))
        self.entries = [CatalogEntry(**item) for item in data.get("entries", [])]

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"entries": [item.to_dict() for item in self.entries]}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def update(self, entry: CatalogEntry) -> None:
        self.entries = [item for item in self.entries if item.id != entry.id]
        self.entries.append(entry)
        self.save()

    def snapshot(self) -> list[dict[str, object]]:
        return [item.to_dict() for item in self.entries]
