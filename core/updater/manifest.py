from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ReleaseArtifact:
    name: str
    url: str = ""
    sha256: str = ""
    size: int | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ReleaseManifest:
    name: str
    version: str
    channel: str
    python: str
    db_contract: str
    event_contract: str
    requires_migration: bool
    required_alembic_revision: str = ""
    artifacts: list[ReleaseArtifact] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ReleaseManifest":
        artifacts = [ReleaseArtifact(**item) for item in data.get("artifacts", [])]
        return cls(
            name=str(data.get("name") or "CajeerBots"),
            version=str(data.get("version") or "0.0.0"),
            channel=str(data.get("channel") or "stable"),
            python=str(data.get("python") or ">=3.11"),
            db_contract=str(data.get("db_contract") or ""),
            event_contract=str(data.get("event_contract") or ""),
            requires_migration=bool(data.get("requires_migration", False)),
            required_alembic_revision=str(data.get("required_alembic_revision") or ""),
            artifacts=artifacts,
        )

    @classmethod
    def from_file(cls, path: Path) -> "ReleaseManifest":
        return cls.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["artifacts"] = [item.to_dict() for item in self.artifacts]
        return data

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2)


@dataclass
class UpdateStatus:
    current_version: str
    available_version: str | None = None
    channel: str = "stable"
    source: str = "github"
    last_action: str | None = None
    last_error: str | None = None
    staged_path: str | None = None
    previous_version: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
