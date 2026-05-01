from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class PluginArtifact:
    path: str
    sha256: str


@dataclass
class PluginLock:
    plugin_id: str
    version: str
    artifacts: list[PluginArtifact] = field(default_factory=list)
    signature_required: bool = False
    migrations: list[str] = field(default_factory=list)
    hooks: dict[str, object] = field(default_factory=dict)
    rollback: dict[str, object] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "PluginLock":
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            plugin_id=str(data["plugin_id"]),
            version=str(data["version"]),
            artifacts=[PluginArtifact(str(item["path"]), str(item["sha256"])) for item in data.get("artifacts", [])],
            signature_required=bool(data.get("signature_required", False)),
            migrations=[str(item) for item in data.get("migrations", [])],
            hooks=dict(data.get("hooks", {})),
            rollback=dict(data.get("rollback", {})),
        )

    def verify_files(self, root: Path) -> list[str]:
        errors: list[str] = []
        for artifact in self.artifacts:
            target = (root / artifact.path).resolve()
            if not target.is_file() or root.resolve() not in target.parents:
                errors.append(f"artifact отсутствует или выходит за пределы plugin root: {artifact.path}")
                continue
            digest = hashlib.sha256(target.read_bytes()).hexdigest()
            if digest != artifact.sha256:
                errors.append(f"sha256 mismatch for {artifact.path}: expected {artifact.sha256}, got {digest}")
        return errors
