from __future__ import annotations

import hashlib
import json
import shutil
import tarfile
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from core.catalog_signing import sign_catalog_payload, verify_catalog_signature


@dataclass
class CatalogEntry:
    id: str
    version: str
    source: str
    sha256: str = ""
    signature: str = ""
    installed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    previous_version: str = ""
    capabilities: list[str] = field(default_factory=list)
    enabled: bool = True

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class RuntimeCatalogLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.entries: list[CatalogEntry] = []
        self.history: list[CatalogEntry] = []
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self.entries = []
            self.history = []
            return
        data = json.loads(self.path.read_text(encoding="utf-8"))
        self.entries = [CatalogEntry(**item) for item in data.get("entries", [])]
        self.history = [CatalogEntry(**item) for item in data.get("history", [])]

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(
                {"entries": [item.to_dict() for item in self.entries], "history": [item.to_dict() for item in self.history[-50:]]},
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    def _sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _safe_extract(self, artifact: Path, target: Path) -> None:
        target.mkdir(parents=True, exist_ok=True)
        with tarfile.open(artifact, "r:gz") as tf:
            root = target.resolve()
            for member in tf.getmembers():
                member_path = Path(member.name)
                if member_path.is_absolute() or ".." in member_path.parts:
                    raise ValueError(f"небезопасный путь catalog artifact: {member.name}")
                if member.issym() or member.islnk() or member.isdev() or member.isfifo():
                    raise ValueError(f"запрещённый тип файла catalog artifact: {member.name}")
                resolved = (target / member.name).resolve()
                if root not in resolved.parents and resolved != root:
                    raise ValueError(f"catalog artifact пишет вне target: {member.name}")
            tf.extractall(target)

    def _download(self, source: str, destination: Path) -> Path:
        if source.startswith(("http://", "https://")):
            destination.parent.mkdir(parents=True, exist_ok=True)
            with urllib.request.urlopen(source, timeout=30) as response:  # noqa: S310 - operator-provided source
                destination.write_bytes(response.read())
            return destination
        return Path(source)

    def update(self, entry: CatalogEntry) -> None:
        previous = next((item for item in self.entries if item.id == entry.id), None)
        if previous:
            entry.previous_version = previous.version
            self.history.append(previous)
        self.entries = [item for item in self.entries if item.id != entry.id]
        self.entries.append(entry)
        self.save()

    def install(self, entry: CatalogEntry, *, project_root: Path) -> dict[str, object]:
        target = project_root / "runtime" / "catalog" / "plugins" / entry.id / entry.version
        if entry.source and entry.source not in {"local", "manual"}:
            artifact = self._download(entry.source, project_root / "runtime" / "catalog" / "downloads" / f"{entry.id}-{entry.version}.tar.gz")
            actual = self._sha256(artifact)
            if entry.sha256 and actual.lower() != entry.sha256.lower():
                raise ValueError(f"sha256 не совпадает для {entry.id}: {actual}")
            if entry.signature:
                ok, message = verify_catalog_signature(actual, entry.signature, required=True)
                if not ok:
                    raise ValueError(f"signature не прошла проверку для {entry.id}: {message}")
            elif entry.sha256:
                entry.signature = sign_catalog_payload(actual)
            if target.exists():
                shutil.rmtree(target)
            self._safe_extract(artifact, target)
            if not entry.sha256:
                entry.sha256 = actual
        self.update(entry)
        return {"ok": True, "entry": entry.to_dict(), "target": str(target)}

    def verify(self, *, project_root: Path, entry_id: str | None = None) -> dict[str, object]:
        results: list[dict[str, object]] = []
        for entry in self.entries:
            if entry_id and entry.id != entry_id:
                continue
            target = project_root / "runtime" / "catalog" / "plugins" / entry.id / entry.version
            signature_ok, signature_message = verify_catalog_signature(entry.sha256, entry.signature, required=bool(entry.signature))
            results.append({
                "id": entry.id,
                "version": entry.version,
                "source": entry.source,
                "installed": target.exists() or entry.source in {"local", "manual"},
                "sha256": entry.sha256,
                "signature": bool(entry.signature),
                "signature_ok": signature_ok,
                "signature_message": signature_message,
            })
        return {"ok": all(item["installed"] for item in results), "items": results}

    def set_enabled(self, entry_id: str, enabled: bool) -> dict[str, object]:
        entry = next((item for item in self.entries if item.id == entry_id), None)
        if entry is None:
            return {"ok": False, "error": "entry not found", "id": entry_id}
        entry.enabled = enabled
        self.save()
        return {"ok": True, "entry": entry.to_dict()}

    def rollback(self, entry_id: str) -> dict[str, object]:
        current = next((item for item in self.entries if item.id == entry_id), None)
        previous = next((item for item in reversed(self.history) if item.id == entry_id), None)
        if previous is None:
            return {"ok": False, "error": "previous version not found", "id": entry_id}
        self.entries = [item for item in self.entries if item.id != entry_id]
        self.entries.append(previous)
        if current:
            self.history.append(current)
        self.save()
        return {"ok": True, "entry": previous.to_dict()}

    def snapshot(self) -> list[dict[str, object]]:
        return [item.to_dict() for item in self.entries]
