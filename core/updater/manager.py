from __future__ import annotations

import hashlib
import json
import os
import shutil
import tarfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.request import urlretrieve

from core.contracts import DB_CONTRACT_VERSION, EVENT_CONTRACT_VERSION_ID
from core.updater.github import GitHubReleaseSource
from core.updater.manifest import ReleaseManifest, UpdateStatus


@dataclass(frozen=True)
class UpdateHistoryRecord:
    action: str
    result: str
    version: str | None
    message: str
    created_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class UpdateManager:
    """Безопасный staged updater для GitHub Releases и локальных tar.gz-артефактов."""

    def __init__(self, runtime: Any) -> None:
        self.runtime = runtime
        self.settings = runtime.settings
        self.root = runtime.project_root
        self.install_root = Path(os.getenv("CAJEER_UPDATE_INSTALL_ROOT", str(self.settings.runtime_dir / "updates"))).resolve()
        self.releases_dir = self.install_root / "releases"
        self.staging_dir = self.install_root / "staging"
        self.history_path = self.install_root / "history.jsonl"
        self.repo = os.getenv("CAJEER_UPDATE_REPO", "CajeerTeam/CajeerBots")
        self.channel = os.getenv("CAJEER_UPDATE_CHANNEL", "stable")
        self.source = os.getenv("CAJEER_UPDATE_SOURCE", "github")
        self.allow_prerelease = os.getenv("CAJEER_UPDATE_ALLOW_PRERELEASE", "false").lower() in {"1", "true", "yes", "on"}

    def status(self) -> UpdateStatus:
        available = None
        history = self.history()[-1:]
        last = history[0] if history else None
        return UpdateStatus(
            current_version=self.runtime.version,
            available_version=available,
            channel=self.channel,
            source=self.source,
            last_action=last.action if last else None,
            last_error=last.message if last and last.result == "error" else None,
        )

    def history(self) -> list[UpdateHistoryRecord]:
        if not self.history_path.exists():
            return []
        records: list[UpdateHistoryRecord] = []
        for line in self.history_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            records.append(UpdateHistoryRecord(**json.loads(line)))
        return records[-100:]

    def _record(self, action: str, result: str, version: str | None = None, message: str = "") -> None:
        self.install_root.mkdir(parents=True, exist_ok=True)
        item = UpdateHistoryRecord(action, result, version, message, datetime.now(timezone.utc).isoformat())
        with self.history_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(item.to_dict(), ensure_ascii=False) + "\n")
        self.runtime.audit.write(actor_type="system", actor_id="updater", action=f"update.{action}", resource=version or "latest", result=result, message=message)
        event_payload = {"action": action, "result": result, "version": version or "", "message": message}
        # Fire-and-forget integrations. Они не должны ломать локальный updater.
        try:
            event = self.runtime.make_system_event(f"cajeer.bots.update.{action}", event_payload)
            import asyncio
            loop = asyncio.get_running_loop()
            loop.create_task(self.runtime.workspace.report_event(event))
            loop.create_task(self.runtime.remote_logs.emit_event(event, level="INFO" if result == "ok" else "ERROR"))
        except Exception:
            pass

    def check(self) -> dict[str, object]:
        if self.source == "local":
            artifact = os.getenv("CAJEER_UPDATE_LOCAL_ARTIFACT", "")
            manifest_path = os.getenv("CAJEER_UPDATE_LOCAL_MANIFEST", "")
            manifest = ReleaseManifest.from_file(Path(manifest_path)) if manifest_path else None
            self._record("checked", "ok", manifest.version if manifest else None, "local")
            return {"source": "local", "artifact": artifact, "manifest": manifest.to_dict() if manifest else None}
        manifest = GitHubReleaseSource(self.repo, self.channel, self.allow_prerelease).fetch_latest()
        self._record("checked", "ok" if manifest else "error", manifest.version if manifest else None, "github release найден" if manifest else "github release не найден")
        return {"source": "github", "repo": self.repo, "manifest": manifest.to_dict() if manifest else None}

    def _sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def verify(self, artifact: Path, expected_sha256: str | None) -> dict[str, object]:
        actual = self._sha256(artifact)
        ok = not expected_sha256 or actual.lower() == expected_sha256.lower()
        self._record("verified", "ok" if ok else "error", None, actual)
        return {"ok": ok, "sha256": actual, "expected": expected_sha256 or ""}

    def preflight(self, manifest: ReleaseManifest | None = None) -> list[str]:
        problems: list[str] = []
        if manifest:
            if manifest.db_contract and manifest.db_contract != DB_CONTRACT_VERSION:
                problems.append(f"DB contract несовместим: {manifest.db_contract} != {DB_CONTRACT_VERSION}")
            if manifest.event_contract and manifest.event_contract != EVENT_CONTRACT_VERSION_ID:
                problems.append(f"Event contract несовместим: {manifest.event_contract} != {EVENT_CONTRACT_VERSION_ID}")
        problems.extend(self.runtime.doctor(offline=True))
        self._record("preflight", "ok" if not problems else "error", manifest.version if manifest else None, "; ".join(problems[:5]))
        return problems

    def stage_local_artifact(self, artifact_path: Path, manifest: ReleaseManifest | None = None, expected_sha256: str | None = None) -> dict[str, object]:
        artifact_path = artifact_path.resolve()
        if not artifact_path.exists():
            raise FileNotFoundError(str(artifact_path))
        verify = self.verify(artifact_path, expected_sha256)
        if not verify["ok"]:
            raise ValueError("checksum artifact не совпадает")
        problems = self.preflight(manifest)
        if problems:
            raise RuntimeError("preflight не пройден: " + "; ".join(problems))
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        target = self.staging_dir / artifact_path.name.replace(".tar.gz", "")
        if target.exists():
            shutil.rmtree(target)
        with tarfile.open(artifact_path, "r:gz") as tf:
            tf.extractall(target)
        self._record("staged", "ok", manifest.version if manifest else None, str(target))
        return {"staged_path": str(target), "manifest": manifest.to_dict() if manifest else None}

    def apply_staged(self, version: str, staged_path: str) -> dict[str, object]:
        self.releases_dir.mkdir(parents=True, exist_ok=True)
        release_path = self.releases_dir / version
        if release_path.exists():
            shutil.rmtree(release_path)
        shutil.copytree(staged_path, release_path)
        current = self.install_root / "current"
        previous = self.install_root / "previous"
        if current.exists() or current.is_symlink():
            if previous.exists() or previous.is_symlink():
                previous.unlink()
            previous.symlink_to(current.resolve(), target_is_directory=True)
            current.unlink()
        current.symlink_to(release_path, target_is_directory=True)
        self._record("applied", "ok", version, str(release_path))
        return {"ok": True, "current": str(current), "release": str(release_path)}

    def rollback(self) -> dict[str, object]:
        current = self.install_root / "current"
        previous = self.install_root / "previous"
        if not previous.exists():
            self._record("rollback_completed", "error", None, "previous release отсутствует")
            return {"ok": False, "error": "previous release отсутствует"}
        if current.exists() or current.is_symlink():
            current.unlink()
        current.symlink_to(previous.resolve(), target_is_directory=True)
        self._record("rollback_completed", "ok", None, str(previous.resolve()))
        return {"ok": True, "current": str(current), "target": str(previous.resolve())}
