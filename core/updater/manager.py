from __future__ import annotations

import hashlib
import json
import os
import shutil
import tarfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from core.contracts import DB_CONTRACT_VERSION, EVENT_CONTRACT_VERSION_ID
from core.updater.github import GitHubReleaseSource
from core.updater.lock import FileUpdateLock
from core.updater.manifest import ReleaseManifest, UpdateStatus
from core.updater.services import build_service_manager


@dataclass(frozen=True)
class UpdateHistoryRecord:
    action: str
    result: str
    version: str | None
    message: str
    created_at: str
    old_version: str | None = None
    new_version: str | None = None
    artifact_name: str | None = None
    artifact_sha256: str | None = None
    actor: str = "system"
    source: str = "github"
    channel: str = "stable"
    started_at: str | None = None
    finished_at: str | None = None
    duration_ms: int | None = None
    preflight_result: str | None = None
    rollback_available: bool = False

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
        self.downloads_dir = self.install_root / "downloads"
        self.staging_dir = self.install_root / "staging"
        self.history_path = self.install_root / "history.jsonl"
        self.lock_path = self.install_root / "update.lock"
        self.repo = os.getenv("CAJEER_UPDATE_REPO", "CajeerTeam/CajeerBots")
        self.channel = os.getenv("CAJEER_UPDATE_CHANNEL", "stable")
        self.source = os.getenv("CAJEER_UPDATE_SOURCE", "github")
        self.allow_prerelease = os.getenv("CAJEER_UPDATE_ALLOW_PRERELEASE", "false").lower() in {"1", "true", "yes", "on"}
        self.require_signature = os.getenv("CAJEER_UPDATE_REQUIRE_SIGNATURE", "false").lower() in {"1", "true", "yes", "on"}

    def status(self) -> UpdateStatus:
        history = self.history()[-1:]
        last = history[0] if history else None
        return UpdateStatus(
            current_version=self.runtime.version,
            available_version=None,
            channel=self.channel,
            source=self.source,
            last_action=last.action if last else None,
            last_error=last.message if last and last.result == "error" else None,
            staged_path=str(self.staging_dir) if self.staging_dir.exists() else None,
            previous_version=self._read_previous_version(),
        )

    def history(self) -> list[UpdateHistoryRecord]:
        if not self.history_path.exists():
            return []
        records: list[UpdateHistoryRecord] = []
        for line in self.history_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            data = json.loads(line)
            # Backward compatibility with older compact records.
            allowed = set(UpdateHistoryRecord.__dataclass_fields__.keys())
            records.append(UpdateHistoryRecord(**{k: v for k, v in data.items() if k in allowed}))
        return records[-200:]

    def _record(self, action: str, result: str, version: str | None = None, message: str = "", **extra: Any) -> None:
        self.install_root.mkdir(parents=True, exist_ok=True)
        now = datetime.now(timezone.utc).isoformat()
        item = UpdateHistoryRecord(action, result, version, message, now, source=self.source, channel=self.channel, **extra)
        with self.history_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(item.to_dict(), ensure_ascii=False) + "\n")
        self.runtime.audit.write(actor_type="system", actor_id="updater", action=f"update.{action}", resource=version or "latest", result=result, message=message)
        try:
            event = self.runtime.make_system_event(f"cajeer.bots.update.{action}", item.to_dict())
            import asyncio
            loop = asyncio.get_running_loop()
            loop.create_task(self.runtime.workspace.report_event(event))
            loop.create_task(self.runtime.remote_logs.emit_event(event, level="INFO" if result == "ok" else "ERROR"))
        except Exception:
            pass

    def _read_previous_version(self) -> str | None:
        previous = self.install_root / "previous"
        if not previous.exists():
            return None
        version_file = previous.resolve() / "VERSION"
        return version_file.read_text(encoding="utf-8").strip() if version_file.exists() else None

    def check(self) -> dict[str, object]:
        if self.source == "local":
            artifact = os.getenv("CAJEER_UPDATE_LOCAL_ARTIFACT", "")
            manifest_path = os.getenv("CAJEER_UPDATE_LOCAL_MANIFEST", "")
            manifest = ReleaseManifest.from_file(Path(manifest_path)) if manifest_path else None
            self._record("checked", "ok", manifest.version if manifest else None, "local")
            return {"source": "local", "artifact": artifact, "manifest": manifest.to_dict() if manifest else None}
        manifest = GitHubReleaseSource(self.repo, self.channel, self.allow_prerelease).fetch_latest()
        self._record("checked", "ok" if manifest else "error", manifest.version if manifest else None, "github release найден" if manifest else "github release не найден", new_version=manifest.version if manifest else None)
        return {"source": "github", "repo": self.repo, "manifest": manifest.to_dict() if manifest else None}

    def download_latest(self) -> dict[str, object]:
        manifest = GitHubReleaseSource(self.repo, self.channel, self.allow_prerelease).fetch_latest()
        if manifest is None or not manifest.artifacts:
            self._record("downloaded", "error", None, "artifact не найден")
            return {"ok": False, "error": "artifact не найден"}
        artifact = next((item for item in manifest.artifacts if item.name.endswith(".tar.gz")), manifest.artifacts[0])
        path = GitHubReleaseSource(self.repo, self.channel, self.allow_prerelease).download_artifact(artifact, self.downloads_dir)
        verify = self.verify(path, artifact.sha256 or None)
        self._record("downloaded", "ok" if verify["ok"] else "error", manifest.version, str(path), new_version=manifest.version, artifact_name=artifact.name, artifact_sha256=str(verify["sha256"]))
        return {"ok": bool(verify["ok"]), "artifact": str(path), "manifest": manifest.to_dict(), "verify": verify}

    def _sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def verify(self, artifact: Path, expected_sha256: str | None) -> dict[str, object]:
        actual = self._sha256(artifact)
        ok = not expected_sha256 or actual.lower() == expected_sha256.lower()
        signature = artifact.with_suffix(artifact.suffix + ".sig")
        if self.require_signature and not signature.exists():
            ok = False
        self._record("verified", "ok" if ok else "error", None, actual, artifact_name=artifact.name, artifact_sha256=actual)
        return {"ok": ok, "sha256": actual, "expected": expected_sha256 or "", "signature_required": self.require_signature, "signature_present": signature.exists()}

    def preflight(self, manifest: ReleaseManifest | None = None) -> list[str]:
        problems: list[str] = []
        if manifest:
            if manifest.db_contract and manifest.db_contract != DB_CONTRACT_VERSION:
                problems.append(f"DB contract несовместим: {manifest.db_contract} != {DB_CONTRACT_VERSION}")
            if manifest.event_contract and manifest.event_contract != EVENT_CONTRACT_VERSION_ID:
                problems.append(f"Event contract несовместим: {manifest.event_contract} != {EVENT_CONTRACT_VERSION_ID}")
        problems.extend(self.runtime.doctor(offline=True))
        self._record("preflight", "ok" if not problems else "error", manifest.version if manifest else None, "; ".join(problems[:5]), preflight_result="ok" if not problems else "error")
        return problems

    def stage_local_artifact(self, artifact_path: Path, manifest: ReleaseManifest | None = None, expected_sha256: str | None = None) -> dict[str, object]:
        with FileUpdateLock(self.lock_path):
            artifact_path = artifact_path.resolve()
            if not artifact_path.exists():
                raise FileNotFoundError(str(artifact_path))
            verify = self.verify(artifact_path, expected_sha256)
            if not verify["ok"]:
                raise ValueError("checksum/signature artifact не совпадает")
            problems = self.preflight(manifest)
            if problems:
                raise RuntimeError("preflight не пройден: " + "; ".join(problems))
            self.staging_dir.mkdir(parents=True, exist_ok=True)
            target = self.staging_dir / artifact_path.name.replace(".tar.gz", "")
            if target.exists():
                shutil.rmtree(target)
            with tarfile.open(artifact_path, "r:gz") as tf:
                tf.extractall(target)
            self._record("staged", "ok", manifest.version if manifest else None, str(target), artifact_name=artifact_path.name, artifact_sha256=str(verify["sha256"]))
            return {"staged_path": str(target), "manifest": manifest.to_dict() if manifest else None, "verify": verify}

    def stage_latest(self) -> dict[str, object]:
        downloaded = self.download_latest()
        if not downloaded.get("ok"):
            return downloaded
        manifest = ReleaseManifest.from_dict(downloaded["manifest"]) if isinstance(downloaded.get("manifest"), dict) else None
        artifact = Path(str(downloaded["artifact"]))
        expected = str(downloaded.get("verify", {}).get("sha256", "")) if isinstance(downloaded.get("verify"), dict) else None
        return self.stage_local_artifact(artifact, manifest=manifest, expected_sha256=expected)

    def apply_staged(self, version: str, staged_path: str) -> dict[str, object]:
        started = time.time()
        with FileUpdateLock(self.lock_path):
            services = build_service_manager()
            stop_results = services.stop()
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
            reload_results = services.reload()
            start_results = services.start()
            health_results = services.healthcheck()
            ok = all(item.ok for item in [*stop_results, *reload_results, *start_results, *health_results])
            duration_ms = int((time.time() - started) * 1000)
            self._record("applied", "ok" if ok else "error", version, str(release_path), old_version=self.runtime.version, new_version=version, duration_ms=duration_ms, rollback_available=previous.exists())
            if not ok:
                rollback = self._rollback_unlocked(started=time.time())
                return {"ok": False, "current": str(current), "release": str(release_path), "service_results": [r.to_dict() for r in [*stop_results, *reload_results, *start_results, *health_results]], "rollback": rollback}
            return {"ok": True, "current": str(current), "release": str(release_path), "service_results": [r.to_dict() for r in [*stop_results, *reload_results, *start_results, *health_results]]}

    def _rollback_unlocked(self, *, started: float | None = None) -> dict[str, object]:
        started = started or time.time()
        current = self.install_root / "current"
        previous = self.install_root / "previous"
        if not previous.exists():
            self._record("rollback_completed", "error", None, "previous release отсутствует")
            return {"ok": False, "error": "previous release отсутствует"}
        services = build_service_manager()
        stop_results = services.stop()
        if current.exists() or current.is_symlink():
            current.unlink()
        current.symlink_to(previous.resolve(), target_is_directory=True)
        reload_results = services.reload()
        start_results = services.start()
        health_results = services.healthcheck()
        ok = all(item.ok for item in [*stop_results, *reload_results, *start_results, *health_results])
        duration_ms = int((time.time() - started) * 1000)
        self._record("rollback_completed", "ok" if ok else "error", None, str(previous.resolve()), duration_ms=duration_ms)
        return {"ok": ok, "current": str(current), "target": str(previous.resolve()), "service_results": [r.to_dict() for r in [*stop_results, *reload_results, *start_results, *health_results]]}

    def rollback(self) -> dict[str, object]:
        with FileUpdateLock(self.lock_path):
            return self._rollback_unlocked()
