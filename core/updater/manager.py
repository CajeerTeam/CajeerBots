from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
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
        self.state_path = self.install_root / "state.json"
        self.lock_path = self.install_root / "update.lock"
        self.repo = os.getenv("CAJEER_UPDATE_REPO", "CajeerTeam/CajeerBots")
        self.channel = os.getenv("CAJEER_UPDATE_CHANNEL", "stable")
        self.source = os.getenv("CAJEER_UPDATE_SOURCE", "github")
        self.allow_prerelease = os.getenv("CAJEER_UPDATE_ALLOW_PRERELEASE", "false").lower() in {"1", "true", "yes", "on"}
        self.require_signature = os.getenv("CAJEER_UPDATE_REQUIRE_SIGNATURE", "false").lower() in {"1", "true", "yes", "on"}
        self.public_key = Path(os.getenv("CAJEER_UPDATE_PUBLIC_KEY", "runtime/secrets/release-public.pem"))
        self.auto_migrate = os.getenv("CAJEER_UPDATE_AUTO_MIGRATE", "false").lower() in {"1", "true", "yes", "on"}
        self.block_on_required_migration = os.getenv("CAJEER_UPDATE_BLOCK_ON_REQUIRED_MIGRATION", "true").lower() in {"1", "true", "yes", "on"}

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def status(self) -> UpdateStatus:
        history = self.history()[-1:]
        last = history[0] if history else None
        state = self._read_state()
        return UpdateStatus(
            current_version=self.runtime.version,
            available_version=str(state.get("available_version")) if state.get("available_version") else None,
            channel=self.channel,
            source=self.source,
            last_action=last.action if last else None,
            last_error=last.message if last and last.result == "error" else None,
            staged_path=str(state.get("staged_path") or self.staging_dir if self.staging_dir.exists() else "") or None,
            previous_version=self._read_previous_version(),
        )

    def history(self) -> list[UpdateHistoryRecord]:
        if not self.history_path.exists():
            return []
        records: list[UpdateHistoryRecord] = []
        allowed = set(UpdateHistoryRecord.__dataclass_fields__.keys())
        for line in self.history_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            data = json.loads(line)
            records.append(UpdateHistoryRecord(**{k: v for k, v in data.items() if k in allowed}))
        return records[-int(os.getenv("UPDATE_HISTORY_RETENTION", "200")) :]

    def _record(self, action: str, result: str, version: str | None = None, message: str = "", **extra: Any) -> None:
        self.install_root.mkdir(parents=True, exist_ok=True)
        now = self._now()
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

    def _read_state(self) -> dict[str, object]:
        if not self.state_path.exists():
            return {}
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _write_state(self, **changes: object) -> None:
        self.install_root.mkdir(parents=True, exist_ok=True)
        state = self._read_state()
        state.update(changes)
        state["updated_at"] = self._now()
        self.state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

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
            self._write_state(source="local", artifact=artifact, available_version=manifest.version if manifest else None, manifest=manifest.to_dict() if manifest else None)
            self._record("checked", "ok", manifest.version if manifest else None, "local")
            return {"source": "local", "artifact": artifact, "manifest": manifest.to_dict() if manifest else None}
        manifest = GitHubReleaseSource(self.repo, self.channel, self.allow_prerelease).fetch_latest()
        self._write_state(source="github", repo=self.repo, available_version=manifest.version if manifest else None, manifest=manifest.to_dict() if manifest else None)
        self._record("checked", "ok" if manifest else "error", manifest.version if manifest else None, "github release найден" if manifest else "github release не найден", new_version=manifest.version if manifest else None)
        return {"source": "github", "repo": self.repo, "manifest": manifest.to_dict() if manifest else None}

    def plan(self, version: str = "latest") -> dict[str, object]:
        checked = self.check() if version == "latest" else {}
        manifest_data = checked.get("manifest") if isinstance(checked, dict) else None
        if not manifest_data:
            manifest_data = self._read_state().get("manifest")
        manifest = ReleaseManifest.from_dict(manifest_data) if isinstance(manifest_data, dict) else None
        problems = self.preflight(manifest, record=False)
        services = [item.strip() for item in os.getenv("CAJEER_UPDATE_SERVICES", "").split(",") if item.strip()]
        migration_required = bool(manifest.requires_migration if manifest else False)
        blocked_by_migration = bool(migration_required and self.block_on_required_migration and not self.auto_migrate)
        plan = {
            "current_version": self.runtime.version,
            "target_version": manifest.version if manifest else version,
            "channel": self.channel,
            "source": self.source,
            "artifact": manifest.artifacts[0].to_dict() if manifest and manifest.artifacts else None,
            "requires_migration": migration_required,
            "blocked_by_migration": blocked_by_migration,
            "db_contract_change": manifest.db_contract if manifest else None,
            "event_contract_change": manifest.event_contract if manifest else None,
            "services_to_restart": services,
            "preflight": {"ok": not problems, "problems": problems},
            "rollback_available": bool(self._read_previous_version()),
        }
        self._write_state(plan=plan)
        self._record("planned", "ok" if not problems and not blocked_by_migration else "error", manifest.version if manifest else version, "update plan")
        return plan

    def download_latest(self) -> dict[str, object]:
        manifest = GitHubReleaseSource(self.repo, self.channel, self.allow_prerelease).fetch_latest()
        if manifest is None or not manifest.artifacts:
            self._record("downloaded", "error", None, "artifact не найден")
            return {"ok": False, "error": "artifact не найден"}
        artifact = next((item for item in manifest.artifacts if item.name.endswith(".tar.gz")), manifest.artifacts[0])
        source = GitHubReleaseSource(self.repo, self.channel, self.allow_prerelease)
        path = source.download_artifact(artifact, self.downloads_dir)
        # Best-effort: скачать подпись рядом с tar.gz, если она опубликована как asset.
        try:
            sig_artifact = next((item for item in manifest.artifacts if item.name == artifact.name + ".sig"), None)
            if sig_artifact:
                source.download_artifact(sig_artifact, self.downloads_dir)
        except Exception:
            pass
        verify = self.verify(path, artifact.sha256 or None)
        self._write_state(available_version=manifest.version, artifact=str(path), manifest=manifest.to_dict(), verify=verify)
        self._record("downloaded", "ok" if verify["ok"] else "error", manifest.version, str(path), new_version=manifest.version, artifact_name=artifact.name, artifact_sha256=str(verify["sha256"]))
        return {"ok": bool(verify["ok"]), "artifact": str(path), "manifest": manifest.to_dict(), "verify": verify}

    def _sha256(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _signature_path(self, artifact: Path) -> Path:
        return artifact.with_name(artifact.name + ".sig")

    def _verify_signature(self, artifact: Path) -> tuple[bool, str]:
        signature = self._signature_path(artifact)
        if not signature.exists():
            return False, "signature file отсутствует"
        public_key = self.public_key if self.public_key.is_absolute() else (self.root / self.public_key)
        if not public_key.exists():
            return False, f"public key не найден: {public_key}"
        completed = subprocess.run(
            ["openssl", "dgst", "-sha256", "-verify", str(public_key), "-signature", str(signature), str(artifact)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
            check=False,
        )
        return completed.returncode == 0, completed.stdout.strip()

    def verify(self, artifact: Path, expected_sha256: str | None) -> dict[str, object]:
        actual = self._sha256(artifact)
        checksum_ok = not expected_sha256 or actual.lower() == expected_sha256.lower()
        signature = self._signature_path(artifact)
        signature_ok = True
        signature_message = "not required"
        if self.require_signature:
            signature_ok, signature_message = self._verify_signature(artifact)
        ok = checksum_ok and signature_ok
        self._record("verified", "ok" if ok else "error", None, actual, artifact_name=artifact.name, artifact_sha256=actual)
        return {
            "ok": ok,
            "sha256": actual,
            "expected": expected_sha256 or "",
            "checksum_ok": checksum_ok,
            "signature_required": self.require_signature,
            "signature_present": signature.exists(),
            "signature_ok": signature_ok,
            "signature_message": signature_message,
        }

    def _current_alembic_revision(self) -> str:
        configured = os.getenv("CAJEER_DB_CURRENT_REVISION", "").strip()
        if configured:
            return configured
        try:
            completed = subprocess.run(
                ["alembic", "-c", str(self.runtime.settings.storage.alembic_config), "current"],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=15,
                check=False,
            )
            if completed.returncode == 0:
                return completed.stdout.strip().split()[0] if completed.stdout.strip() else ""
        except Exception:
            return ""
        return ""

    def preflight(self, manifest: ReleaseManifest | None = None, *, record: bool = True) -> list[str]:
        problems: list[str] = []
        if manifest:
            if manifest.db_contract and manifest.db_contract != DB_CONTRACT_VERSION:
                problems.append(f"DB contract несовместим: {manifest.db_contract} != {DB_CONTRACT_VERSION}")
            if manifest.event_contract and manifest.event_contract != EVENT_CONTRACT_VERSION_ID:
                problems.append(f"Event contract несовместим: {manifest.event_contract} != {EVENT_CONTRACT_VERSION_ID}")
            if manifest.requires_migration and self.block_on_required_migration and not self.auto_migrate:
                problems.append("release требует миграцию БД; установите CAJEER_UPDATE_AUTO_MIGRATE=true или выполните миграцию вручную")
            required_revision = getattr(manifest, "required_alembic_revision", "")
            current_revision = self._current_alembic_revision()
            if required_revision and required_revision != "head" and current_revision and required_revision != current_revision:
                problems.append(f"Alembic revision не готова: {current_revision} != {required_revision}")
        problems.extend(self.runtime.doctor(offline=True))
        if record:
            self._record("preflight", "ok" if not problems else "error", manifest.version if manifest else None, "; ".join(problems[:5]), preflight_result="ok" if not problems else "error")
        return problems

    def _safe_extract(self, tf: tarfile.TarFile, target: Path) -> None:
        target_resolved = target.resolve()
        for member in tf.getmembers():
            member_path = Path(member.name)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise ValueError(f"небезопасный путь в архиве: {member.name}")
            resolved = (target / member.name).resolve()
            if target_resolved not in resolved.parents and resolved != target_resolved:
                raise ValueError(f"архив пытается записать вне staging: {member.name}")
            if member.issym() or member.islnk() or member.isdev() or member.isfifo():
                raise ValueError(f"release artifact содержит запрещённый тип файла: {member.name}")
            if not (member.isdir() or member.isfile()):
                raise ValueError(f"release artifact содержит неподдерживаемый тип файла: {member.name}")
        tf.extractall(target)

    def _normalized_staged_root(self, target: Path) -> Path:
        children = [item for item in target.iterdir() if item.name not in {".DS_Store"}]
        if len(children) == 1 and children[0].is_dir() and (children[0] / "VERSION").exists() and (children[0] / "core").is_dir():
            return children[0]
        return target

    def _staged_preflight(self, staged_root: Path, manifest: ReleaseManifest | None) -> list[str]:
        problems = []
        required = ["VERSION", "core"]
        for item in required:
            if not (staged_root / item).exists():
                problems.append(f"staged release не содержит {item}")
        if (staged_root / "scripts" / "check_syntax.py").exists():
            try:
                completed = subprocess.run(
                    [os.getenv("PYTHON_BIN", "python3"), "-S", "scripts/check_syntax.py"],
                    cwd=staged_root,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    timeout=30,
                    check=False,
                    env={**os.environ, "EVENT_SIGNING_SECRET": os.getenv("EVENT_SIGNING_SECRET", "staged-secret"), "API_TOKEN": os.getenv("API_TOKEN", "staged-token")},
                )
                if completed.returncode != 0:
                    problems.append("staged syntax check failed: " + completed.stdout.strip()[:500])
            except Exception as exc:
                problems.append(f"staged syntax check не выполнен: {exc}")
        problems.extend(self.preflight(manifest, record=False))
        return problems

    def stage_local_artifact(self, artifact_path: Path, manifest: ReleaseManifest | None = None, expected_sha256: str | None = None) -> dict[str, object]:
        with FileUpdateLock(self.lock_path):
            artifact_path = artifact_path.resolve()
            if not artifact_path.exists():
                raise FileNotFoundError(str(artifact_path))
            verify = self.verify(artifact_path, expected_sha256)
            if not verify["ok"]:
                raise ValueError("checksum/signature artifact не совпадает")
            self.staging_dir.mkdir(parents=True, exist_ok=True)
            target = self.staging_dir / artifact_path.name.replace(".tar.gz", "")
            if target.exists():
                shutil.rmtree(target)
            target.mkdir(parents=True, exist_ok=True)
            with tarfile.open(artifact_path, "r:gz") as tf:
                self._safe_extract(tf, target)
            staged_root = self._normalized_staged_root(target)
            problems = self._staged_preflight(staged_root, manifest)
            if problems:
                self._write_state(stage="preflight_failed", staged_path=str(staged_root), raw_staging_path=str(target), manifest=manifest.to_dict() if manifest else None, preflight={"ok": False, "problems": problems})
                raise RuntimeError("staged preflight не пройден: " + "; ".join(problems))
            if manifest:
                (staged_root / ".cajeer-update-manifest.json").write_text(manifest.to_json() + "\n", encoding="utf-8")
            result = {"staged_path": str(staged_root), "raw_staging_path": str(target), "manifest": manifest.to_dict() if manifest else None, "verify": verify, "preflight": {"ok": True, "problems": []}}
            self._write_state(stage="staged", staged_path=str(staged_root), raw_staging_path=str(target), manifest=manifest.to_dict() if manifest else None, verify=verify)
            self._record("staged", "ok", manifest.version if manifest else None, str(staged_root), artifact_name=artifact_path.name, artifact_sha256=str(verify["sha256"]))
            return result

    def stage_latest(self) -> dict[str, object]:
        downloaded = self.download_latest()
        if not downloaded.get("ok"):
            return downloaded
        manifest = ReleaseManifest.from_dict(downloaded["manifest"]) if isinstance(downloaded.get("manifest"), dict) else None
        artifact = Path(str(downloaded["artifact"]))
        expected = next((a.sha256 for a in manifest.artifacts if a.name == artifact.name), None) if manifest else None
        return self.stage_local_artifact(artifact, manifest=manifest, expected_sha256=expected)

    def _manifest_for_staged(self, staged_path: str) -> ReleaseManifest | None:
        manifest_file = Path(staged_path) / ".cajeer-update-manifest.json"
        if manifest_file.exists():
            return ReleaseManifest.from_file(manifest_file)
        state_manifest = self._read_state().get("manifest")
        return ReleaseManifest.from_dict(state_manifest) if isinstance(state_manifest, dict) else None

    def _replace_link_or_path(self, path: Path, backup_name: str | None = None) -> None:
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.exists():
            target = (self.install_root / (backup_name or f"{path.name}.backup-{int(time.time())}"))
            if target.exists():
                shutil.rmtree(target)
            shutil.move(str(path), str(target))

    def _atomic_symlink(self, link: Path, target: Path) -> None:
        tmp = link.with_name(link.name + ".new")
        self._replace_link_or_path(tmp)
        tmp.symlink_to(target, target_is_directory=True)
        os.replace(tmp, link)

    def apply_staged(self, version: str, staged_path: str, *, dry_run: bool = False) -> dict[str, object]:
        started = time.time()
        with FileUpdateLock(self.lock_path):
            if not staged_path:
                raise ValueError("staged_path обязателен")
            manifest = self._manifest_for_staged(staged_path)
            migration_problems = []
            if manifest and manifest.requires_migration and self.block_on_required_migration and not self.auto_migrate:
                migration_problems.append("release требует миграцию БД; применение заблокировано")
            if migration_problems:
                self._record("apply_blocked", "error", version, "; ".join(migration_problems), old_version=self.runtime.version, new_version=version)
                return {"ok": False, "error": "migration_required", "problems": migration_problems}
            if dry_run:
                plan = self.plan(version)
                return {"ok": True, "dry_run": True, "plan": plan, "staged_path": staged_path}
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
                self._replace_link_or_path(previous, backup_name=f"previous.backup-{int(time.time())}")
                previous.symlink_to(current.resolve(), target_is_directory=True)
            self._atomic_symlink(current, release_path)
            reload_results = services.reload()
            start_results = services.start()
            health_results = services.healthcheck()
            ok = all(item.ok for item in [*stop_results, *reload_results, *start_results, *health_results])
            duration_ms = int((time.time() - started) * 1000)
            self._write_state(stage="applied" if ok else "apply_failed", current=str(current), release=str(release_path), applied_version=version)
            self._record("applied", "ok" if ok else "error", version, str(release_path), old_version=self.runtime.version, new_version=version, duration_ms=duration_ms, rollback_available=previous.exists())
            if not ok:
                rollback = self._rollback_unlocked(started=time.time())
                return {"ok": False, "current": str(current), "release": str(release_path), "service_results": [r.to_dict() for r in [*stop_results, *reload_results, *start_results, *health_results]], "rollback": rollback}
            return {"ok": True, "current": str(current), "release": str(release_path), "service_results": [r.to_dict() for r in [*stop_results, *reload_results, *start_results, *health_results]]}

    def apply_latest(self, *, dry_run: bool = False) -> dict[str, object]:
        staged = self.stage_latest()
        if not staged.get("staged_path"):
            return {"ok": False, "error": staged.get("error", "stage_latest failed"), "stage": staged}
        manifest = staged.get("manifest") if isinstance(staged.get("manifest"), dict) else {}
        version = str(manifest.get("version") or self._read_state().get("available_version") or self.runtime.version)
        result = self.apply_staged(version, str(staged["staged_path"]), dry_run=dry_run)
        result["downloaded"] = True
        result["staged_path"] = staged["staged_path"]
        result["applied_version"] = version
        return result

    def _rollback_unlocked(self, *, started: float | None = None) -> dict[str, object]:
        started = started or time.time()
        current = self.install_root / "current"
        previous = self.install_root / "previous"
        if not previous.exists():
            self._record("rollback_completed", "error", None, "previous release отсутствует")
            return {"ok": False, "error": "previous release отсутствует"}
        services = build_service_manager()
        stop_results = services.stop()
        self._atomic_symlink(current, previous.resolve())
        reload_results = services.reload()
        start_results = services.start()
        health_results = services.healthcheck()
        ok = all(item.ok for item in [*stop_results, *reload_results, *start_results, *health_results])
        duration_ms = int((time.time() - started) * 1000)
        self._write_state(stage="rollback_completed" if ok else "rollback_failed", current=str(current), rollback_target=str(previous.resolve()))
        self._record("rollback_completed", "ok" if ok else "error", None, str(previous.resolve()), duration_ms=duration_ms)
        return {"ok": ok, "current": str(current), "target": str(previous.resolve()), "service_results": [r.to_dict() for r in [*stop_results, *reload_results, *start_results, *health_results]]}

    def rollback(self) -> dict[str, object]:
        with FileUpdateLock(self.lock_path, operation="rollback"):
            return self._rollback_unlocked()

    def unlock_stale(self) -> dict[str, object]:
        lock = FileUpdateLock(self.lock_path)
        removed = lock.unlock_stale()
        return {"ok": True, "removed": removed, "path": str(self.lock_path), "metadata": lock.read_metadata()}

    def resume(self) -> dict[str, object]:
        state = self._read_state()
        stage = str(state.get("stage") or "idle")
        staged_path = str(state.get("staged_path") or "")
        target_version = str(state.get("available_version") or state.get("applied_version") or self.runtime.version)
        if stage in {"staged", "ready_to_apply"} and staged_path:
            return self.apply_staged(target_version, staged_path)
        if stage in {"applying", "apply_failed", "rollback_failed"}:
            return self.rollback()
        return {"ok": True, "stage": stage, "message": "нет незавершённого обновления"}
