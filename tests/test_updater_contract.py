from pathlib import Path
import tarfile

import pytest

from core.updater.manager import UpdateManager
from core.updater.manifest import ReleaseManifest


class DummyRuntime:
    version = "0.10.1"
    project_root = Path.cwd()

    def __init__(self, runtime_dir: Path):
        class Settings:
            pass
        self.settings = Settings()
        self.settings.runtime_dir = runtime_dir
        self.settings.instance_id = "test"
        self.audit = type("Audit", (), {"write": lambda *a, **k: None})()
        self.workspace = type("Workspace", (), {"report_event": lambda *a, **k: None})()
        self.remote_logs = type("Logs", (), {"emit_event": lambda *a, **k: None})()

    def doctor(self, offline=True):
        return []

    def make_system_event(self, event_type, payload):
        from core.events import CajeerEvent
        return CajeerEvent.create(source="system", type=event_type, payload=payload)


def test_safe_extract_blocks_path_traversal(tmp_path):
    manager = UpdateManager(DummyRuntime(tmp_path / "runtime"))
    archive = tmp_path / "bad.tar.gz"
    evil = tmp_path / "evil.txt"
    evil.write_text("x", encoding="utf-8")
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(evil, arcname="../evil.txt")
    with tarfile.open(archive, "r:gz") as tf:
        with pytest.raises(ValueError):
            manager._safe_extract(tf, tmp_path / "out")


def test_stage_normalizes_single_root(tmp_path, monkeypatch):
    monkeypatch.setenv("CAJEER_UPDATE_INSTALL_ROOT", str(tmp_path / "updates"))
    project = tmp_path / "CajeerBots-0.10.1"
    (project / "core").mkdir(parents=True)
    (project / "VERSION").write_text("0.10.1\n", encoding="utf-8")
    (project / "README.md").write_text("ok", encoding="utf-8")
    archive = tmp_path / "CajeerBots-0.10.1.tar.gz"
    with tarfile.open(archive, "w:gz") as tf:
        tf.add(project, arcname=project.name)
    manager = UpdateManager(DummyRuntime(tmp_path / "runtime"))
    result = manager.stage_local_artifact(archive, manifest=ReleaseManifest("CajeerBots", "0.10.1", "stable", ">=3.11", "cajeer.bots.db.v1", "cajeer.bots.event.v1", False))
    staged = Path(result["staged_path"])
    assert (staged / "VERSION").exists()
    assert (staged / "core").is_dir()
