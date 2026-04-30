from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path


class UpdateLockError(RuntimeError):
    pass


class FileUpdateLock:
    """Atomic file lock for updater operations with stale-lock recovery support."""

    def __init__(self, path: Path, *, operation: str = "update", stale_after_seconds: int | None = None) -> None:
        self.path = path
        self.operation = operation
        self.stale_after_seconds = stale_after_seconds or int(os.getenv("CAJEER_UPDATE_LOCK_STALE_SECONDS", "1800"))
        self.fd: int | None = None

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    def _metadata(self) -> dict[str, object]:
        return {
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "operation": self.operation,
            "created_at": int(time.time()),
        }

    def read_metadata(self) -> dict[str, object]:
        if not self.path.exists():
            return {}
        try:
            raw = self.path.read_text(encoding="utf-8").strip()
            if raw.startswith("{"):
                data = json.loads(raw)
                return data if isinstance(data, dict) else {}
            return {"pid": int(raw), "created_at": 0, "operation": "unknown"}
        except Exception:
            return {"pid": 0, "created_at": 0, "operation": "unknown"}

    def is_stale(self) -> bool:
        data = self.read_metadata()
        pid = int(data.get("pid") or 0)
        created_at = int(data.get("created_at") or 0)
        age = time.time() - created_at if created_at else self.stale_after_seconds + 1
        return (not self._pid_alive(pid)) or age > self.stale_after_seconds

    def unlock_stale(self) -> bool:
        if self.path.exists() and self.is_stale():
            self.path.unlink()
            return True
        return False

    def __enter__(self) -> "FileUpdateLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            self.fd = os.open(str(self.path), flags, 0o600)
            os.write(self.fd, json.dumps(self._metadata(), ensure_ascii=False).encode("utf-8"))
        except FileExistsError as exc:
            if self.unlock_stale():
                self.fd = os.open(str(self.path), flags, 0o600)
                os.write(self.fd, json.dumps(self._metadata(), ensure_ascii=False).encode("utf-8"))
                return self
            data = self.read_metadata()
            raise UpdateLockError(f"обновление уже выполняется: {self.path} ({data})") from exc
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
