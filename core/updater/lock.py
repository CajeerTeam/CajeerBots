from __future__ import annotations

import os
from pathlib import Path


class UpdateLockError(RuntimeError):
    pass


class FileUpdateLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None

    def __enter__(self) -> "FileUpdateLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            self.fd = os.open(str(self.path), flags, 0o600)
            os.write(self.fd, str(os.getpid()).encode("utf-8"))
        except FileExistsError as exc:
            raise UpdateLockError(f"обновление уже выполняется: {self.path}") from exc
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
