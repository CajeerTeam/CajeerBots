from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def _prune_old(backups_dir: Path, keep_last: int) -> int:
    if keep_last <= 0:
        return 0
    files = sorted(backups_dir.glob("cajeer-bots-*"), key=lambda p: p.stat().st_mtime, reverse=True)
    removed = 0
    for path in files[keep_last:]:
        if path.is_file():
            path.unlink()
            removed += 1
    return removed


def backup_database(database_url: str, backups_dir: Path, *, fmt: str = "custom", schema: str | None = None, compress: bool = True, keep_last: int | None = None) -> dict[str, object]:
    if not database_url:
        return {"ok": False, "error": "DATABASE_URL не задан"}
    backups_dir.mkdir(parents=True, exist_ok=True)
    suffix = "dump" if fmt == "custom" else "sql"
    target = backups_dir / f"cajeer-bots-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.{suffix}"
    command = ["pg_dump", database_url, "-f", str(target)]
    if fmt == "custom":
        command.insert(2, "-Fc")
    if schema:
        command.extend(["--schema", schema])
    if compress and fmt == "custom":
        command.extend(["--compress", os.getenv("DB_BACKUP_COMPRESS_LEVEL", "6")])
    completed = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=int(os.getenv("DB_BACKUP_TIMEOUT_SECONDS", "300")), check=False)
    removed = _prune_old(backups_dir, int(os.getenv("DB_BACKUP_KEEP_LAST", str(keep_last or 10))))
    return {"ok": completed.returncode == 0, "path": str(target), "output": completed.stdout.strip(), "format": fmt, "schema": schema or "all", "removed_old": removed}
