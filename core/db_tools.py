from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def backup_database(database_url: str, backups_dir: Path, *, fmt: str = "custom") -> dict[str, object]:
    if not database_url:
        return {"ok": False, "error": "DATABASE_URL не задан"}
    backups_dir.mkdir(parents=True, exist_ok=True)
    suffix = "dump" if fmt == "custom" else "sql"
    target = backups_dir / f"cajeer-bots-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.{suffix}"
    command = ["pg_dump", database_url, "-f", str(target)]
    if fmt == "custom":
        command.insert(2, "-Fc")
    completed = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=int(os.getenv("DB_BACKUP_TIMEOUT_SECONDS", "300")), check=False)
    return {"ok": completed.returncode == 0, "path": str(target), "output": completed.stdout.strip(), "format": fmt}
