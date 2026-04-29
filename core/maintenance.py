from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any


def _days(name: str, default: int) -> int:
    try:
        return max(0, int(os.getenv(name, str(default))))
    except ValueError:
        return default


def _remove_older_than(path: Path, older_than_seconds: int) -> int:
    if not path.exists():
        return 0
    now = time.time()
    removed = 0
    for item in path.rglob("*"):
        if not item.is_file():
            continue
        try:
            if now - item.stat().st_mtime > older_than_seconds:
                item.unlink()
                removed += 1
        except OSError:
            continue
    return removed


def cleanup_runtime(project_root: Path, settings: Any) -> dict[str, object]:
    """Best-effort cleanup for file-backed runtime artifacts.

    DB-backed retention is intentionally explicit in SQL migrations/operators. This
    command handles local runtime files and gives the operator the active DB
    retention policy in the response.
    """
    runtime_dir = settings.runtime_dir if settings.runtime_dir.is_absolute() else project_root / settings.runtime_dir
    policies = {
        "AUDIT_RETENTION_DAYS": _days("AUDIT_RETENTION_DAYS", 90),
        "DEAD_LETTER_RETENTION_DAYS": _days("DEAD_LETTER_RETENTION_DAYS", 30),
        "EVENT_BUS_RETENTION_DAYS": _days("EVENT_BUS_RETENTION_DAYS", 14),
        "DELIVERY_SENT_RETENTION_DAYS": _days("DELIVERY_SENT_RETENTION_DAYS", 7),
        "UPDATE_HISTORY_RETENTION": int(os.getenv("UPDATE_HISTORY_RETENTION", "200")),
    }
    removed = {
        "tmp": _remove_older_than(runtime_dir / "tmp", 24 * 3600),
        "downloads": _remove_older_than(runtime_dir / "updates" / "downloads", max(1, policies["UPDATE_HISTORY_RETENTION"]) * 24 * 3600),
    }
    return {"ok": True, "runtime_dir": str(runtime_dir), "removed": removed, "policies": policies, "db_cleanup": "use SQL retention job or future cajeer-bots maintenance db-cleanup"}
