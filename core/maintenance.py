from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any

from core.schema import validate_schema_name


def _older_than(path: Path, seconds: int) -> bool:
    try:
        return time.time() - path.stat().st_mtime > seconds
    except FileNotFoundError:
        return False


def _retention_days(settings: Any, name: str, default: int) -> int:
    import os
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


async def _cleanup_db(settings: Any, policies: dict[str, int], *, apply: bool) -> dict[str, object]:
    if not getattr(settings.storage, "async_database_url", ""):
        return {"enabled": False, "reason": "DATABASE_ASYNC_URL не задан"}
    try:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine
    except Exception as exc:
        return {"enabled": False, "reason": f"sqlalchemy недоступен: {exc}"}
    schema = validate_schema_name(settings.shared_schema)
    engine = create_async_engine(settings.storage.async_database_url, pool_pre_ping=True)
    operations = [
        ("audit_log", "created_at < NOW() - (:days || ' days')::interval", policies["audit_days"]),
        ("dead_letters", "created_at < NOW() - (:days || ' days')::interval", policies["dead_letter_days"]),
        ("event_bus", "created_at < NOW() - (:days || ' days')::interval AND status IN ('delivered','failed')", policies["event_bus_days"]),
        ("delivery_queue", "created_at < NOW() - (:days || ' days')::interval AND status IN ('sent','failed')", policies["delivery_sent_days"]),
        ("idempotency_keys", "expires_at IS NOT NULL AND expires_at < NOW()", 0),
    ]
    result: dict[str, object] = {"enabled": True, "apply": apply, "tables": {}}
    try:
        async with engine.begin() as conn:
            for table, condition, days in operations:
                params = {"days": max(0, int(days))}
                count = (await conn.execute(text(f"SELECT count(*) FROM {schema}.{table} WHERE {condition}"), params)).scalar_one()
                deleted = 0
                if apply and count:
                    deleted = (await conn.execute(text(f"DELETE FROM {schema}.{table} WHERE {condition}"), params)).rowcount or 0
                result["tables"][table] = {"matched": int(count), "deleted": int(deleted)}  # type: ignore[index]
    except Exception as exc:
        result["error"] = str(exc)
    finally:
        await engine.dispose()
    return result


def cleanup_runtime(project_root: Path, settings: Any, *, dry_run: bool = True) -> dict[str, object]:
    runtime_dir = project_root / getattr(settings, "runtime_dir", Path("runtime"))
    policies = {
        "audit_days": _retention_days(settings, "AUDIT_RETENTION_DAYS", 90),
        "dead_letter_days": _retention_days(settings, "DEAD_LETTER_RETENTION_DAYS", 30),
        "event_bus_days": _retention_days(settings, "EVENT_BUS_RETENTION_DAYS", 14),
        "delivery_sent_days": _retention_days(settings, "DELIVERY_SENT_RETENTION_DAYS", 14),
        "file_cache_days": _retention_days(settings, "RUNTIME_FILE_CACHE_RETENTION_DAYS", 7),
    }
    removed: list[str] = []
    candidates = [runtime_dir / "events.jsonl", runtime_dir / "delivery.jsonl"]
    candidates.extend((runtime_dir / "tmp").glob("*") if (runtime_dir / "tmp").exists() else [])
    for candidate in candidates:
        if candidate.is_file() and _older_than(candidate, policies["file_cache_days"] * 86400):
            removed.append(str(candidate))
            if not dry_run:
                candidate.unlink(missing_ok=True)
    db_cleanup = asyncio.run(_cleanup_db(settings, policies, apply=not dry_run))
    return {"ok": True, "dry_run": dry_run, "runtime_dir": str(runtime_dir), "removed": removed, "policies": policies, "db_cleanup": db_cleanup}
