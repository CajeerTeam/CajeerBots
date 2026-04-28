from __future__ import annotations

import json
from typing import Any

from . import __version__
from .community_store import COMMUNITY_SCHEMA_MIGRATIONS, SCHEMA_VERSION, CommunityStore
from .event_contracts import EVENT_CONTRACT_VERSION
from .storage import StorageManager
from .config import Settings


async def run_schema_doctor(settings: Settings, *, runtime_version: str = __version__) -> int:
    storage = StorageManager(
        backend=settings.storage_backend,
        database_url=settings.database_url,
        sqlite_path=settings.sqlite_path,
        postgres_pool_min_size=settings.postgres_pool_min_size,
        postgres_pool_max_size=settings.postgres_pool_max_size,
        redis_url=settings.redis_url,
        redis_namespace=settings.redis_namespace,
        redis_relay_dedupe_ttl_seconds=settings.redis_relay_dedupe_ttl_seconds,
        redis_lock_ttl_seconds=settings.redis_lock_ttl_seconds,
        redis_command_cooldown_seconds=settings.redis_command_cooldown_seconds,
        allow_degraded_without_redis=settings.allow_degraded_without_redis,
        sqlite_optimize_on_cleanup=settings.sqlite_optimize_on_cleanup,
        sqlite_analyze_on_cleanup=settings.sqlite_analyze_on_cleanup,
        sqlite_vacuum_min_interval_seconds=settings.sqlite_vacuum_min_interval_seconds,
    )
    community = CommunityStore(storage, code_version=runtime_version)
    errors: list[str] = []
    warnings: list[str] = []
    payload: dict[str, Any] = {
        "runtime_version": runtime_version,
        "expected_schema_version": SCHEMA_VERSION,
        "expected_event_contract_version": str(EVENT_CONTRACT_VERSION),
        "expected_migrations": [{"version": version, "name": name} for version, name in COMMUNITY_SCHEMA_MIGRATIONS],
        "storage_backend": settings.storage_backend,
    }
    try:
        await storage.connect()
        await community.ensure_schema()
        payload["health"] = await community.health()
        payload["schema_version"] = await community.get_schema_version()
        payload["schema_meta"] = await community.list_schema_meta()
        payload["schema_meta_ledger_recent"] = await community.list_schema_meta_ledger(limit=25)
        payload["migration_plan"] = await community.schema_migration_plan()
        payload["schema_parity_issues"] = community.schema_parity_issues()
        if int(payload["schema_version"] or 0) != SCHEMA_VERSION:
            errors.append(f"recorded schema_version={payload['schema_version']} expected={SCHEMA_VERSION}")
        meta = payload.get("schema_meta") or {}
        if str(meta.get("nmdiscordbot_schema_current") or "") != str(SCHEMA_VERSION):
            errors.append("schema_meta.nmdiscordbot_schema_current is not in sync")
        if str(meta.get("nmdiscordbot_event_contract_version") or "") != str(EVENT_CONTRACT_VERSION):
            errors.append("schema_meta.nmdiscordbot_event_contract_version is not in sync")
        plan = payload.get("migration_plan") or {}
        if plan.get("pending_versions"):
            errors.append("pending migrations: " + ", ".join(str(item) for item in plan.get("pending_versions") or []))
        if payload.get("schema_parity_issues"):
            warnings.extend(str(item) for item in payload.get("schema_parity_issues") or [])
    except Exception as exc:
        errors.append(f"schema-doctor failed: {exc}")
    finally:
        await storage.close()
    payload["ok"] = not errors
    payload["errors"] = errors
    payload["warnings"] = warnings
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))
    return 0 if not errors else 5
