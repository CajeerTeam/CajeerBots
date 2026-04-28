from __future__ import annotations

import argparse
import asyncio
import sys

from . import __version__
from .community_store import CommunityStore
from .config import Settings, SettingsError
from .storage import StorageManager
from .buildmeta import build_runtime_drift_report


def _build_storage(settings: Settings) -> StorageManager:
    return StorageManager(
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


async def _readiness(settings: Settings) -> int:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    if settings.storage_backend == 'sqlite':
        settings.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    storage = _build_storage(settings)
    community = CommunityStore(storage, code_version=__version__)
    try:
        await storage.connect()
        await community.ensure_schema()
        await storage.healthcheck(strict_redis=settings.healthcheck_strict_redis)
        print({"ok": True, "kind": "readiness"})
        return 0
    except Exception as exc:
        print(f"healthcheck-error: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            await storage.close()
        except Exception:
            pass


async def _liveness(settings: Settings) -> int:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    if settings.storage_backend == 'sqlite':
        settings.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    print({"ok": True, "kind": "liveness"})
    return 0


async def _platform(settings: Settings) -> int:
    storage = _build_storage(settings)
    community = CommunityStore(storage, code_version=__version__)
    try:
        await storage.connect()
        await community.ensure_schema()
        print(await community.health())
        return 0
    except Exception as exc:
        print(f"platform-health-error: {exc}", file=sys.stderr)
        return 1
    finally:
        try:
            await storage.close()
        except Exception:
            pass



async def _preflight(settings: Settings) -> int:
    report = build_runtime_drift_report(settings, __version__)
    print(report)
    return 0 if not report['errors'] else 4

async def _run(mode: str) -> int:
    try:
        settings = Settings.load()
    except SettingsError as exc:
        print(f"config-error: {exc}", file=sys.stderr)
        return 2
    if mode == 'readiness':
        return await _readiness(settings)
    if mode == 'platform':
        return await _platform(settings)
    if mode in {'preflight', 'self_test'}:
        code = await _preflight(settings)
        if code != 0 or mode == 'preflight':
            return code
        return await _readiness(settings)
    return await _liveness(settings)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='NMDiscordBot healthcheck')
    parser.add_argument('--mode', choices=('liveness', 'readiness', 'platform', 'preflight', 'self_test'), default='liveness')
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    return asyncio.run(_run(args.mode))


if __name__ == '__main__':
    raise SystemExit(main())
