from __future__ import annotations

import argparse
import os
import stat
import asyncio
import logging
import json
import hashlib
import sys
from pathlib import Path

from . import __version__
from .config import Settings, SettingsError
from .logging_setup import configure_logging
from .buildmeta import build_runtime_drift_report
from .release_check import run_release_check
from .env_doctor import run_env_doctor

LOGGER = logging.getLogger("nmdiscordbot")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NMDiscordBot")
    parser.add_argument("--check-config", action="store_true", help="validate .env and exit")
    parser.add_argument("--prepare-runtime", action="store_true", help="connect storage and ensure community schema")
    parser.add_argument("--run-cleanup-once", action="store_true", help="run one cleanup cycle and exit")
    parser.add_argument("--sync-commands", action="store_true", help="sync app commands on startup regardless of env flag")
    parser.add_argument("--show-platform-health", action="store_true", help="print community platform health and exit")
    parser.add_argument("--preflight", action="store_true", help="run strict preflight checks and exit")
    parser.add_argument("--self-test", action="store_true", help="run config/schema/storage self-test and exit")
    parser.add_argument("--smoke-check", action="store_true", help="alias for --self-test")
    parser.add_argument("--migrate-only", action="store_true", help="connect storage, ensure schema and exit")
    parser.add_argument("--schema-info", action="store_true", help="print schema/build/content version snapshot and exit")
    parser.add_argument("--list-backups", action="store_true", help="list recent operational backups and exit")
    parser.add_argument("--history-snapshot", action="store_true", help="print bridge/job observability snapshot and exit")
    parser.add_argument("--recovery-mode", action="store_true", help="start runtime in safe recovery mode")
    parser.add_argument("--release-check", action="store_true", help="run production archive release sanity checks and exit")
    parser.add_argument("--schema-doctor", action="store_true", help="run storage schema diagnostics and exit")
    parser.add_argument("--env-doctor", action="store_true", help="run redacted .env diagnostics and exit")
    parser.add_argument("--discord-bindings-check", action="store_true", help="verify configured Discord channel/role IDs through Discord API and exit")
    parser.add_argument("--export-discord-bindings", action="store_true", help="print .env channel/role bindings discovered from the Discord server and exit")
    parser.add_argument("--bridge-doctor", action="store_true", help="run bridge destination/routing diagnostics and exit")
    parser.add_argument("--bridge-smoke", action="store_true", help="send a signed synthetic event to configured bridge destinations and exit")
    parser.add_argument("--bridge-smoke-event", default="community.world_signal.created", help="event type for --bridge-smoke")
    parser.add_argument("--ingress-smoke", action="store_true", help="send signed/invalid synthetic events to local ingress and exit")
    parser.add_argument("--event-coverage", action="store_true", help="print event contract and bridge route coverage and exit")
    return parser.parse_args(argv)


def _build_storage(settings: Settings):
    from .storage import StorageManager
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


async def _prepare_runtime(settings: Settings, *, show_platform_health: bool = False) -> int:
    from .community_store import CommunityStore
    storage = _build_storage(settings)
    community = CommunityStore(storage, code_version=__version__)
    try:
        await storage.connect()
        await community.ensure_schema()
        if show_platform_health:
            print(await community.health())
        return 0
    finally:
        await storage.close()



async def _run_preflight(settings: Settings, *, with_storage: bool = False, emit_report: bool = True) -> int:
    from .community_store import CommunityStore
    report = build_runtime_drift_report(settings, __version__)
    warnings = list(report.get('warnings') or [])
    errors = list(report.get('errors') or [])
    _ = settings.discord_content_file_path.read_text(encoding="utf-8")
    _ = dict(settings.forum_policy_overrides or {})
    _ = dict(settings.staff_scope_role_map or {})
    _ = dict(settings.bridge_event_rules or {})
    for path in [Path('.env'), settings.log_dir, settings.data_dir, settings.backup_dir]:
        try:
            st = path.stat()
        except FileNotFoundError:
            warnings.append(f'Путь отсутствует: {path}')
            continue
        mode = stat.S_IMODE(st.st_mode)
        if path.name == '.env':
            if mode & 0o077:
                warnings.append('.env имеет слишком широкие права доступа; ожидается 0600.')
            if st.st_uid != os.getuid():
                warnings.append('.env принадлежит другому пользователю; проверьте владельца файла.')
        else:
            if not os.access(path, os.W_OK):
                errors.append(f'Нет прав на запись: {path}')
    report['warnings'] = warnings
    report['errors'] = errors
    if report['errors'] and settings.strict_runtime_precheck:
        print(report, file=sys.stderr)
        return 4
    if with_storage:
        storage = _build_storage(settings)
        community = CommunityStore(storage, code_version=__version__)
        try:
            await storage.connect()
            await community.ensure_schema()
            await storage.healthcheck(strict_redis=settings.healthcheck_strict_redis)
            print({'report': report, 'schema_version': await community.get_schema_version()})
            return 0 if not report['errors'] else 4
        finally:
            await storage.close()
    if emit_report:
        print(report)
    return 0 if not report['errors'] else 4

async def _run_cleanup_once(settings: Settings) -> int:
    from .community_store import CommunityStore
    storage = _build_storage(settings)
    community = CommunityStore(storage, code_version=__version__)
    try:
        await storage.connect()
        await community.ensure_schema()
        deleted = await storage.purge_old_records(
            audit_log_retention_days=settings.audit_log_retention_days,
            verification_session_retention_days=settings.verification_session_retention_days,
            relay_history_retention_days=settings.relay_history_retention_days,
        )
        optimize_actions = await storage.optimize_sqlite(deleted_rows=deleted)
        print({"deleted": deleted, "sqlite_actions": optimize_actions})
        return 0
    finally:
        await storage.close()


async def _migrate_only(settings: Settings) -> int:
    from .community_store import CommunityStore
    storage = _build_storage(settings)
    community = CommunityStore(storage, code_version=__version__)
    try:
        await storage.connect()
        await community.ensure_schema()
        print({"ok": True, "mode": "migrate_only", "schema_version": await community.get_schema_version()})
        return 0
    finally:
        await storage.close()




async def _list_backups(settings: Settings) -> int:
    settings.backup_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for path in sorted(settings.backup_dir.glob('nmdiscordbot-backup-*.json'), reverse=True)[:50]:
        item = {'name': path.name, 'size_bytes': path.stat().st_size, 'modified_at': path.stat().st_mtime}
        try:
            raw = path.read_text(encoding='utf-8')
            payload = json.loads(raw)
            item['reason'] = payload.get('reason')
            item['actor_user_id'] = payload.get('actor_user_id')
            item['created_at'] = payload.get('created_at')
            item['snapshot_version'] = payload.get('version')
            item['snapshot_kind'] = payload.get('snapshot_kind') or payload.get('export_kind')
            item['sha256'] = hashlib.sha256(raw.encode('utf-8')).hexdigest()
        except Exception:
            item['parse_error'] = True
        rows.append(item)
    print({'backup_dir': str(settings.backup_dir), 'count': len(rows), 'items': rows})
    return 0


async def _history_snapshot(settings: Settings) -> int:
    from .community_store import CommunityStore
    storage = _build_storage(settings)
    community = CommunityStore(storage, code_version=__version__)
    try:
        await storage.connect()
        await community.ensure_schema()
        payload = {
            'bridge_delivery_stats': await community.get_external_sync_delivery_stats(since_hours=24),
            'failed_bridge_events': await community.list_failed_external_sync_events(limit=50, since_hours=24),
            'scheduled_jobs_pending': await community.list_scheduled_jobs(statuses=('pending','retry'), limit=50, since_hours=24),
            'scheduled_jobs_terminal': await community.list_scheduled_jobs(statuses=('failed','sent','cancelled'), limit=50, since_hours=24),
        }
        trends = {}
        kind_trends = {}
        for row in payload['failed_bridge_events']:
            dst = str(row.get('destination') or 'unknown')
            kind = str(row.get('event_kind') or row.get('kind') or 'unknown')
            trends.setdefault(dst, {'failed': 0})['failed'] += 1
            kind_trends[kind] = kind_trends.get(kind, 0) + 1
        payload['bridge_trends'] = trends
        payload['bridge_kind_trends'] = kind_trends
        payload['job_trends'] = {'pending': len(payload['scheduled_jobs_pending']), 'terminal': len(payload['scheduled_jobs_terminal'])}
        print(payload)
        return 0
    finally:
        await storage.close()


def _apply_recovery_mode(settings: Settings) -> None:
    settings.recovery_mode_default = True
    settings.ingress_enabled = False
    settings.relay_enabled = False
    settings.panel_auto_reconcile_on_ready = False
    settings.community_core_event_url = ''
    settings.telegram_bridge_url = ''
    settings.vk_bridge_url = ''
    settings.workspace_bridge_url = ''

async def _schema_info(settings: Settings) -> int:
    from .community_store import COMMUNITY_SCHEMA_MIGRATIONS, CommunityStore
    storage = _build_storage(settings)
    community = CommunityStore(storage, code_version=__version__)
    try:
        await storage.connect()
        await community.ensure_schema()
        print({
            "build": build_runtime_drift_report(settings, __version__),
            "community_health": await community.health(),
            "schema_version": await community.get_schema_version(),
            "schema_parity_issues": community.schema_parity_issues(),
            "migration_count": len(COMMUNITY_SCHEMA_MIGRATIONS),
            "migration_plan": await community.schema_migration_plan(),
            "storage_backend": settings.storage_backend,
            "content_path": str(settings.discord_content_file_path),
        })
        return 0
    finally:
        await storage.close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.env_doctor:
        return run_env_doctor(runtime_version=__version__)
    try:
        settings = Settings.load()
    except SettingsError as exc:
        print(f"[!] Configuration error: {exc}", file=sys.stderr)
        return 2

    configure_logging(settings.log_level, json_logs=settings.log_json, log_dir=settings.log_dir, app_version=__version__)
    settings.data_dir.mkdir(parents=True, exist_ok=True)

    if args.check_config:
        print("ok")
        return 0
    if args.release_check:
        return run_release_check(settings, __version__)
    if args.schema_doctor:
        from .schema_doctor import run_schema_doctor
        return asyncio.run(run_schema_doctor(settings, runtime_version=__version__))
    if args.bridge_doctor:
        from .bridge_doctor import run_bridge_doctor
        return run_bridge_doctor(settings)
    if args.bridge_smoke:
        from .bridge_smoke import run_bridge_smoke
        return run_bridge_smoke(settings, event_type=args.bridge_smoke_event)
    if args.ingress_smoke:
        from .ingress_smoke import run_ingress_smoke
        return run_ingress_smoke(settings)
    if args.event_coverage:
        from .event_coverage import run_event_coverage
        return run_event_coverage(settings)
    if args.discord_bindings_check:
        from .discord_bindings import run_discord_bindings_check
        return asyncio.run(run_discord_bindings_check(settings))
    if args.export_discord_bindings:
        from .discord_bindings import run_export_discord_bindings
        return asyncio.run(run_export_discord_bindings(settings))
    if args.prepare_runtime or args.show_platform_health:
        return asyncio.run(_prepare_runtime(settings, show_platform_health=args.show_platform_health))
    if args.preflight:
        return asyncio.run(_run_preflight(settings, with_storage=False))
    if args.self_test or args.smoke_check:
        return asyncio.run(_run_preflight(settings, with_storage=True))
    if args.migrate_only:
        return asyncio.run(_migrate_only(settings))
    if args.schema_info:
        return asyncio.run(_schema_info(settings))
    if args.list_backups:
        return asyncio.run(_list_backups(settings))
    if args.history_snapshot:
        return asyncio.run(_history_snapshot(settings))
    if args.run_cleanup_once:
        return asyncio.run(_run_cleanup_once(settings))

    if args.recovery_mode or settings.recovery_mode_default:
        _apply_recovery_mode(settings)
        LOGGER.warning("NMDiscordBot запускается в recovery mode: ingress/relay/panel reconcile/external destinations отключены")

    LOGGER.info("Starting NMDiscordBot version=%s", __version__)
    LOGGER.info("Guild scoped sync: %s", bool(settings.discord_guild_id))
    LOGGER.info("Relay enabled: %s", settings.relay_enabled)
    LOGGER.info("API configured: %s", bool(settings.nevermine_api_base_url))
    LOGGER.info("Storage backend: %s", settings.storage_backend)
    LOGGER.info("Runtime data dir: %s", settings.data_dir)
    LOGGER.info("Runtime log dir: %s", settings.log_dir)
    LOGGER.info("Shared storage dir: %s available=%s", settings.shared_dir, settings.shared_dir.exists())
    LOGGER.info("Redis configured: %s", bool(settings.redis_url))
    LOGGER.info("Allow degraded mode without Redis: %s", settings.allow_degraded_without_redis)
    LOGGER.info("Ingress enabled: %s bind=%s:%s", settings.ingress_enabled, settings.ingress_host, settings.ingress_port)
    LOGGER.info("HTTP public URL: %s", settings.app_public_url or "not configured")
    LOGGER.info("Bridge destinations configured: %s", bool(settings.community_core_event_url or settings.telegram_bridge_url or settings.vk_bridge_url or settings.workspace_bridge_url))
    LOGGER.info("Remote logs enabled: %s", os.getenv("REMOTE_LOGS_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"})

    if args.sync_commands:
        settings.discord_sync_commands_on_start = True

    preflight_code = asyncio.run(_run_preflight(settings, with_storage=False, emit_report=False))
    if preflight_code != 0 and settings.strict_runtime_precheck:
        return preflight_code

    storage = _build_storage(settings)

    from .bot import NMDiscordBot

    bot = NMDiscordBot(settings, storage)
    try:
        bot.run(settings.discord_token, log_handler=None)
    except Exception as exc:
        print(f"[!] Runtime error: {exc}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
