from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from core.commands import build_default_commands
from core.permissions_fix import fix_permissions
from core.registry import Registry


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cajeer-bots",
        description="Cajeer Bots — универсальная платформа для запуска и расширения ботов.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Запустить выбранную цель local-режима или служебный процесс.")
    run.add_argument("target", nargs="?", choices=["all", "telegram", "discord", "vkontakte", "fake", "worker", "api", "bridge"])

    doctor = sub.add_parser("doctor", help="Проверить конфигурацию и состояние платформы.")
    doctor.add_argument("--offline", action="store_true", help="Не проверять внешние сервисы и PostgreSQL.")
    doctor.add_argument("--mode", choices=["local", "distributed"], default="local", help="Проверяемый архитектурный режим.")
    doctor.add_argument("--profile", choices=["dev", "staging", "production", "release-artifact"], default=None, help="Профиль строгости doctor-проверок.")
    doctor.add_argument("--fix-permissions", action="store_true", help="Исправить права shell/python entrypoints перед проверкой.")

    release = sub.add_parser("release", help="Проверка и обслуживание release artifacts.")
    release_sub = release.add_subparsers(dest="release_command", required=True)
    release_verify = release_sub.add_parser("verify", help="Проверить tar.gz/zip release artifact.")
    release_verify.add_argument("artifact")
    release_verify.add_argument("--deep", action="store_true", help="Распаковать артефакт и выполнить syntax/doctor/smoke проверки.")
    release_verify.add_argument("--python", default="python3", help="Python interpreter для deep-проверок.")
    release_checklist = release_sub.add_parser("checklist", help="Запустить исполняемые release checklist/drill проверки.")
    release_checklist.add_argument("--file", default="release/checklist.yaml")

    sub.add_parser("init", help="Создать .env, runtime-каталоги и базовые секреты.")
    sub.add_parser("fix-permissions", help="Исправить права запускаемых файлов.")
    modules_cmd = sub.add_parser("modules", help="Показать зарегистрированные модули или проверить module.json.")
    modules_cmd.add_argument("--validate", metavar="PATH", default="", help="Проверить module.json без запуска runtime.")
    plugins_cmd = sub.add_parser("plugins", help="Показать зарегистрированные плагины или проверить plugin.json.")
    plugins_cmd.add_argument("--validate", metavar="PATH", default="", help="Проверить plugin.json без запуска runtime.")
    sub.add_parser("adapters", help="Показать зарегистрированные адаптеры.")
    sub.add_parser("components", help="Показать runtime-компоненты с entrypoint.")
    sub.add_parser("commands", help="Показать зарегистрированные команды.")

    update = sub.add_parser("update", help="Безопасные обновления из GitHub Releases или локального tar.gz.")
    update_sub = update.add_subparsers(dest="update_command", required=True)
    update_sub.add_parser("status", help="Показать локальный статус updater.")
    update_sub.add_parser("history", help="Показать историю обновлений.")
    update_sub.add_parser("check", help="Проверить доступное обновление.")
    plan = update_sub.add_parser("plan", help="Показать план обновления без применения.")
    plan.add_argument("--version", default="latest")
    update_sub.add_parser("download", help="Скачать latest artifact из GitHub Releases.")
    update_sub.add_parser("stage-latest", help="Скачать и распаковать latest artifact в staging.")
    stage = update_sub.add_parser("stage", help="Проверить и распаковать локальный release artifact в staging.")
    stage.add_argument("artifact", help="Путь к CajeerBots-*.tar.gz")
    stage.add_argument("--manifest", help="Путь к *.release.json", default="")
    stage.add_argument("--sha256", help="Ожидаемый sha256", default="")
    apply = update_sub.add_parser("apply", help="Переключить current на staged release.")
    apply.add_argument("--version", required=True)
    apply.add_argument("--staged-path", default="")
    apply.add_argument("--dry-run", action="store_true")
    update_sub.add_parser("rollback", help="Откатить current на previous.")
    update_sub.add_parser("resume", help="Продолжить безопасный шаг update после сбоя.")
    update_sub.add_parser("unlock", help="Удалить stale update.lock, если процесс уже умер.")

    secrets = sub.add_parser("secrets", help="Операции с секретами.")
    secrets_sub = secrets.add_subparsers(dest="secrets_command", required=True)
    secrets_sub.add_parser("generate", help="Сгенерировать безопасный env-блок секретов.")

    tokens = sub.add_parser("tokens", help="Управление scoped API-токенами.")
    tokens_sub = tokens.add_subparsers(dest="tokens_command", required=True)
    create_token = tokens_sub.add_parser("create", help="Создать API-токен и сохранить только sha256-хэш.")
    create_token.add_argument("--id", required=True)
    create_token.add_argument("--scope", action="append", default=[])
    create_token.add_argument("--prefix", default="cb_")
    revoke_token = tokens_sub.add_parser("revoke", help="Отозвать API-токен по id.")
    revoke_token.add_argument("id")
    inspect_token = tokens_sub.add_parser("inspect", help="Показать один token record без значения токена.")
    inspect_token.add_argument("id")
    rotate_token = tokens_sub.add_parser("rotate", help="Перевыпустить токен с сохранением scopes.")
    rotate_token.add_argument("id")
    tokens_sub.add_parser("list", help="Показать token registry без значений токенов.")

    logs = sub.add_parser("logs", help="Операции с буфером Cajeer Logs.")
    logs_sub = logs.add_subparsers(dest="logs_command", required=True)
    logs_sub.add_parser("flush", help="Отправить накопленный JSONL-буфер в Cajeer Logs.")
    logs_sub.add_parser("buffer-status", help="Показать состояние локального буфера Cajeer Logs.")

    db = sub.add_parser("db", help="Команды PostgreSQL/SQLAlchemy/Alembic.")
    db_sub = db.add_subparsers(dest="db_command", required=True)
    db_sub.add_parser("check", help="Проверить внешний DB contract без изменения схемы.")
    db_sub.add_parser("contract", help="Показать обязательные таблицы и поля DB contract.")
    db_sub.add_parser("doctor", help="Проверить async-подключение и DB contract.")
    db_sub.add_parser("alembic", help="Показать путь к Alembic-конфигурации.")
    db_sub.add_parser("current", help="Показать текущую Alembic revision.")
    db_sub.add_parser("history", help="Показать историю Alembic revisions.")
    db_backup = db_sub.add_parser("backup", help="Сделать pg_dump перед миграцией.")
    db_backup.add_argument("--format", choices=["custom", "plain"], default="custom")
    db_backup.add_argument("--schema", default="")
    db_backup.add_argument("--no-compress", action="store_true")
    db_backup.add_argument("--keep-last", type=int, default=10)
    db_restore = db_sub.add_parser("restore", help="Проверить или выполнить восстановление PostgreSQL из backup-файла.")
    db_restore.add_argument("backup_file")
    db_restore.add_argument("--dry-run", action="store_true", help="Только проверить наличие файла и команд восстановления.")
    db_upgrade = db_sub.add_parser("upgrade", help="Выполнить alembic upgrade.")
    db_upgrade.add_argument("revision", nargs="?", default="head")

    sub.add_parser("migrate", help="Показать статус управления схемой БД через Alembic.")
    sub.add_parser("db-status", help="Показать статус модели БД без выполнения миграций.")
    self_test = sub.add_parser("self-test", help="Операторская комплексная самопроверка runtime.")
    self_test.add_argument("--profile", choices=["local-memory", "staging", "production"], default="local-memory")
    self_test.add_argument("--offline", action="store_true", help="Не проверять внешние зависимости.")

    catalog = sub.add_parser("catalog", help="Управление runtime catalog.")
    catalog_sub = catalog.add_subparsers(dest="catalog_command", required=True)
    catalog_sub.add_parser("list", help="Показать catalog.lock.")
    catalog_install = catalog_sub.add_parser("install", help="Зафиксировать установленный plugin/module в catalog.lock.")
    catalog_install.add_argument("id")
    catalog_install.add_argument("--version", required=True)
    catalog_install.add_argument("--source", default="local")
    catalog_install.add_argument("--sha256", default="")
    catalog_update = catalog_sub.add_parser("update", help="Обновить запись catalog.lock.")
    catalog_update.add_argument("id")
    catalog_update.add_argument("--version", required=True)
    catalog_update.add_argument("--source", default="local")
    catalog_update.add_argument("--sha256", default="")
    catalog_verify = catalog_sub.add_parser("verify", help="Проверить catalog.lock и установленные артефакты.")
    catalog_verify.add_argument("id", nargs="?")
    catalog_rollback = catalog_sub.add_parser("rollback", help="Откатить запись catalog.lock к предыдущей версии, если она есть.")
    catalog_rollback.add_argument("id")

    maintenance = sub.add_parser("maintenance", help="Операционное обслуживание runtime.")
    maintenance_sub = maintenance.add_subparsers(dest="maintenance_command", required=True)
    maintenance_sub.add_parser("cleanup", help="Очистить устаревшие runtime-записи по retention policy.")

    distributed = sub.add_parser("distributed", help="Команды дополнительного распределённого режима.")
    distributed_sub = distributed.add_subparsers(dest="distributed_command", required=True)
    distributed_sub.add_parser("doctor", help="Проверить настройки распределённого режима.").add_argument("--offline", action="store_true")
    distributed_sub.add_parser("protocol", help="Показать версии протоколов distributed mode.")
    return parser


def _json(items: object) -> str:
    return json.dumps(items, ensure_ascii=False, indent=2)


def _write_env_if_missing(root: Path) -> bool:
    env_path = root / ".env"
    if env_path.exists():
        return False
    example = root / ".env.example"
    content = example.read_text(encoding="utf-8") if example.exists() else ""
    from core.secrets import generate_env_block

    content = content.rstrip() + "\n\n# Сгенерированные секреты\n" + generate_env_block()
    env_path.write_text(content, encoding="utf-8")
    return True


def _runtime(project_root: Path):
    from core.config import Settings
    from core.logging import configure_logging
    from core.runtime import Runtime

    settings = Settings.from_env()
    configure_logging(settings.log_level)
    return Runtime(settings, project_root=project_root)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    project_root = Path.cwd()

    if args.command == "fix-permissions":
        changed = fix_permissions(project_root)
        print(_json({"changed": changed}), flush=True)
        return 0

    if args.command == "init":
        changed_env = _write_env_if_missing(project_root)
        for relative in ["runtime", "runtime/catalog", "runtime/catalog/modules", "runtime/catalog/plugins"]:
            (project_root / relative).mkdir(parents=True, exist_ok=True)
        changed_permissions = fix_permissions(project_root)
        print(_json({"env_created": changed_env, "permissions_changed": changed_permissions}), flush=True)
        return 0

    if args.command == "secrets" and args.secrets_command == "generate":
        from core.secrets import generate_env_block

        print(generate_env_block(), end="", flush=True)
        return 0

    if args.command == "tokens":
        from core.config import Settings
        from core.token_registry import ApiTokenRegistry

        settings = Settings.from_env()
        registry = ApiTokenRegistry(settings.api_tokens_file)
        if args.tokens_command == "create":
            token, record = registry.create_token(token_id=args.id, scopes=args.scope or ["readonly"], prefix=args.prefix)
            print(_json({"token": token, "record": record.to_dict()}), flush=True)
            return 0
        if args.tokens_command == "revoke":
            print(_json({"revoked": registry.revoke(args.id)}), flush=True)
            return 0
        if args.tokens_command == "inspect":
            print(_json(registry.inspect(args.id) or {"error": "not_found"}), flush=True)
            return 0
        if args.tokens_command == "rotate":
            token, record = registry.rotate(args.id)
            print(_json({"token": token, "record": record.to_dict()}), flush=True)
            return 0
        if args.tokens_command == "list":
            print(_json({"items": registry.snapshot()}), flush=True)
            return 0

    if args.command in {"modules", "plugins", "adapters"}:
        from core.config import Settings

        settings = Settings.from_env()
        registry = Registry(project_root, settings=settings)
        if args.command == "modules":
            if getattr(args, "validate", ""):
                errors = registry.validate_manifest_path(Path(args.validate), expected_type="module")
                print(_json({"ok": not errors, "errors": errors}), flush=True)
                return 0 if not errors else 1
            print(_json([item.to_dict() for item in registry.modules()]), flush=True)
        elif args.command == "plugins":
            if getattr(args, "validate", ""):
                manifest_path = Path(args.validate)
                errors = registry.validate_manifest_path(manifest_path, expected_type="plugin")
                from core.plugin_policy import validate_plugin_import_policy
                policy = validate_plugin_import_policy(manifest_path.parent if manifest_path.name == "plugin.json" else manifest_path)
                errors.extend(policy.errors)
                print(_json({"ok": not errors, "errors": errors}), flush=True)
                return 0 if not errors else 1
            print(_json([item.to_dict() for item in registry.plugins()]), flush=True)
        else:
            print(_json([item.to_dict() for item in registry.adapters()]), flush=True)
        return 0

    if args.command == "components":
        from core.config import Settings

        settings = Settings.from_env()
        registry = Registry(project_root, settings=settings)
        print(_json([item.to_dict() for item in registry.load_order() if item.entrypoint]), flush=True)
        return 0

    if args.command == "commands":
        print(_json([item.to_dict() for item in build_default_commands().list()]), flush=True)
        return 0

    if args.command == "update":
        runtime = _runtime(project_root)
        if args.update_command == "status":
            print(_json(runtime.updater.status().to_dict()), flush=True)
            return 0
        if args.update_command == "history":
            print(_json([item.to_dict() for item in runtime.updater.history()]), flush=True)
            return 0
        if args.update_command == "check":
            print(_json(runtime.updater.check()), flush=True)
            return 0
        if args.update_command == "plan":
            print(_json(runtime.updater.plan(args.version)), flush=True)
            return 0
        if args.update_command == "download":
            print(_json(runtime.updater.download_latest()), flush=True)
            return 0
        if args.update_command == "stage-latest":
            print(_json(runtime.updater.stage_latest()), flush=True)
            return 0
        if args.update_command == "stage":
            from core.updater.manifest import ReleaseManifest

            manifest = ReleaseManifest.from_file(Path(args.manifest)) if args.manifest else None
            print(_json(runtime.updater.stage_local_artifact(Path(args.artifact), manifest=manifest, expected_sha256=args.sha256 or None)), flush=True)
            return 0
        if args.update_command == "apply":
            if args.version == "latest" and not args.staged_path:
                print(_json(runtime.updater.apply_latest(dry_run=args.dry_run)), flush=True)
                return 0
            print(_json(runtime.updater.apply_staged(args.version, args.staged_path, dry_run=args.dry_run)), flush=True)
            return 0
        if args.update_command == "rollback":
            print(_json(runtime.updater.rollback()), flush=True)
            return 0
        if args.update_command == "resume":
            print(_json(runtime.updater.resume()), flush=True)
            return 0
        if args.update_command == "unlock":
            print(_json(runtime.updater.unlock_stale()), flush=True)
            return 0

    if args.command == "release" and args.release_command == "verify":
        from core.release_verify import verify_release_artifact

        result = verify_release_artifact(Path(args.artifact), deep=args.deep, python_bin=args.python)
        print(_json(result.to_dict()), flush=True)
        return 0 if result.ok else 1

    if args.command == "release" and args.release_command == "checklist":
        from core.release_checklist import run_release_drills

        result = run_release_drills(args.file)
        print(_json(result.to_dict()), flush=True)
        return 0 if result.ok else 1

    if args.command == "logs":
        runtime = _runtime(project_root)
        if args.logs_command == "flush":
            print(_json(asyncio.run(runtime.remote_logs.flush_buffer())), flush=True)
            return 0
        if args.logs_command == "buffer-status":
            root = runtime.remote_logs.buffer_dir or (runtime.settings.runtime_dir / "logs-buffer")
            files = sorted(root.glob("*.jsonl")) if root.exists() else []
            print(_json({"path": str(root), "files": len(files), "bytes": sum(item.stat().st_size for item in files)}), flush=True)
            return 0

    if args.command == "maintenance" and args.maintenance_command == "cleanup":
        from core.config import Settings
        from core.maintenance import cleanup_runtime

        settings = Settings.from_env()
        print(_json(cleanup_runtime(project_root, settings)), flush=True)
        return 0

    if args.command == "self-test":
        runtime = _runtime(project_root)
        doctor_profile = "production" if args.profile == "production" else "staging" if args.profile == "staging" else "dev"
        result = {
            "profile": args.profile,
            "doctor": runtime.doctor(offline=args.offline or args.profile == "local-memory", profile=doctor_profile),
            "ready": runtime.readiness_snapshot(),
            "dependencies": runtime.dependency_health_snapshot() if not args.offline else {"checks": []},
            "updates": runtime.updater.status().to_dict(),
            "metrics": {
                "events": runtime.event_bus.metrics().to_dict(),
                "delivery_backend": getattr(runtime.delivery, "backend", "memory"),
                "audit_backend": getattr(runtime.audit, "backend", "memory"),
            },
        }
        print(_json(result), flush=True)
        return 0 if not result["doctor"] and (args.profile == "local-memory" or result["ready"].get("ok")) else 1

    if args.command == "catalog":
        from core.catalog import CatalogEntry, RuntimeCatalogLock
        lock = RuntimeCatalogLock(project_root / "runtime" / "catalog" / "catalog.lock")
        if args.catalog_command == "list":
            print(_json({"items": lock.snapshot()}), flush=True)
            return 0
        if args.catalog_command in {"install", "update"}:
            result = lock.install(CatalogEntry(id=args.id, version=args.version, source=args.source, sha256=args.sha256), project_root=project_root)
            print(_json(result), flush=True)
            return 0
        if args.catalog_command == "verify":
            print(_json(lock.verify(project_root=project_root, entry_id=args.id)), flush=True)
            return 0
        if args.catalog_command == "rollback":
            print(_json(lock.rollback(args.id)), flush=True)
            return 0

    if args.command in {"migrate", "db-status"}:
        print("Схема PostgreSQL управляется Alembic.")
        print("Конфигурация: alembic.ini")
        print("Проверка контракта: cajeer-bots db check")
        return 0

    if args.command == "db":
        from core.config import Settings
        from core.db_async import REQUIRED_TABLES, check_schema

        settings = Settings.from_env()
        if args.db_command == "contract":
            print(_json({table: sorted(columns) for table, columns in REQUIRED_TABLES.items()}), flush=True)
            return 0
        if args.db_command == "alembic":
            print(_json({"config": settings.storage.alembic_config, "async_url_configured": bool(settings.storage.async_database_url)}), flush=True)
            return 0
        if args.db_command == "backup":
            from core.db_tools import backup_database
            print(_json(backup_database(settings.database_url, settings.runtime_dir / "backups" / "db", fmt=args.format, schema=args.schema or None, compress=not args.no_compress, keep_last=args.keep_last)), flush=True)
            return 0
        if args.db_command == "restore":
            from core.db_tools import restore_database
            print(_json(restore_database(settings.database_url, Path(args.backup_file), dry_run=args.dry_run)), flush=True)
            return 0
        if args.db_command in {"current", "history", "upgrade"}:
            import subprocess
            alembic_args = {"current": ["alembic", "-c", settings.storage.alembic_config, "current"], "history": ["alembic", "-c", settings.storage.alembic_config, "history"], "upgrade": ["alembic", "-c", settings.storage.alembic_config, "upgrade", args.revision]}[args.db_command]
            completed = subprocess.run(alembic_args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
            print(completed.stdout, end="", flush=True)
            return completed.returncode
        if args.db_command in {"check", "doctor"}:
            problems = asyncio.run(check_schema(settings.storage.async_database_url, settings.shared_schema))
            if problems:
                print("Проверка DB contract: есть проблемы")
                for problem in problems:
                    print(f"- {problem}")
                return 1
            print("Проверка DB contract: успешно")
            return 0

    if args.command == "distributed" and args.distributed_command == "protocol":
        from distributed.protocol import PROTOCOL_VERSIONS

        print(_json(PROTOCOL_VERSIONS), flush=True)
        return 0

    runtime = _runtime(project_root)

    if args.command == "doctor" and args.fix_permissions:
        fix_permissions(project_root)

    if args.command == "run":
        asyncio.run(runtime.run(args.target or runtime.settings.default_target))
        return 0

    if args.command == "doctor":
        problems = runtime.doctor(offline=args.offline, doctor_mode=args.mode, profile=args.profile)
        if problems:
            print("Проверка Cajeer Bots: есть проблемы")
            for problem in problems:
                print(f"- {problem}")
            return 1
        print("Проверка Cajeer Bots: успешно")
        return 0

    if args.command == "distributed" and args.distributed_command == "doctor":
        problems = runtime.doctor(offline=args.offline, doctor_mode="distributed")
        if problems:
            print("Проверка распределённого режима: есть проблемы")
            for problem in problems:
                print(f"- {problem}")
            return 1
        print("Проверка распределённого режима: успешно")
        return 0

    return 2
