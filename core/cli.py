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
    doctor.add_argument("--fix-permissions", action="store_true", help="Исправить права shell/python entrypoints перед проверкой.")

    sub.add_parser("init", help="Создать .env, runtime-каталоги и базовые секреты.")
    sub.add_parser("fix-permissions", help="Исправить права запускаемых файлов.")
    sub.add_parser("modules", help="Показать зарегистрированные модули.")
    sub.add_parser("plugins", help="Показать зарегистрированные плагины.")
    sub.add_parser("adapters", help="Показать зарегистрированные адаптеры.")
    sub.add_parser("components", help="Показать runtime-компоненты с entrypoint.")
    sub.add_parser("commands", help="Показать зарегистрированные команды.")

    update = sub.add_parser("update", help="Безопасные обновления из GitHub Releases или локального tar.gz.")
    update_sub = update.add_subparsers(dest="update_command", required=True)
    update_sub.add_parser("status", help="Показать локальный статус updater.")
    update_sub.add_parser("history", help="Показать историю обновлений.")
    update_sub.add_parser("check", help="Проверить доступное обновление.")
    update_sub.add_parser("download", help="Скачать latest artifact из GitHub Releases.")
    update_sub.add_parser("stage-latest", help="Скачать и распаковать latest artifact в staging.")
    stage = update_sub.add_parser("stage", help="Проверить и распаковать локальный release artifact в staging.")
    stage.add_argument("artifact", help="Путь к CajeerBots-*.tar.gz")
    stage.add_argument("--manifest", help="Путь к *.release.json", default="")
    stage.add_argument("--sha256", help="Ожидаемый sha256", default="")
    apply = update_sub.add_parser("apply", help="Переключить current на staged release.")
    apply.add_argument("--version", required=True)
    apply.add_argument("--staged-path", default="")
    update_sub.add_parser("rollback", help="Откатить current на previous.")

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
    tokens_sub.add_parser("list", help="Показать token registry без значений токенов.")

    db = sub.add_parser("db", help="Команды PostgreSQL/SQLAlchemy/Alembic.")
    db_sub = db.add_subparsers(dest="db_command", required=True)
    db_sub.add_parser("check", help="Проверить внешний DB contract без изменения схемы.")
    db_sub.add_parser("contract", help="Показать обязательные таблицы и поля DB contract.")
    db_sub.add_parser("doctor", help="Проверить async-подключение и DB contract.")
    db_sub.add_parser("alembic", help="Показать путь к Alembic-конфигурации.")

    sub.add_parser("migrate", help="Показать статус управления схемой БД через Alembic.")
    sub.add_parser("db-status", help="Показать статус модели БД без выполнения миграций.")

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
        if args.tokens_command == "list":
            print(_json({"items": registry.snapshot()}), flush=True)
            return 0

    if args.command in {"modules", "plugins", "adapters"}:
        from core.config import Settings

        settings = Settings.from_env()
        registry = Registry(project_root, settings=settings)
        if args.command == "modules":
            print(_json([item.to_dict() for item in registry.modules()]), flush=True)
        elif args.command == "plugins":
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
            staged_path = args.staged_path
            if args.version == "latest" and not staged_path:
                staged = runtime.updater.stage_latest()
                staged_path = str(staged.get("staged_path") or "")
                manifest = staged.get("manifest") if isinstance(staged.get("manifest"), dict) else {}
                args.version = str(manifest.get("version") or runtime.version)
            print(_json(runtime.updater.apply_staged(args.version, staged_path)), flush=True)
            return 0
        if args.update_command == "rollback":
            print(_json(runtime.updater.rollback()), flush=True)
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
        problems = runtime.doctor(offline=args.offline, doctor_mode=args.mode)
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
