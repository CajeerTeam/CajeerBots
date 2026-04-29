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
    run.add_argument(
        "target",
        nargs="?",
        choices=["all", "telegram", "discord", "vkontakte", "fake", "worker", "api", "bridge"],
        help="Цель запуска: все адаптеры, отдельный адаптер, рабочий процесс, API или bridge.",
    )

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

    secrets = sub.add_parser("secrets", help="Операции с секретами.")
    secrets_sub = secrets.add_subparsers(dest="secrets_command", required=True)
    secrets_sub.add_parser("generate", help="Сгенерировать безопасный env-блок секретов.")

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
    distributed_sub.add_parser("doctor", help="Проверить настройки распределённого режима.").add_argument(
        "--offline", action="store_true", help="Не проверять внешние сервисы."
    )
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

    from core.config import Settings
    from core.logging import configure_logging
    from core.runtime import Runtime

    if args.command == "doctor" and args.fix_permissions:
        fix_permissions(project_root)

    settings = Settings.from_env()
    configure_logging(settings.log_level)
    runtime = Runtime(settings, project_root=project_root)

    if args.command == "run":
        asyncio.run(runtime.run(args.target or settings.default_target))
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
