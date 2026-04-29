from __future__ import annotations

import argparse
import json
from pathlib import Path

from core.commands import build_default_commands
from core.registry import Registry


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cajeer-bots",
        description="Cajeer Bots — универсальная платформа для запуска и расширения ботов.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Запустить выбранный режим платформы.")
    run.add_argument(
        "mode",
        choices=["all", "telegram", "discord", "vkontakte", "worker", "api", "bridge"],
        help="Режим запуска: все адаптеры, отдельный адаптер, рабочий процесс, API или шина событий.",
    )

    doctor = sub.add_parser("doctor", help="Проверить конфигурацию и состояние платформы.")
    doctor.add_argument("--offline", action="store_true", help="Не проверять внешние сервисы и PostgreSQL.")

    sub.add_parser("modules", help="Показать зарегистрированные модули.")
    sub.add_parser("plugins", help="Показать зарегистрированные плагины.")
    sub.add_parser("adapters", help="Показать зарегистрированные адаптеры.")
    sub.add_parser("commands", help="Показать зарегистрированные команды.")
    sub.add_parser("migrate", help="Показать статус управления схемой БД. Встроенные миграции не используются.")
    sub.add_parser("db-status", help="Показать статус модели БД без выполнения миграций.")
    return parser


def _json(items: object) -> str:
    return json.dumps(items, ensure_ascii=False, indent=2)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    project_root = Path.cwd()

    if args.command in {"modules", "plugins", "adapters"}:
        registry = Registry(project_root)
        if args.command == "modules":
            print(_json([item.to_dict() for item in registry.modules()]), flush=True)
        elif args.command == "plugins":
            print(_json([item.to_dict() for item in registry.plugins()]), flush=True)
        else:
            print(_json([item.to_dict() for item in registry.adapters()]), flush=True)
        return 0

    if args.command == "commands":
        print(_json([item.to_dict() for item in build_default_commands().list()]), flush=True)
        return 0

    if args.command in {"migrate", "db-status"}:
        print("Встроенные миграции не используются.")
        print("Структура PostgreSQL управляется внешним эксплуатационным слоем по контракту из GitHub Wiki.")
        print("Команда не изменяет базу данных и безопасна для запуска в любом окружении.")
        return 0

    from core.config import Settings
    from core.logging import configure_logging
    from core.runtime import Runtime

    settings = Settings.from_env()
    configure_logging(settings.log_level)
    runtime = Runtime(settings, project_root=project_root)

    if args.command == "run":
        import asyncio

        asyncio.run(runtime.run(args.mode))
        return 0

    if args.command == "doctor":
        problems = runtime.doctor(offline=args.offline)
        if problems:
            print("Проверка Cajeer Bots: есть проблемы")
            for problem in problems:
                print(f"- {problem}")
            return 1
        print("Проверка Cajeer Bots: успешно")
        return 0

    return 2
