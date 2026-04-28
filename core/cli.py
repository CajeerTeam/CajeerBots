from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from core.config import Settings
from core.logging import configure_logging
from core.runtime import Runtime


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
    sub.add_parser("migrate", help="Показать статус миграций. В этом каркасе миграции не используются.")
    return parser


def _manifest_json(items):
    return json.dumps([m.__dict__ | {"path": str(m.path)} for m in items], ensure_ascii=False, indent=2)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    runtime = Runtime(settings, project_root=Path.cwd())

    if args.command == "run":
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

    if args.command == "migrate":
        print("Миграции не используются: структура базы данных управляется внешним слоем эксплуатации.")
        return 0

    if args.command == "modules":
        print(_manifest_json(runtime.registry.modules()))
        return 0

    if args.command == "plugins":
        print(_manifest_json(runtime.registry.plugins()))
        return 0

    if args.command == "adapters":
        print(_manifest_json(runtime.registry.adapters()))
        return 0

    return 2
