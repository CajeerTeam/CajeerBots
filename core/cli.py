from __future__ import annotations
import argparse, asyncio, json
from pathlib import Path
from cajeer_bots.config import Settings
from cajeer_bots.logging import configure_logging
from cajeer_bots.runtime import Runtime

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cajeer-bots", description="Cajeer Bots Platform CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="Run runtime mode")
    run.add_argument("mode", choices=["all", "telegram", "discord", "vkontakte", "worker", "api", "bridge"])
    doctor = sub.add_parser("doctor", help="Validate configuration and platform state")
    doctor.add_argument("--offline", action="store_true", help="Skip external services checks")
    sub.add_parser("migrate", help="Apply database migrations")
    sub.add_parser("modules", help="List modules")
    sub.add_parser("plugins", help="List plugins")
    sub.add_parser("adapters", help="List adapters")
    return parser

def _manifest_json(items):
    return json.dumps([m.__dict__ | {"path": str(m.path)} for m in items], ensure_ascii=False, indent=2)

def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    runtime = Runtime(settings, project_root=Path.cwd())
    if args.command == "run":
        asyncio.run(runtime.run(args.mode)); return 0
    if args.command == "doctor":
        problems = runtime.doctor(offline=args.offline)
        if problems:
            print("Cajeer Bots doctor: FAILED")
            for problem in problems:
                print(f"- {problem}")
            return 1
        print("Cajeer Bots doctor: OK"); return 0
    if args.command == "migrate":
        print(f"Applied migration files: {runtime.migrate()}"); return 0
    if args.command == "modules":
        print(_manifest_json(runtime.registry.modules())); return 0
    if args.command == "plugins":
        print(_manifest_json(runtime.registry.plugins())); return 0
    if args.command == "adapters":
        print(_manifest_json(runtime.registry.adapters())); return 0
    return 2
