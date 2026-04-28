from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: str, *, json_logs: bool = False, log_dir: Path | None = None, app_version: str | None = None) -> None:
    handlers: list[logging.Handler] = []

    console = logging.StreamHandler()
    file_handler: logging.Handler | None = None
    if log_dir is not None:
        log_path = log_dir / "nmdiscordbot.log"
        try:
            log_dir.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_path, encoding="utf-8")
        except OSError:
            fallback_dir = Path(os.getenv("TMPDIR") or "/tmp", "nmdiscordbot", "logs")
            fallback_path = fallback_dir / "nmdiscordbot.log"
            try:
                fallback_dir.mkdir(parents=True, exist_ok=True)
                file_handler = logging.FileHandler(fallback_path, encoding="utf-8")
                print(
                    f"[WARN] Log path {log_path} is not writable, using fallback {fallback_path}",
                    file=sys.stderr,
                )
            except OSError:
                print(
                    f"[WARN] Log path {log_path} is not writable; file logging disabled",
                    file=sys.stderr,
                )
                file_handler = None

    if json_logs:
        formatter: logging.Formatter = JsonFormatter()
    else:
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    remote_handler: logging.Handler | None = None
    if os.getenv("REMOTE_LOGS_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}:
        try:
            from .remote_logs import create_remote_log_handler_from_env

            remote_handler = create_remote_log_handler_from_env(default_project="NeverMine", default_bot="NMDiscordBot", default_version=app_version or "")
            if remote_handler is None:
                print(
                    "[WARN] REMOTE_LOGS_ENABLED=true, but REMOTE_LOGS_URL or REMOTE_LOGS_TOKEN is empty; remote logs disabled",
                    file=sys.stderr,
                )
        except Exception as exc:
            print(f"[WARN] Remote logs handler could not be initialized: {exc}", file=sys.stderr)
            remote_handler = None

    console.setFormatter(formatter)
    handlers.append(console)
    if file_handler is not None:
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    if remote_handler is not None:
        handlers.append(remote_handler)

    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), handlers=handlers, force=True)
