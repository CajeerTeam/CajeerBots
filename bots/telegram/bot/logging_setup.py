from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys


SECRET_PATTERNS = [
    re.compile(r"Bearer\s+[A-Za-z0-9._:-]+", re.IGNORECASE),
    re.compile(r"(token\s*[:=]\s*)[^\s,;]+", re.IGNORECASE),
    re.compile(r"(secret\s*[:=]\s*)[^\s,;]+", re.IGNORECASE),
    re.compile(r"(signature\s*[:=]\s*)[^\s,;]+", re.IGNORECASE),
    re.compile(r"(https?://api\.telegram\.org/bot)[^/\s]+(/[^\s\"]*)?", re.IGNORECASE),
]


def _redact(text: str) -> str:
    value = text
    value = SECRET_PATTERNS[0].sub("Bearer ***", value)
    value = SECRET_PATTERNS[1].sub(r"\1***", value)
    value = SECRET_PATTERNS[2].sub(r"\1***", value)
    value = SECRET_PATTERNS[3].sub(r"\1***", value)
    value = SECRET_PATTERNS[4].sub(lambda m: f"{m.group(1)}***{m.group(2) or ''}", value)
    return value


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = _redact(str(record.msg))
        if not record.args:
            return True
        if isinstance(record.args, dict):
            record.args = {k: (_redact(v) if isinstance(v, str) else v) for k, v in record.args.items()}
            return True
        if isinstance(record.args, tuple):
            record.args = tuple(_redact(v) if isinstance(v, str) else v for v in record.args)
            return True
        if isinstance(record.args, list):
            record.args = [_redact(v) if isinstance(v, str) else v for v in record.args]
            return True
        if isinstance(record.args, str):
            record.args = (_redact(record.args),)
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": _redact(record.getMessage()),
        }
        if record.exc_info:
            payload["exception"] = _redact(self.formatException(record.exc_info))
        return json.dumps(payload, ensure_ascii=False)



def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _read_secret_file(raw: str | None) -> str:
    if not raw or not raw.strip():
        return ""
    try:
        return Path(raw.strip()).expanduser().read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _env_or_file(name: str) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    return _read_secret_file(os.getenv(f"{name}_FILE"))


def _close_existing_handlers(root: logging.Logger) -> None:
    for handler in list(root.handlers):
        root.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass


def _app_version() -> str:
    try:
        from nmbot import __version__

        return str(__version__)
    except Exception:
        return os.getenv("APP_VERSION", "")


def _remote_log_level(default_level: str) -> int:
    configured = os.getenv("REMOTE_LOGS_LEVEL", default_level).strip().upper() or default_level.upper()
    return getattr(logging, configured, logging.INFO)


def _add_remote_log_handler(root: logging.Logger, redactor: logging.Filter, *, default_level: str) -> None:
    if not _env_bool("REMOTE_LOGS_ENABLED", default=False):
        return
    token = _env_or_file("REMOTE_LOGS_TOKEN")
    if not token:
        root.warning("remote logs enabled but REMOTE_LOGS_TOKEN is empty; remote logging disabled")
        return
    url = os.getenv("REMOTE_LOGS_URL", "https://logs.cajeer.ru/api/v1/ingest").strip() or "https://logs.cajeer.ru/api/v1/ingest"
    try:
        from nmbot.remote_log_handler import RemoteLogHandler

        handler = RemoteLogHandler(
            url=url,
            token=token,
            project=os.getenv("REMOTE_LOGS_PROJECT", "NeverMine").strip() or "NeverMine",
            bot=os.getenv("REMOTE_LOGS_BOT", "NMTelegramBot").strip() or "NMTelegramBot",
            environment=os.getenv("REMOTE_LOGS_ENVIRONMENT", "production").strip() or "production",
            version=_app_version(),
            level=_remote_log_level(default_level),
        )
        handler.addFilter(redactor)
        root.addHandler(handler)
        root.info(
            "remote logging enabled url=%s project=%s bot=%s environment=%s",
            url,
            handler.project,
            handler.bot,
            handler.environment,
        )
    except Exception as exc:
        root.warning("remote logging disabled: %s", exc)

def _fallback_log_file(original: Path) -> Path:
    fallback_base = Path(os.getenv("NMBOT_FALLBACK_RUNTIME_DIR", "/tmp/nmtelegrambot")) / "logs"
    return (fallback_base / original.name).resolve()


def _should_warn_about_log_fallback() -> bool:
    return os.getenv("NMBOT_LOG_FALLBACK_WARN", "").strip().lower() in {"1", "true", "yes", "on"}


def _log_file_is_writable(log_file: Path) -> bool:
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        if log_file.exists() and not os.access(log_file, os.W_OK):
            return False
        probe = log_file.parent / f".nmbot-log-probe-{os.getpid()}"
        with probe.open("w", encoding="utf-8") as handle:
            handle.write("ok")
        probe.unlink(missing_ok=True)
        return True
    except OSError:
        return False


def _safe_prepare_log_file(log_file: Path) -> Path:
    if _log_file_is_writable(log_file):
        return log_file
    fallback = _fallback_log_file(log_file)
    fallback.parent.mkdir(parents=True, exist_ok=True)
    if _should_warn_about_log_fallback():
        print(f"[WARN] Log path {log_file} is not writable, using fallback {fallback}", file=sys.stderr)
    return fallback


def configure_logging(level: str, log_file: Path, *, log_format: str = "plain") -> None:
    log_file = _safe_prepare_log_file(log_file)
    formatter: logging.Formatter = JsonFormatter() if log_format == "json" else logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    _close_existing_handlers(root)
    redactor = RedactingFilter()
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.addFilter(redactor)
    root.addHandler(console)
    try:
        file_handler = RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=5, encoding="utf-8")
    except OSError:
        fallback = _fallback_log_file(log_file)
        fallback.parent.mkdir(parents=True, exist_ok=True)
        if _should_warn_about_log_fallback():
            print(f"[WARN] File logging fallback engaged: {fallback}", file=sys.stderr)
        file_handler = RotatingFileHandler(fallback, maxBytes=1_000_000, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.addFilter(redactor)
    root.addHandler(file_handler)
    _add_remote_log_handler(root, redactor, default_level=level)
