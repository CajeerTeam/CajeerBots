"""Asynchronous remote logging client for logs.cajeer.ru.

The handler is intentionally dependency-free: it uses the Python standard library
only, so BotHost deployments do not need additional packages. It is safe to keep
it enabled in production: network errors are spooled locally and never interrupt
the bot process.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import queue
import re
import secrets
import socket
import threading
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


_SECRET_PATTERNS = [
    re.compile(r"Bearer\s+[A-Za-z0-9._:-]+", re.IGNORECASE),
    re.compile(r"(token\s*[:=]\s*)[^\s,;]+", re.IGNORECASE),
    re.compile(r"(secret\s*[:=]\s*)[^\s,;]+", re.IGNORECASE),
    re.compile(r"(signature\s*[:=]\s*)[^\s,;]+", re.IGNORECASE),
    re.compile(r"(password\s*[:=]\s*)[^\s,;]+", re.IGNORECASE),
    re.compile(r"(postgresql://[^:\s/@]+:)[^@\s]+(@)", re.IGNORECASE),
    re.compile(r"(https?://api\.telegram\.org/bot)[^/\s]+(/[^\s\"]*)?", re.IGNORECASE),
]


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, min_value: int | None = None) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        parsed = int(value)
    except ValueError:
        return default
    if min_value is not None:
        parsed = max(min_value, parsed)
    return parsed


def _env_float(name: str, default: float, *, min_value: float | None = None) -> float:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    try:
        parsed = float(value)
    except ValueError:
        return default
    if min_value is not None:
        parsed = max(min_value, parsed)
    return parsed


def _redact(text: str) -> str:
    value = text
    for pattern in _SECRET_PATTERNS:
        if pattern.pattern.startswith("(postgresql://"):
            value = pattern.sub(r"\1***\2", value)
        elif pattern.pattern.startswith("(https?://api"):
            value = pattern.sub(lambda m: f"{m.group(1)}***{m.group(2) or ''}", value)
        elif "Bearer" in pattern.pattern:
            value = pattern.sub("Bearer ***", value)
        else:
            value = pattern.sub(r"\1***", value)
    return value


def _prepare_spool_dir(bot: str, explicit: Optional[str]) -> Path:
    bot_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", bot.strip() or "unknown")
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit).expanduser())
    shared_dir = os.getenv("SHARED_DIR", "").strip()
    if shared_dir:
        candidates.append(Path(shared_dir).expanduser() / "remote_logs")
    data_dir = os.getenv("DATA_DIR", "").strip() or os.getenv("NMBOT_RUNTIME_DIR", "").strip()
    if data_dir:
        candidates.append(Path(data_dir).expanduser() / "remote_logs")
    candidates.append(Path("/tmp/nmtelegrambot/remote_logs"))

    for base in candidates:
        path = (base / bot_name).resolve()
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe = path / f".remote-log-probe-{os.getpid()}"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            return path
        except OSError:
            continue
    fallback = Path("/tmp/nmtelegrambot/remote_logs") / bot_name
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback.resolve()


class RemoteLogHandler(logging.Handler):
    def __init__(
        self,
        url: Optional[str] = None,
        token: Optional[str] = None,
        project: str = "NeverMine",
        bot: str = "NMTelegramBot",
        environment: str = "production",
        version: Optional[str] = None,
        batch_size: int = 50,
        flush_interval: float = 5.0,
        timeout: float = 3.0,
        level: int = logging.INFO,
        sign_requests: Optional[bool] = None,
        spool_dir: Optional[str] = None,
        max_spool_files: int = 200,
    ) -> None:
        super().__init__(level=level)
        self.url = (url or os.getenv("REMOTE_LOGS_URL", "")).strip()
        self.token = (token or os.getenv("REMOTE_LOGS_TOKEN", "")).strip()
        self.project = os.getenv("REMOTE_LOGS_PROJECT", project).strip() or project
        self.bot = os.getenv("REMOTE_LOGS_BOT", bot).strip() or bot
        self.environment = os.getenv("REMOTE_LOGS_ENVIRONMENT", environment).strip() or environment
        self.version = os.getenv("APP_VERSION", version or "").strip()
        self.batch_size = _env_int("REMOTE_LOGS_BATCH_SIZE", batch_size, min_value=1)
        self.flush_interval = _env_float("REMOTE_LOGS_FLUSH_INTERVAL", flush_interval, min_value=0.5)
        self.timeout = _env_float("REMOTE_LOGS_TIMEOUT", timeout, min_value=0.5)
        self.sign_requests = _env_bool("REMOTE_LOGS_SIGN_REQUESTS", False) if sign_requests is None else sign_requests
        self.host = socket.gethostname()
        self.max_spool_files = _env_int("REMOTE_LOGS_MAX_SPOOL_FILES", max_spool_files, min_value=1)
        self.spool_dir = _prepare_spool_dir(self.bot, spool_dir or os.getenv("REMOTE_LOGS_SPOOL_DIR"))
        self._queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=_env_int("REMOTE_LOGS_QUEUE_SIZE", 5000, min_value=1))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._worker, name="nmtgbot-remote-log-handler", daemon=True)
        self.dropped = 0
        self.sent = 0
        self.spooled = 0
        self.replayed = 0
        self.failed_batches = 0
        self.last_error: Optional[str] = None
        if self.url and self.token:
            self._thread.start()

    def emit(self, record: logging.LogRecord) -> None:
        if not self.url or not self.token:
            return
        try:
            event: Dict[str, Any] = {
                "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
                "level": record.levelname,
                "project": self.project,
                "bot": self.bot,
                "environment": self.environment,
                "version": self.version,
                "host": self.host,
                "logger": record.name,
                "message": _redact(record.getMessage()),
            }
            if record.exc_info:
                event["exception"] = _redact("".join(traceback.format_exception(*record.exc_info)))
            extra = getattr(record, "context", None)
            if isinstance(extra, dict):
                event["context"] = {str(k): (_redact(v) if isinstance(v, str) else v) for k, v in extra.items()}
            trace_id = getattr(record, "trace_id", None)
            if trace_id:
                event["trace_id"] = str(trace_id)
            self._queue.put_nowait(event)
        except queue.Full:
            self.dropped += 1
            self._spool_batch([self._fallback_record(record)])
        except Exception:
            self.handleError(record)

    def diagnostics(self) -> Dict[str, Any]:
        return {
            "enabled": bool(self.url and self.token),
            "sent": self.sent,
            "dropped": self.dropped,
            "spooled": self.spooled,
            "replayed": self.replayed,
            "failed_batches": self.failed_batches,
            "queue_size": self._queue.qsize(),
            "spool_dir": str(self.spool_dir),
            "last_error": self.last_error,
        }

    def _worker(self) -> None:
        batch: List[Dict[str, Any]] = []
        last = time.monotonic()
        backoff = 1.0
        while not self._stop.is_set():
            self._replay_spool_once()
            timeout = max(0.1, self.flush_interval - (time.monotonic() - last))
            try:
                item = self._queue.get(timeout=timeout)
                batch.append(item)
                if len(batch) >= self.batch_size:
                    if self._send(batch):
                        backoff = 1.0
                    else:
                        self._spool_batch(batch)
                        self._stop.wait(backoff)
                        backoff = min(backoff * 2, 30.0)
                    batch = []
                    last = time.monotonic()
            except queue.Empty:
                if batch:
                    if self._send(batch):
                        backoff = 1.0
                    else:
                        self._spool_batch(batch)
                        self._stop.wait(backoff)
                        backoff = min(backoff * 2, 30.0)
                    batch = []
                last = time.monotonic()
        if batch and not self._send(batch):
            self._spool_batch(batch)

    def _signed_headers(self, body: bytes) -> Dict[str, str]:
        timestamp = str(int(time.time()))
        nonce = secrets.token_hex(16)
        digest = hashlib.sha256(body).hexdigest()
        canonical = f"{timestamp}\n{nonce}\n{digest}".encode("utf-8")
        signature = hmac.new(self.token.encode("utf-8"), canonical, hashlib.sha256).hexdigest()
        return {"X-Log-Timestamp": timestamp, "X-Log-Nonce": nonce, "X-Log-Signature": signature}

    def _send(self, batch: List[Dict[str, Any]]) -> bool:
        body = json.dumps({"logs": batch}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-Log-Token": self.token,
            "User-Agent": "nmtgbot-remote-log-handler/1.0",
        }
        if self.sign_requests:
            headers.update(self._signed_headers(body))
        req = urllib.request.Request(self.url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                if 200 <= resp.status < 300:
                    self.sent += len(batch)
                    return True
                self.failed_batches += 1
                self.last_error = f"HTTP {resp.status}"
                return False
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            self.failed_batches += 1
            self.last_error = str(exc)
            return False

    def _spool_batch(self, batch: List[Dict[str, Any]]) -> None:
        try:
            self._cleanup_spool()
            name = f"{int(time.time())}-{secrets.token_hex(8)}.ndjson"
            path = self.spool_dir / name
            with path.open("w", encoding="utf-8") as fh:
                for event in batch:
                    fh.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
            self.spooled += len(batch)
        except Exception as exc:
            self.last_error = f"spool failed: {exc}"
            self.dropped += len(batch)

    def _replay_spool_once(self) -> None:
        files = sorted(self.spool_dir.glob("*.ndjson"))[:3]
        for path in files:
            try:
                batch = list(self._read_spool_file(path))
                if not batch:
                    path.unlink(missing_ok=True)
                    continue
                if self._send(batch):
                    self.replayed += len(batch)
                    path.unlink(missing_ok=True)
                else:
                    return
            except Exception as exc:
                self.last_error = f"spool replay failed: {exc}"
                return

    def _read_spool_file(self, path: Path) -> Iterable[Dict[str, Any]]:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj

    def _cleanup_spool(self) -> None:
        files = sorted(self.spool_dir.glob("*.ndjson"))
        overflow = len(files) - self.max_spool_files
        if overflow <= 0:
            return
        for path in files[:overflow]:
            path.unlink(missing_ok=True)

    def _fallback_record(self, record: logging.LogRecord) -> Dict[str, Any]:
        return {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "project": self.project,
            "bot": self.bot,
            "environment": self.environment,
            "host": self.host,
            "logger": record.name,
            "message": _redact(record.getMessage()),
        }

    def close(self) -> None:
        self._stop.set()
        if self._thread.is_alive() and threading.current_thread() is not self._thread:
            close_timeout = _env_float("REMOTE_LOGS_CLOSE_TIMEOUT", 0.2, min_value=0.0)
            if close_timeout > 0:
                self._thread.join(timeout=close_timeout)
        super().close()
