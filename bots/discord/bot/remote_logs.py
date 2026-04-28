"""Remote log shipping client for logs.cajeer.ru.

The module is intentionally stdlib-only so NMDiscordBot can ship logs to the
self-hosted logs service from BotHost without extra dependencies. Configure with
REMOTE_LOGS_* environment variables.
"""
from __future__ import annotations

import atexit
import hashlib
import hmac
import json
import logging
import os
import queue
import secrets
import socket
import threading
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    return default


def env_int(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    raw = os.getenv(name)
    try:
        value = int(str(raw if raw is not None else default).strip())
    except (TypeError, ValueError):
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def env_float(name: str, default: float, *, minimum: float | None = None, maximum: float | None = None) -> float:
    raw = os.getenv(name)
    try:
        value = float(str(raw if raw is not None else default).strip())
    except (TypeError, ValueError):
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def remote_logs_enabled() -> bool:
    return env_bool("REMOTE_LOGS_ENABLED", False)


def _level_from_env(default: int = logging.INFO) -> int:
    raw = (os.getenv("REMOTE_LOGS_LEVEL") or "").strip()
    if not raw:
        return default
    if raw.isdigit():
        return int(raw)
    return int(getattr(logging, raw.upper(), default))


def _candidate_spool_dirs(bot: str, explicit: str | None = None) -> list[Path]:
    candidates: list[Path] = []
    for raw in (
        explicit,
        os.getenv("REMOTE_LOGS_SPOOL_DIR"),
        str(Path(os.getenv("DATA_DIR", "/app/data")) / "remote-logs-spool"),
        os.getenv("SHARED_DIR"),
        str(Path(os.getenv("TMPDIR") or "/tmp") / "nmdiscordbot" / "remote-logs-spool"),
    ):
        if not raw:
            continue
        path = Path(raw).expanduser()
        if path.name != bot:
            path = path / bot
        if path not in candidates:
            candidates.append(path)
    return candidates


def _ensure_writable_dir(candidates: Iterable[Path]) -> Path:
    last_error: Exception | None = None
    for path in candidates:
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe = path / ".write-test"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            return path
        except Exception as exc:  # pragma: no cover - platform dependent permissions
            last_error = exc
            continue
    fallback = Path(os.getenv("TMPDIR") or "/tmp") / "nmdiscordbot" / "remote-logs-spool"
    fallback.mkdir(parents=True, exist_ok=True)
    if last_error:
        # Keep the exception out of startup logs unless the final fallback fails.
        pass
    return fallback


class RemoteLogHandler(logging.Handler):
    """Batching logging handler for the Cajeer/NeverMine logs service."""

    def __init__(
        self,
        *,
        url: str,
        token: str,
        project: str = "NeverMine",
        bot: str = "NMDiscordBot",
        environment: str = "production",
        version: str = "",
        batch_size: int = 25,
        flush_interval: float = 5.0,
        timeout: float = 3.0,
        level: int = logging.INFO,
        sign_requests: bool = False,
        spool_dir: str | None = None,
        max_spool_files: int = 200,
        queue_size: int = 5000,
    ) -> None:
        super().__init__(level=level)
        self.url = url.strip()
        self.token = token.strip()
        self.project = project.strip() or "NeverMine"
        self.bot = bot.strip() or "NMDiscordBot"
        self.environment = environment.strip() or "production"
        self.version = version.strip()
        self.batch_size = max(1, min(int(batch_size), 500))
        self.flush_interval = max(0.2, float(flush_interval))
        self.timeout = max(0.2, float(timeout))
        self.sign_requests = bool(sign_requests)
        self.host = socket.gethostname()
        self.max_spool_files = max(1, int(max_spool_files))
        self.spool_dir = _ensure_writable_dir(_candidate_spool_dirs(self.bot, spool_dir))
        self._queue: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=max(1, int(queue_size)))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._worker, name="nmdiscordbot-remote-log-handler", daemon=True)
        self.dropped = 0
        self.sent = 0
        self.spooled = 0
        self.replayed = 0
        self.failed_batches = 0
        self.last_error: str | None = None
        self._started = False
        if self.url and self.token:
            self._thread.start()
            self._started = True
            atexit.register(self.close)

    @classmethod
    def from_env(cls, *, default_project: str = "NeverMine", default_bot: str = "NMDiscordBot", default_version: str = "") -> Optional["RemoteLogHandler"]:
        if not remote_logs_enabled():
            return None
        url = (os.getenv("REMOTE_LOGS_URL") or "").strip()
        token = (os.getenv("REMOTE_LOGS_TOKEN") or "").strip()
        if not url or not token:
            return None
        return cls(
            url=url,
            token=token,
            project=os.getenv("REMOTE_LOGS_PROJECT", default_project),
            bot=os.getenv("REMOTE_LOGS_BOT", default_bot),
            environment=os.getenv("REMOTE_LOGS_ENVIRONMENT", os.getenv("APP_ENV", "production")),
            version=os.getenv("APP_VERSION", default_version),
            batch_size=env_int("REMOTE_LOGS_BATCH_SIZE", 25, minimum=1, maximum=500),
            flush_interval=env_float("REMOTE_LOGS_FLUSH_INTERVAL", 5.0, minimum=0.2, maximum=60.0),
            timeout=env_float("REMOTE_LOGS_TIMEOUT", 3.0, minimum=0.2, maximum=30.0),
            level=_level_from_env(logging.INFO),
            sign_requests=env_bool("REMOTE_LOGS_SIGN_REQUESTS", False),
            spool_dir=os.getenv("REMOTE_LOGS_SPOOL_DIR"),
            max_spool_files=env_int("REMOTE_LOGS_MAX_SPOOL_FILES", 200, minimum=1, maximum=10000),
            queue_size=env_int("REMOTE_LOGS_QUEUE_SIZE", 5000, minimum=100, maximum=100000),
        )

    def emit(self, record: logging.LogRecord) -> None:
        if not self.url or not self.token:
            return
        try:
            event: dict[str, Any] = {
                "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
                "level": record.levelname,
                "project": self.project,
                "bot": self.bot,
                "environment": self.environment,
                "version": self.version,
                "host": self.host,
                "logger": record.name,
                "message": record.getMessage(),
            }
            if record.exc_info:
                event["exception"] = "".join(traceback.format_exception(*record.exc_info))
            context = getattr(record, "context", None)
            if isinstance(context, dict):
                event["context"] = context
            trace_id = getattr(record, "trace_id", None)
            if trace_id:
                event["trace_id"] = str(trace_id)
            self._queue.put_nowait(event)
        except queue.Full:
            self.dropped += 1
            self._spool_batch([self._fallback_record(record)])
        except Exception:
            self.handleError(record)

    def diagnostics(self) -> dict[str, Any]:
        return {
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
        batch: list[dict[str, Any]] = []
        last = time.monotonic()
        backoff = 1.0
        while not self._stop.is_set():
            self._replay_spool_once()
            timeout = max(0.1, self.flush_interval - (time.monotonic() - last))
            try:
                item = self._queue.get(timeout=timeout)
                batch.append(item)
                if len(batch) >= self.batch_size:
                    backoff = self._flush_batch(batch, backoff)
                    batch = []
                    last = time.monotonic()
            except queue.Empty:
                if batch:
                    backoff = self._flush_batch(batch, backoff)
                    batch = []
                last = time.monotonic()
        if batch and not self._send(batch):
            self._spool_batch(batch)

    def _flush_batch(self, batch: list[dict[str, Any]], backoff: float) -> float:
        if self._send(batch):
            return 1.0
        self._spool_batch(batch)
        time.sleep(backoff)
        return min(backoff * 2, 30.0)

    def _signed_headers(self, body: bytes) -> dict[str, str]:
        timestamp = str(int(time.time()))
        nonce = secrets.token_hex(16)
        digest = hashlib.sha256(body).hexdigest()
        canonical = f"{timestamp}\n{nonce}\n{digest}".encode("utf-8")
        signature = hmac.new(self.token.encode("utf-8"), canonical, hashlib.sha256).hexdigest()
        return {"X-Log-Timestamp": timestamp, "X-Log-Nonce": nonce, "X-Log-Signature": signature}

    def _send(self, batch: list[dict[str, Any]]) -> bool:
        body = json.dumps({"logs": batch}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-Log-Token": self.token,
            "User-Agent": "nmdiscordbot-remote-log-handler/1.0",
        }
        if self.sign_requests:
            headers.update(self._signed_headers(body))
        request = urllib.request.Request(self.url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                if 200 <= response.status < 300:
                    self.sent += len(batch)
                    return True
                self.failed_batches += 1
                self.last_error = f"HTTP {response.status}"
                return False
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            self.failed_batches += 1
            self.last_error = str(exc)
            return False

    def _spool_batch(self, batch: list[dict[str, Any]]) -> None:
        try:
            self._cleanup_spool()
            path = self.spool_dir / f"{int(time.time())}-{secrets.token_hex(8)}.ndjson"
            with path.open("w", encoding="utf-8") as fh:
                for event in batch:
                    fh.write(json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n")
            self.spooled += len(batch)
        except Exception as exc:
            self.last_error = f"spool failed: {exc}"
            self.dropped += len(batch)

    def _replay_spool_once(self) -> None:
        for path in sorted(self.spool_dir.glob("*.ndjson"))[:3]:
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

    def _read_spool_file(self, path: Path) -> Iterable[dict[str, Any]]:
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

    def _fallback_record(self, record: logging.LogRecord) -> dict[str, Any]:
        return {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "project": self.project,
            "bot": self.bot,
            "environment": self.environment,
            "host": self.host,
            "logger": record.name,
            "message": record.getMessage(),
        }

    def close(self) -> None:
        try:
            self._stop.set()
            if self._started and self._thread.is_alive() and threading.current_thread() is not self._thread:
                self._thread.join(timeout=self.flush_interval + 1)
        finally:
            super().close()


def create_remote_log_handler_from_env(*, default_project: str = "NeverMine", default_bot: str = "NMDiscordBot", default_version: str = "") -> Optional[RemoteLogHandler]:
    return RemoteLogHandler.from_env(default_project=default_project, default_bot=default_bot, default_version=default_version)
