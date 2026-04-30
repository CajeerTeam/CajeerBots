from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from core.config import RemoteLogsSettings
from core.contracts import LOGS_CONTRACT_VERSION
from core.events import CajeerEvent
from core.integrations.http import post_json

logger = logging.getLogger(__name__)


@dataclass
class CajeerLogsClient:
    settings: RemoteLogsSettings
    instance_id: str
    buffer_dir: Path | None = None

    def _headers(self, body: bytes) -> dict[str, str]:
        headers = {"X-Log-Token": self.settings.token, "X-Log-Contract": LOGS_CONTRACT_VERSION}
        if self.settings.sign_requests:
            ts = str(int(time.time()))
            nonce = str(uuid4())
            digest = hashlib.sha256(body).hexdigest()
            canonical = f"{ts}\n{nonce}\n{digest}".encode("utf-8")
            headers.update({"X-Log-Timestamp": ts, "X-Log-Nonce": nonce, "X-Log-Body-SHA256": digest, "X-Log-Signature": hmac.new(self.settings.token.encode("utf-8"), canonical, hashlib.sha256).hexdigest()})
        return headers

    def _buffer_path(self) -> Path:
        root = self.buffer_dir or Path("runtime/logs-buffer")
        root.mkdir(parents=True, exist_ok=True)
        return root / f"{int(time.time())}-{uuid4()}.jsonl"

    def _buffer_payload(self, payload: dict[str, object], reason: str) -> None:
        try:
            item = {"reason": reason, "payload": payload, "created_at": int(time.time())}
            with self._buffer_path().open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")
        except Exception as exc:  # pragma: no cover
            logger.warning("не удалось записать буфер Cajeer Logs: %s", exc)

    async def flush_buffer(self) -> dict[str, int]:
        if not self.settings.enabled:
            return {"sent": 0, "failed": 0}
        root = self.buffer_dir or Path("runtime/logs-buffer")
        sent = failed = 0
        for path in sorted(root.glob("*.jsonl")) if root.exists() else []:
            remaining: list[str] = []
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                    payload = data.get("payload")
                    if isinstance(payload, dict):
                        await post_json(self.settings.url, payload, headers_factory=self._headers, timeout=self.settings.timeout_seconds)
                        sent += 1
                    else:
                        failed += 1
                except Exception:
                    remaining.append(line)
                    failed += 1
            if remaining:
                path.write_text("\n".join(remaining) + "\n", encoding="utf-8")
            else:
                path.unlink(missing_ok=True)
        return {"sent": sent, "failed": failed, "buffered": failed}

    async def emit_event(self, event: CajeerEvent, *, level: str = "INFO") -> None:
        if not self.settings.enabled:
            return
        payload = {"contract": LOGS_CONTRACT_VERSION, "events": [{"project": self.settings.project, "bot": self.settings.bot, "environment": self.settings.environment, "instance_id": self.instance_id, "level": level, "source": event.source, "event_type": event.type, "event_id": event.event_id, "trace_id": event.trace_id, "payload": event.payload, "created_at": event.created_at}]}
        try:
            await post_json(self.settings.url, payload, headers_factory=self._headers, timeout=self.settings.timeout_seconds)
        except Exception as exc:  # pragma: no cover - сеть не должна ронять runtime
            self._buffer_payload(payload, str(exc))
            logger.warning("не удалось отправить событие в Cajeer Logs; событие сохранено в буфер: %s", exc)
