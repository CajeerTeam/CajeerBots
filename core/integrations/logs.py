from __future__ import annotations

import hashlib
import hmac
import logging
import time
from dataclasses import dataclass
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

    def _headers(self, body: bytes) -> dict[str, str]:
        headers = {"X-Log-Token": self.settings.token, "X-Log-Contract": LOGS_CONTRACT_VERSION}
        if self.settings.sign_requests:
            ts = str(int(time.time()))
            nonce = str(uuid4())
            digest = hashlib.sha256(body).hexdigest()
            canonical = f"{ts}\n{nonce}\n{digest}".encode("utf-8")
            headers.update(
                {
                    "X-Log-Timestamp": ts,
                    "X-Log-Nonce": nonce,
                    "X-Log-Body-SHA256": digest,
                    "X-Log-Signature": hmac.new(
                        self.settings.token.encode("utf-8"), canonical, hashlib.sha256
                    ).hexdigest(),
                }
            )
        return headers

    async def emit_event(self, event: CajeerEvent, *, level: str = "INFO") -> None:
        if not self.settings.enabled:
            return
        payload = {
            "contract": LOGS_CONTRACT_VERSION,
            "events": [
                {
                    "project": self.settings.project,
                    "bot": self.settings.bot,
                    "environment": self.settings.environment,
                    "instance_id": self.instance_id,
                    "level": level,
                    "source": event.source,
                    "event_type": event.type,
                    "event_id": event.event_id,
                    "trace_id": event.trace_id,
                    "payload": event.payload,
                    "created_at": event.created_at,
                }
            ],
        }
        try:
            await post_json(
                self.settings.url,
                payload,
                headers_factory=self._headers,
                timeout=self.settings.timeout_seconds,
            )
        except Exception as exc:  # pragma: no cover - сеть не должна ронять runtime
            logger.warning("не удалось отправить событие в Cajeer Logs: %s", exc)
