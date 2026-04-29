from __future__ import annotations

import hashlib
import hmac
import logging
import time
from dataclasses import dataclass
from uuid import uuid4

from core.config import RemoteLogsSettings
from core.events import CajeerEvent
from core.integrations.http import post_json

logger = logging.getLogger(__name__)


@dataclass
class CajeerLogsClient:
    settings: RemoteLogsSettings
    instance_id: str

    def _headers(self, body: bytes) -> dict[str, str]:
        headers = {"X-Log-Token": self.settings.token}
        if self.settings.sign_requests:
            ts = str(int(time.time()))
            nonce = str(uuid4())
            signature_payload = b"\n".join([ts.encode(), nonce.encode(), body])
            headers.update(
                {
                    "X-Log-Timestamp": ts,
                    "X-Log-Nonce": nonce,
                    "X-Log-Signature": hmac.new(self.settings.token.encode(), signature_payload, hashlib.sha256).hexdigest(),
                }
            )
        return headers

    async def emit_event(self, event: CajeerEvent, *, level: str = "INFO") -> None:
        if not self.settings.enabled:
            return
        payload = {
            "events": [
                {
                    "project": self.settings.project,
                    "bot": self.settings.bot,
                    "environment": self.settings.environment,
                    "level": level,
                    "message": f"{event.source}:{event.type}",
                    "trace_id": event.trace_id,
                    "context": {"instance_id": self.instance_id, "event": event.to_dict()},
                }
            ]
        }
        import json

        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        try:
            await post_json(self.settings.url, payload, self._headers(body), timeout=self.settings.timeout_seconds)
        except Exception as exc:  # noqa: BLE001
            logger.warning("не удалось отправить событие в Cajeer Logs: %s", exc)
