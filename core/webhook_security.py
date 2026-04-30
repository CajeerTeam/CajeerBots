from __future__ import annotations

import hmac
import time
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Mapping


@dataclass
class WebhookReplayGuard:
    ttl_seconds: int = 300
    _seen: dict[str, float] = field(default_factory=dict)

    def _cleanup(self, now: float) -> None:
        expired = [key for key, ts in self._seen.items() if now - ts > self.ttl_seconds]
        for key in expired:
            self._seen.pop(key, None)

    def check_and_mark(self, key: str) -> bool:
        now = time.time()
        self._cleanup(now)
        if key in self._seen:
            return False
        self._seen[key] = now
        return True


def body_digest(raw_body: bytes) -> str:
    return sha256(raw_body).hexdigest()


def replay_key(provider: str, headers: Mapping[str, str], raw_body: bytes) -> str:
    update_id = headers.get("x-telegram-bot-api-secret-token", "") if provider == "telegram" else ""
    nonce = headers.get("x-cajeer-nonce") or headers.get("x-request-id") or ""
    timestamp = headers.get("x-cajeer-timestamp") or headers.get("x-telegram-bot-api-timestamp") or ""
    basis = f"{provider}:{timestamp}:{nonce}:{update_id}:{body_digest(raw_body)}"
    return sha256(basis.encode("utf-8")).hexdigest()


def verify_optional_hmac(secret: str, headers: Mapping[str, str], raw_body: bytes) -> bool:
    """Проверить дополнительную подпись X-Cajeer-Signature, если она присутствует.

    Формат заголовка: sha256=<hex>. Отсутствие заголовка не запрещает нативные
    Telegram/VK проверки, но production doctor требует replay guard.
    """
    signature = headers.get("x-cajeer-signature", "")
    if not signature:
        return True
    if not secret:
        return False
    expected = "sha256=" + hmac.new(secret.encode("utf-8"), raw_body, sha256).hexdigest()
    return hmac.compare_digest(signature, expected)
