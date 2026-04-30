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


class RedisWebhookReplayGuard:
    def __init__(self, redis_url: str, ttl_seconds: int = 300, key_prefix: str = "cajeer:bots:webhook-replay") -> None:
        if not redis_url:
            raise RuntimeError("WEBHOOK_REPLAY_CACHE=redis требует REDIS_URL")
        try:
            from redis import Redis
        except ImportError as exc:  # pragma: no cover - depends on optional extra
            raise RuntimeError("WEBHOOK_REPLAY_CACHE=redis требует пакет redis") from exc
        self.ttl_seconds = ttl_seconds
        self.key_prefix = key_prefix.rstrip(":")
        self.client = Redis.from_url(redis_url)

    def check_and_mark(self, key: str) -> bool:
        redis_key = f"{self.key_prefix}:{key}"
        return bool(self.client.set(redis_key, "1", ex=self.ttl_seconds, nx=True))


def _header(headers: Mapping[str, str], name: str) -> str:
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return str(value)
    return ""


def body_digest(raw_body: bytes) -> str:
    return sha256(raw_body).hexdigest()


def replay_key(provider: str, headers: Mapping[str, str], raw_body: bytes) -> str:
    update_id = _header(headers, "x-telegram-bot-api-secret-token") if provider == "telegram" else ""
    nonce = _header(headers, "x-cajeer-nonce") or _header(headers, "x-request-id")
    timestamp = _header(headers, "x-cajeer-timestamp") or _header(headers, "x-telegram-bot-api-timestamp")
    basis = f"{provider}:{timestamp}:{nonce}:{update_id}:{body_digest(raw_body)}"
    return sha256(basis.encode("utf-8")).hexdigest()


def timestamp_valid(headers: Mapping[str, str], ttl_seconds: int) -> bool:
    value = _header(headers, "x-cajeer-timestamp") or _header(headers, "x-telegram-bot-api-timestamp")
    if not value:
        return False
    try:
        timestamp = float(value)
    except ValueError:
        return False
    return abs(time.time() - timestamp) <= ttl_seconds


def verify_optional_hmac(
    secret: str,
    headers: Mapping[str, str],
    raw_body: bytes,
    *,
    required: bool = False,
    timestamp_required: bool = False,
    timestamp_ttl_seconds: int = 300,
) -> bool:
    """Проверить дополнительную подпись X-Cajeer-Signature.

    Формат подписи: sha256=<hex>. По умолчанию подпись остаётся optional для
    совместимости с native Telegram/VK webhook-проверками. В production можно
    включить строгий режим через WEBHOOK_HMAC_REQUIRED=true и
    WEBHOOK_TIMESTAMP_REQUIRED=true, если входящие webhooks подписывает reverse
    proxy или внешний gateway.
    """
    signature = _header(headers, "x-cajeer-signature")
    if timestamp_required and not timestamp_valid(headers, timestamp_ttl_seconds):
        return False
    if not signature:
        return not required
    if not secret:
        return False
    expected = "sha256=" + hmac.new(secret.encode("utf-8"), raw_body, sha256).hexdigest()
    return hmac.compare_digest(signature, expected)
