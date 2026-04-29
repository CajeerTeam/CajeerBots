from __future__ import annotations

import hashlib
import hmac
import json


def sign_payload(payload: dict[str, object], secret: str) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()


def verify_payload(payload: dict[str, object], secret: str, signature: str) -> bool:
    return hmac.compare_digest(sign_payload(payload, secret), signature)
