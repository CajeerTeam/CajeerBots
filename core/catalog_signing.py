from __future__ import annotations

import hashlib
import hmac
import os


SIGNATURE_PREFIX = "hmac-sha256:"


def sign_catalog_payload(payload_sha256: str, secret: str | None = None) -> str:
    secret = secret if secret is not None else os.getenv("PLUGIN_CATALOG_SIGNING_SECRET", "")
    if not secret:
        return ""
    digest = hmac.new(secret.encode("utf-8"), payload_sha256.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{SIGNATURE_PREFIX}{digest}"


def verify_catalog_signature(payload_sha256: str, signature: str, secret: str | None = None, *, required: bool = False) -> tuple[bool, str]:
    secret = secret if secret is not None else os.getenv("PLUGIN_CATALOG_SIGNING_SECRET", "")
    if not signature:
        return (not required, "signature отсутствует" if required else "signature не задана")
    if not secret:
        return (not required, "PLUGIN_CATALOG_SIGNING_SECRET не задан" if required else "signature пропущена без secret")
    expected = sign_catalog_payload(payload_sha256, secret)
    ok = hmac.compare_digest(expected, signature)
    return ok, "ok" if ok else "signature не совпадает"
