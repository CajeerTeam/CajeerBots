from __future__ import annotations

from core.catalog_signing import sign_catalog_payload, verify_catalog_signature


def test_catalog_hmac_signature_roundtrip():
    sha = "a" * 64
    signature = sign_catalog_payload(sha, "secret")
    ok, message = verify_catalog_signature(sha, signature, "secret", required=True)
    assert ok is True
    assert message == "ok"

    ok, message = verify_catalog_signature(sha, signature, "other-secret", required=True)
    assert ok is False
    assert "не совпадает" in message
