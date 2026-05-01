from pathlib import Path


def test_webhook_profile_examples_are_split():
    direct = Path("configs/env/.env.production.direct-webhook.example").read_text(encoding="utf-8")
    gateway = Path("configs/env/.env.production.gateway-signed-webhook.example").read_text(encoding="utf-8")
    assert "WEBHOOK_PROFILE=direct" in direct
    assert "WEBHOOK_HMAC_REQUIRED=false" in direct
    assert "WEBHOOK_TIMESTAMP_REQUIRED=false" in direct
    assert "WEBHOOK_PROFILE=gateway-signed" in gateway
    assert "WEBHOOK_HMAC_REQUIRED=true" in gateway
    assert "WEBHOOK_TIMESTAMP_REQUIRED=true" in gateway
