from pathlib import Path


def test_systemd_unit_is_hardened():
    unit = Path("ops/systemd/cajeer-bots.service").read_text(encoding="utf-8")
    for directive in [
        "NoNewPrivileges=true",
        "PrivateTmp=true",
        "ProtectSystem=strict",
        "ProtectHome=true",
        "CapabilityBoundingSet=",
        "RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX",
    ]:
        assert directive in unit


def test_nginx_proxy_has_webhook_rate_limit_and_metrics_allowlist():
    nginx = Path("ops/nginx/cajeer-bots-api.conf").read_text(encoding="utf-8")
    assert "limit_req_zone" in nginx
    assert "location /webhooks/" in nginx
    assert "location /metrics" in nginx
    assert "deny all" in nginx
    assert "X-Forwarded-For" in nginx
