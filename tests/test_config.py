from core.config import Settings


def test_safe_summary_hides_secrets(monkeypatch):
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "secret")
    monkeypatch.setenv("API_TOKEN", "token")
    settings = Settings.from_env()
    summary = settings.safe_summary()
    assert summary["event_signing_secret_configured"] is True
    assert summary["api_token_configured"] is True
    assert "secret" not in str(summary)
    assert "token" not in str(summary)
