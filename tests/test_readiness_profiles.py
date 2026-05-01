from __future__ import annotations


def test_development_readiness_reports_placeholder_as_warning(monkeypatch, tmp_path):
    monkeypatch.setenv("CAJEER_BOTS_ENV", "development")
    monkeypatch.setenv("API_TOKEN", "change-me")
    monkeypatch.setenv("EVENT_SIGNING_SECRET", "change-me")
    from core.config import Settings
    from core.runtime import Runtime
    runtime = Runtime(Settings.from_env(), project_root=tmp_path)
    data = runtime.readiness_snapshot()
    assert any("placeholder" in item for item in data.get("warnings", []))
