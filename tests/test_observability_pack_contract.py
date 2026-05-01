import json
from pathlib import Path


def test_observability_pack_files_exist():
    alerts = Path("ops/prometheus/cajeer-bots-alerts.yml")
    dashboard = Path("ops/grafana/cajeer-bots-dashboard.json")
    assert alerts.exists()
    assert dashboard.exists()
    assert "CajeerBotsDeadLettersGrowing" in alerts.read_text(encoding="utf-8")
    data = json.loads(dashboard.read_text(encoding="utf-8"))
    assert data["title"] == "Cajeer Bots"
    assert data["panels"]
