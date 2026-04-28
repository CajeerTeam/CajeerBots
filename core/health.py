import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

@dataclass(frozen=True)
class HealthStatus:
    status: str
    service: str
    version: str
    checked_at: str
    details: dict[str, str]

def build_health(version: str, details: dict[str, str] | None = None) -> str:
    return json.dumps(asdict(HealthStatus("ok", "cajeer-bots", version, datetime.now(timezone.utc).isoformat(), details or {})), ensure_ascii=False)
