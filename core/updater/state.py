from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Literal

UpdateStage = Literal[
    "idle",
    "checked",
    "downloaded",
    "verified",
    "staged",
    "preflight_failed",
    "ready_to_apply",
    "applying",
    "applied",
    "apply_failed",
    "rolling_back",
    "rollback_completed",
    "rollback_failed",
]


@dataclass
class UpdateState:
    stage: UpdateStage = "idle"
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    current_version: str | None = None
    target_version: str | None = None
    artifact: str | None = None
    staged_path: str | None = None
    error: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
