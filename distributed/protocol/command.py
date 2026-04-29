from __future__ import annotations

from dataclasses import asdict, dataclass, field
from uuid import uuid4

SCHEMA = "cajeer.bots.command.v1"


@dataclass(frozen=True)
class RuntimeCommand:
    node_id: str
    bot_id: str
    type: str
    payload: dict[str, object] = field(default_factory=dict)
    event_id: str | None = None
    timeout_seconds: int = 10
    schema: str = SCHEMA
    command_id: str = field(default_factory=lambda: f"cmd_{uuid4().hex}")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)
