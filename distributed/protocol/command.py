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
    lease_seconds: int = 30
    max_attempts: int = 3
    schema: str = SCHEMA
    command_id: str = field(default_factory=lambda: f"cmd_{uuid4().hex}")

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "RuntimeCommand":
        return cls(
            node_id=str(data["node_id"]),
            bot_id=str(data.get("bot_id") or ""),
            type=str(data["type"]),
            payload=dict(data.get("payload") or {}),
            event_id=str(data.get("event_id") or "") or None,
            timeout_seconds=int(data.get("timeout_seconds") or 10),
            lease_seconds=int(data.get("lease_seconds") or 30),
            max_attempts=int(data.get("max_attempts") or 3),
            schema=str(data.get("schema") or SCHEMA),
            command_id=str(data.get("command_id") or f"cmd_{uuid4().hex}"),
        )
