from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass
class NodeRegistry:
    ttl_seconds: int = 60
    nodes: dict[str, dict[str, object]] = field(default_factory=dict)

    def register(self, node_id: str, payload: dict[str, object]) -> None:
        self.nodes[node_id] = {**payload, "node_id": node_id, "last_seen_at": now_iso()}

    def heartbeat(self, node_id: str, payload: dict[str, object] | None = None) -> dict[str, object]:
        current = self.nodes.get(node_id, {"node_id": node_id})
        current.update(payload or {})
        current["last_seen_at"] = now_iso()
        current["status"] = "online"
        self.nodes[node_id] = current
        return dict(current)

    def expire(self) -> list[str]:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=self.ttl_seconds)
        expired: list[str] = []
        for node_id, payload in list(self.nodes.items()):
            last_seen = str(payload.get("last_seen_at") or "1970-01-01T00:00:00+00:00")
            try:
                if _parse(last_seen) < cutoff:
                    payload["status"] = "expired"
                    expired.append(node_id)
            except ValueError:
                payload["status"] = "expired"
                expired.append(node_id)
        return expired

    def snapshot(self) -> dict[str, dict[str, object]]:
        self.expire()
        return {key: dict(value) for key, value in self.nodes.items()}
