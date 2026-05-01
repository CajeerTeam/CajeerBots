from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from uuid import uuid4


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class CommandQueue:
    lease_seconds: int = 30
    queues: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    leased: dict[str, dict[str, object]] = field(default_factory=dict)

    def push(self, node_id: str, command: dict[str, object]) -> str:
        command_id = str(command.get("command_id") or f"cmd_{uuid4().hex}")
        item = {**command, "command_id": command_id, "node_id": node_id, "status": "queued"}
        self.queues.setdefault(node_id, []).append(item)
        return command_id

    def claim(self, node_id: str, limit: int = 10) -> list[dict[str, object]]:
        self.requeue_expired()
        items = self.queues.setdefault(node_id, [])[:limit]
        self.queues[node_id] = self.queues[node_id][limit:]
        expires_at = (_now() + timedelta(seconds=self.lease_seconds)).isoformat()
        for item in items:
            item["status"] = "leased"
            item["lease_expires_at"] = expires_at
            self.leased[str(item["command_id"])] = item
        return [dict(item) for item in items]

    def ack(self, command_id: str) -> bool:
        return self.leased.pop(command_id, None) is not None

    def nack(self, command_id: str, *, error: str = "", retry: bool = True) -> bool:
        item = self.leased.pop(command_id, None)
        if item is None:
            return False
        item["last_error"] = error
        if retry:
            item["status"] = "queued"
            self.queues.setdefault(str(item["node_id"]), []).append(item)
        else:
            item["status"] = "failed"
        return True

    def requeue_expired(self) -> int:
        now = _now()
        count = 0
        for command_id, item in list(self.leased.items()):
            expires = datetime.fromisoformat(str(item.get("lease_expires_at")))
            if expires <= now:
                self.leased.pop(command_id, None)
                item["status"] = "queued"
                self.queues.setdefault(str(item["node_id"]), []).append(item)
                count += 1
        return count

    def pop_all(self, node_id: str) -> list[dict[str, object]]:
        return self.claim(node_id, limit=1000)
