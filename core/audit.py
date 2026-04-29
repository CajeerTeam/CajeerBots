from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from uuid import uuid4


@dataclass(frozen=True)
class AuditRecord:
    audit_id: str
    actor_type: str
    actor_id: str
    action: str
    resource: str
    result: str
    trace_id: str | None
    ip: str | None
    user_agent: str | None
    message: str
    created_at: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class AuditLog:
    def __init__(self, max_size: int = 2000) -> None:
        self._records: deque[AuditRecord] = deque(maxlen=max_size)

    def write(
        self,
        *,
        actor_type: str,
        actor_id: str,
        action: str,
        resource: str,
        result: str = "ok",
        trace_id: str | None = None,
        ip: str | None = None,
        user_agent: str | None = None,
        message: str = "",
    ) -> AuditRecord:
        record = AuditRecord(
            audit_id=str(uuid4()),
            actor_type=actor_type,
            actor_id=actor_id,
            action=action,
            resource=resource,
            result=result,
            trace_id=trace_id,
            ip=ip,
            user_agent=user_agent,
            message=message,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        self._records.append(record)
        return record

    def snapshot(self) -> list[AuditRecord]:
        return list(self._records)
