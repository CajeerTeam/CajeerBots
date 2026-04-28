from __future__ import annotations

from typing import Any


def legacy_review_summary(rows: list[dict[str, Any]] | None = None) -> list[str]:
    rows = rows or []
    lines: list[str] = []
    for row in rows[:20]:
        resource_type = str(row.get('resource_type') or 'resource')
        name = str(row.get('resource_name') or row.get('discord_id') or 'unknown')
        status = str(row.get('status') or 'legacy')
        delete_after = str(row.get('delete_after') or '')
        lines.append(f"{resource_type}: {name} [{status}] удалить после {delete_after or '—'}")
    return lines
