from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_content_snapshot_from_path(path: str | Path) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except Exception:
        return {}


def collect_runtime_markers(bot: Any) -> dict[str, Any]:
    return {
        'started_at': getattr(bot, 'started_at', None),
        'content_path': str(getattr(bot.settings, 'discord_content_file_path', '')),
        'content_schema_required': int(getattr(bot.settings, 'content_schema_version_required', 0) or 0),
    }
