from __future__ import annotations

import re
from collections import Counter
from typing import Any


def _safe_metric_name(name: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_]+', '_', str(name or '').strip())


async def increment_runtime_metric(bot: Any, key: str, amount: int = 1) -> None:
    if not key:
        return
    key = _safe_metric_name(key)
    counters = getattr(bot, 'metrics_counters', None)
    if counters is None:
        counters = Counter()
        setattr(bot, 'metrics_counters', counters)
    counters[key] += int(amount)
    persistent = getattr(bot, 'persistent_metrics_counters', None)
    if persistent is None:
        persistent = {}
        setattr(bot, 'persistent_metrics_counters', persistent)
    persistent[key] = int(persistent.get(key, 0)) + int(amount)
    try:
        await bot.storage.database.set_key_value('runtime_metrics_counters', persistent)
    except Exception:
        pass


async def load_persistent_runtime_metrics(bot: Any) -> dict[str, int]:
    try:
        value = await bot.storage.database.get_key_value('runtime_metrics_counters')
    except Exception:
        value = None
    normalized: dict[str, int] = {}
    if isinstance(value, dict):
        for key, raw in value.items():
            try:
                normalized[_safe_metric_name(key)] = int(raw)
            except Exception:
                continue
    setattr(bot, 'persistent_metrics_counters', normalized)
    return normalized


def build_runtime_metrics_text(bot: Any) -> str:
    started_at = getattr(bot, 'started_at', None)
    started_at_unix = int(getattr(started_at, 'timestamp', lambda: 0)()) if started_at else 0
    lines = [
        '# HELP nmdiscordbot_runtime_up Whether the runtime is up.',
        '# TYPE nmdiscordbot_runtime_up gauge',
        'nmdiscordbot_runtime_up 1',
        '# HELP nmdiscordbot_started_at_unix Runtime start timestamp.',
        '# TYPE nmdiscordbot_started_at_unix gauge',
        f'nmdiscordbot_started_at_unix {started_at_unix}',
    ]
    merged: dict[str, int] = {}
    for source in [getattr(bot, 'persistent_metrics_counters', {}) or {}, getattr(bot, 'metrics_counters', {}) or {}]:
        for key, value in source.items():
            safe = _safe_metric_name(key)
            try:
                merged[safe] = max(int(value), int(merged.get(safe, 0)))
            except Exception:
                continue
    for key, value in sorted(merged.items()):
        lines.append(f'nmdiscordbot_counter{{name="{key}"}} {value}')
    destination_states = getattr(bot, 'bridge_destination_state_snapshot', {}) or {}
    for destination, payload in sorted(destination_states.items()):
        safe_dest = _safe_metric_name(destination)
        consecutive_failures = int(payload.get('consecutive_failures') or 0)
        circuit_state = str(payload.get('circuit_state') or 'closed').strip().lower()
        circuit_open = 1 if circuit_state == 'open' else 0
        lines.append(f'nmdiscordbot_bridge_destination_failures{{destination="{safe_dest}"}} {consecutive_failures}')
        lines.append(f'nmdiscordbot_bridge_destination_circuit_open{{destination="{safe_dest}"}} {circuit_open}')
