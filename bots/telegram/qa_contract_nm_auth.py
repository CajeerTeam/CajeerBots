#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

REQUIRED_KEYS = [
    'SECURITY_STATUS_URL',
    'SECURITY_CHALLENGES_URL',
    'SECURITY_2FA_ACTION_URL',
    'SECURITY_RECOVERY_URL',
    'SECURITY_SESSIONS_URL',
    'SECURITY_SESSION_ACTION_URL',
]


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        values[key.strip()] = value.strip()
    return values


def main() -> int:
    env = _parse_env_file(Path('.env'))
    configured = [key for key in REQUIRED_KEYS if env.get(key, '').strip()]
    missing = []
    mode = 'disabled'
    if configured:
        mode = 'enabled'
        missing = [key for key in REQUIRED_KEYS if not env.get(key, '').strip()]
    print(json.dumps({
        'required_keys': REQUIRED_KEYS,
        'mode': mode,
        'configured': configured,
        'missing': missing,
    }, ensure_ascii=False, indent=2))
    return 0 if not missing else 1


if __name__ == '__main__':
    raise SystemExit(main())
