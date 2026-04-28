#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from nmbot.config import load_config


def export_bundle(sqlite_path: Path, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(sqlite_path) as connection:
        connection.row_factory = sqlite3.Row
        tables = [row['name'] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()]
        payload = {'sqlite_path': str(sqlite_path), 'format': 'sqlite-export-bundle', 'tables': {}}
        for table in tables:
            rows = [dict(row) for row in connection.execute(f'SELECT * FROM {table}').fetchall()]
            payload['tables'][table] = rows
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description='Export SQLite tables into a transport bundle for external migrations/integrations')
    parser.add_argument('--sqlite', default='')
    parser.add_argument('--output', default='')
    args = parser.parse_args()
    cfg = load_config()
    sqlite_path = Path(args.sqlite) if args.sqlite else cfg.sqlite_path
    output_path = Path(args.output) if args.output else cfg.artifact_root / 'exports' / 'sqlite-export-bundle.json'
    out = export_bundle(sqlite_path, output_path)
    print(out)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
