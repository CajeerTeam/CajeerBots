#!/usr/bin/env python3
from __future__ import annotations

import sys
from sqlite_export_bundle import main as export_main

if __name__ == '__main__':
    print('[WARN] postgres_sync.py is kept for backward compatibility only. Use sqlite_export_bundle.py for honest naming.', file=sys.stderr)
    raise SystemExit(export_main())
