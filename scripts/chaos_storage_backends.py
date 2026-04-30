#!/usr/bin/env python3
from __future__ import annotations

import os
import sys


def main() -> int:
    """Operator-facing chaos preflight.

    Real Redis/PostgreSQL restart drills are run by orchestration, but this
    script makes the drill executable and explicit in CI/release gates.
    """
    checks = {
        "REDIS_URL": bool(os.getenv("REDIS_URL")),
        "DATABASE_ASYNC_URL": bool(os.getenv("DATABASE_ASYNC_URL")),
    }
    print({"storage_chaos_preflight": checks})
    if os.getenv("CHAOS_REQUIRE_EXTERNALS") == "true" and not all(checks.values()):
        print("CHAOS_REQUIRE_EXTERNALS=true требует REDIS_URL и DATABASE_ASYNC_URL", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
