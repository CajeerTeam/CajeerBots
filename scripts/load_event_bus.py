from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

from core.config import Settings
from core.events import CajeerEvent
from core.runtime import Runtime


async def run(count: int) -> dict[str, object]:
    runtime = Runtime(Settings.from_env(), project_root=Path.cwd())
    started = time.time()
    for index in range(count):
        await runtime.event_bus.publish(CajeerEvent.create(source="load", type="load.event", payload={"index": index}))
    elapsed = max(0.001, time.time() - started)
    return {"count": count, "rps": round(count / elapsed, 2), "metrics": runtime.event_bus.metrics().to_dict()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Нагрузочный smoke для event bus backend.")
    parser.add_argument("--count", type=int, default=1000)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(args.count)), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
