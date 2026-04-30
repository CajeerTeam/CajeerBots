#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from datetime import datetime, timedelta, timezone

from core.delivery import DeliveryService
from core.event_bus import InMemoryEventBus
from core.events import CajeerEvent


async def main() -> int:
    delivery = DeliveryService(retry_backoff_seconds=0, lease_seconds=1)
    task = await delivery.enqueue_async("fake", "chat", "message", max_attempts=3)
    first = await delivery.claim("fake", consumer="worker-crashed")
    if not first or first[0].delivery_id != task.delivery_id:
        print("delivery claim failed")
        return 1

    # worker died without ack/sent; lease expires and another worker reclaims.
    first[0].locked_at = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    reclaimed = await delivery.claim("fake", consumer="worker-recovered")
    if not reclaimed or reclaimed[0].locked_by != "worker-recovered":
        print("delivery lease reclaim failed")
        return 1

    await delivery.mark_failed(task.delivery_id, "temporary", retry=True)
    retry = await delivery.claim("fake", consumer="worker-retry")
    if not retry:
        print("delivery retry claim failed")
        return 1

    bus = InMemoryEventBus()
    event = CajeerEvent.create(source="chaos", type="chaos.worker_crash", payload={})
    await bus.publish(event)
    claimed = await bus.claim(limit=1, consumer="worker-crashed")
    await bus.nack(claimed[0], "crash", retry=True)
    claimed_again = await bus.claim(limit=1, consumer="worker-recovered")
    if not claimed_again or claimed_again[0].event.event_id != event.event_id:
        print("event retry after nack failed")
        return 1
    await bus.ack(claimed_again[0])

    print("Chaos drill: worker crash / lease reclaim / retry ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
