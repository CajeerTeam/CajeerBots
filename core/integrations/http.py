from __future__ import annotations

import asyncio
import json
import urllib.request
from typing import Mapping


async def post_json(url: str, payload: dict[str, object], headers: Mapping[str, str], timeout: int = 5) -> dict[str, object]:
    def send() -> dict[str, object]:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(url, data=body, method="POST")
        request.add_header("Content-Type", "application/json")
        for key, value in headers.items():
            request.add_header(key, value)
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - URL задаётся администратором
            raw = response.read().decode("utf-8")
            if not raw:
                return {"ok": True, "status": response.status}
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
            return {"ok": True, "data": data}

    return await asyncio.to_thread(send)
