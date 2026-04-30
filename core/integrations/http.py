from __future__ import annotations

import asyncio
import json
import urllib.request
from collections.abc import Callable, Mapping

HeadersFactory = Callable[[bytes], Mapping[str, str]]


async def post_json(url: str, payload: dict[str, object], headers: Mapping[str, str] | None = None, timeout: int = 5, headers_factory: HeadersFactory | None = None) -> dict[str, object]:
    def send() -> dict[str, object]:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(url, data=body, method="POST")
        request.add_header("Content-Type", "application/json")
        merged = dict(headers or {})
        if headers_factory is not None:
            merged.update(headers_factory(body))
        for key, value in merged.items():
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
