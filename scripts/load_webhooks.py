from __future__ import annotations

import argparse
import json
import time
import urllib.request


def post_json(url: str, payload: dict[str, object], token: str = "") -> int:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310 - operator tool
        return int(response.status)


def main() -> int:
    parser = argparse.ArgumentParser(description="Нагрузочный smoke для webhook/API JSON endpoints.")
    parser.add_argument("--url", default="http://127.0.0.1:8088/events/publish")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--token", default="")
    args = parser.parse_args()
    started = time.time()
    failures = 0
    for index in range(args.count):
        status = post_json(args.url, {"source": "load", "type": "load.event", "payload": {"index": index}}, args.token)
        if status >= 400:
            failures += 1
    elapsed = max(0.001, time.time() - started)
    print(json.dumps({"count": args.count, "failures": failures, "rps": round(args.count / elapsed, 2)}, ensure_ascii=False))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
