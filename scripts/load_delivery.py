from __future__ import annotations

import argparse
import json
import time
import urllib.request


def main() -> int:
    parser = argparse.ArgumentParser(description="Нагрузочный smoke для delivery enqueue.")
    parser.add_argument("--url", default="http://127.0.0.1:8088/delivery/enqueue")
    parser.add_argument("--token", required=True)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--adapter", default="fake")
    parser.add_argument("--target", default="fake-chat")
    args = parser.parse_args()
    failures = 0
    started = time.time()
    for index in range(args.count):
        payload = {"adapter": args.adapter, "target": args.target, "text": f"load message {index}"}
        request = urllib.request.Request(args.url, data=json.dumps(payload).encode("utf-8"), method="POST", headers={"Content-Type": "application/json", "Authorization": f"Bearer {args.token}"})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310 - operator tool
                failures += int(response.status >= 400)
        except Exception:
            failures += 1
    elapsed = max(0.001, time.time() - started)
    print(json.dumps({"count": args.count, "failures": failures, "rps": round(args.count / elapsed, 2)}, ensure_ascii=False))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
