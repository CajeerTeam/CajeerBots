#!/usr/bin/env python3
from pathlib import Path

def main() -> int:
    root = Path(__file__).resolve().parent
    env = root / ".env"
    if not env.exists():
        env.write_text((root / ".env.example").read_text(encoding="utf-8"), encoding="utf-8")
        print("Created .env from .env.example")
    print("Cajeer Bots setup wizard completed")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
