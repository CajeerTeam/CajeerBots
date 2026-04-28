from cajeer_bots.cli import main as _main

def main() -> int:
    return _main(["run", "telegram"])

if __name__ == "__main__":
    raise SystemExit(main())
