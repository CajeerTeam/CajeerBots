from __future__ import annotations

import json
import os
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path


@dataclass
class DrillResult:
    id: str
    command: str
    returncode: int
    output: str = ""


@dataclass
class ChecklistRun:
    ok: bool
    checklist: str
    drills: list[DrillResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["drills"] = [asdict(item) for item in self.drills]
        return data


DEFAULT_DRILLS = [
    ("version", "python3 -S -m core.versioning"),
    ("local_memory", "bash scripts/fault_drill.sh"),
    ("syntax", "python3 -S scripts/check_syntax.py"),
    ("architecture", "python3 -S scripts/check_architecture.py"),
    ("docs", "bash scripts/check_docs.sh"),
    ("secrets", "bash scripts/check_secrets.sh"),
]


def _strip_quotes(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _extract_drills(path: Path) -> list[tuple[str, str]]:
    """Parse the strict YAML subset used by ``release/checklist.yaml``.

    The parser intentionally supports only ``drill_commands`` entries with
    ``id`` and ``command`` keys. This keeps the release gate dependency-free but
    prevents accidental acceptance of malformed arbitrary YAML.
    """
    if not path.exists():
        return DEFAULT_DRILLS

    drills: list[tuple[str, str]] = []
    in_drills = False
    current: dict[str, str] = {}

    def flush() -> None:
        nonlocal current
        if not current:
            return
        if "id" not in current or "command" not in current:
            raise ValueError(f"drill_commands item должен содержать id и command: {current}")
        drills.append((current["id"], current["command"]))
        current = {}

    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        if raw.startswith("drill_commands:"):
            flush()
            in_drills = True
            continue
        if in_drills and raw and not raw.startswith(" ") and not raw.startswith("-"):
            flush()
            in_drills = False
        if not in_drills:
            continue
        stripped = raw.strip()
        if stripped.startswith("- "):
            flush()
            stripped = stripped[2:].strip()
            if not stripped:
                continue
        if ":" not in stripped:
            raise ValueError(f"{path}:{line_no}: неверная строка drill_commands: {raw}")
        key, value = stripped.split(":", 1)
        key = key.strip()
        value = _strip_quotes(value)
        if key not in {"id", "command"}:
            raise ValueError(f"{path}:{line_no}: неизвестное поле drill_commands.{key}")
        current[key] = value
    flush()
    return drills or DEFAULT_DRILLS


def run_release_drills(checklist: str | Path = "release/checklist.yaml", *, env: dict[str, str] | None = None) -> ChecklistRun:
    path = Path(checklist)
    run_env = os.environ.copy()
    run_env.update(env or {})
    run_env.setdefault("CAJEER_BOTS_ENV", "test")
    run_env.setdefault("EVENT_SIGNING_SECRET", "drill-secret")
    run_env.setdefault("API_TOKEN", "drill-token")
    run_env.setdefault("API_TOKEN_READONLY", "drill-readonly")
    run_env.setdefault("API_TOKEN_METRICS", "drill-metrics")
    run_env.setdefault("TELEGRAM_ENABLED", "false")
    run_env.setdefault("DISCORD_ENABLED", "false")
    run_env.setdefault("VKONTAKTE_ENABLED", "false")
    run_env.setdefault("FAKE_ENABLED", "true")
    results: list[DrillResult] = []
    try:
        drills = _extract_drills(path)
    except Exception as exc:
        return ChecklistRun(False, str(path), [DrillResult("parse", f"parse {path}", 1, str(exc))])
    for drill_id, command in drills:
        completed = subprocess.run(command, shell=True, env=run_env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
        results.append(DrillResult(drill_id, command, completed.returncode, completed.stdout[-4000:]))
    return ChecklistRun(ok=all(item.returncode == 0 for item in results), checklist=str(path), drills=results)


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Запустить исполняемые release drill-тесты.")
    parser.add_argument("--file", default="release/checklist.yaml")
    args = parser.parse_args(argv)
    result = run_release_drills(args.file)
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
