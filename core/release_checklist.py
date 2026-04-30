from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, asdict, field
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
    ("local_memory", "bash scripts/fault_drill.sh"),
    ("syntax", "python3 -S scripts/check_syntax.py"),
    ("architecture", "python3 -S scripts/check_architecture.py"),
    ("docs", "bash scripts/check_docs.sh"),
    ("secrets", "bash scripts/check_secrets.sh"),
]


def _extract_drills(path: Path) -> list[tuple[str, str]]:
    if not path.exists():
        return DEFAULT_DRILLS
    drills: list[tuple[str, str]] = []
    current_id = ""
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("- id:"):
            current_id = line.split(":", 1)[1].strip().strip('"\'')
        elif line.startswith("command:") and current_id:
            command = line.split(":", 1)[1].strip().strip('"\'')
            drills.append((current_id, command))
            current_id = ""
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
    for drill_id, command in _extract_drills(path):
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
