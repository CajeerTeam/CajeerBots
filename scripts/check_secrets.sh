#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"
"$PYTHON_BIN" -S - <<'PY'
from __future__ import annotations
import re
import sys
from pathlib import Path

root = Path.cwd()
ignored_dirs = {".git", "dist", "runtime", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".venv"}
allow_files = {".env.example"}
patterns = {
    "telegram-token": re.compile(r"\b\d{6,12}:[A-Za-z0-9_-]{30,}\b"),
    "discord-token": re.compile(r"\b[MNO][A-Za-z\d_-]{23,}\.[A-Za-z\d_-]{6,}\.[A-Za-z\d_-]{27,}\b"),
    "private-key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
}
errors: list[str] = []
for path in root.rglob("*"):
    if not path.is_file() or any(part in ignored_dirs for part in path.parts):
        continue
    if path.name == ".env":
        errors.append(f".env не должен лежать в исходниках/артефакте: {path.relative_to(root)}")
        continue
    if path.suffix.lower() not in {".py", ".sh", ".md", ".json", ".yaml", ".yml", ".toml", ".ini", ".example", ""} and path.name not in {"Dockerfile", "Makefile"}:
        continue
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        continue
    if path.name in allow_files:
        continue
    for name, pattern in patterns.items():
        if pattern.search(text):
            errors.append(f"возможный секрет ({name}) в {path.relative_to(root)}")
if errors:
    print("Проверка секретов: есть проблемы", file=sys.stderr)
    for item in errors:
        print(f"- {item}", file=sys.stderr)
    raise SystemExit(1)
print("Проверка секретов: ok")
PY
