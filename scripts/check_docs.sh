#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"
"$PYTHON_BIN" -S - <<'PY'
from __future__ import annotations
import re
import sys
from pathlib import Path

root = Path.cwd()
errors: list[str] = []
required = [root / "README.md", root / "wiki" / "Home.md"]
for path in required:
    if not path.exists() or not path.read_text(encoding="utf-8").strip():
        errors.append(f"обязательный документ пустой или отсутствует: {path.relative_to(root)}")

for path in [root / "README.md", *sorted((root / "wiki").glob("*.md"))]:
    text = path.read_text(encoding="utf-8")
    for match in re.finditer(r"\]\(([^)]+)\)", text):
        target = match.group(1).strip()
        if "://" in target or target.startswith("#") or target.startswith("mailto:"):
            continue
        raw_target = target.split("#", 1)[0]
        local = (path.parent / raw_target).resolve()
        if raw_target and not local.exists() and not (path.parent / (raw_target + ".md")).exists():
            errors.append(f"битая ссылка в {path.relative_to(root)}: {target}")
    for token in (".env.production.example", "wiki/1.0-Readiness.md"):
        if token in text:
            errors.append(f"устаревшая ссылка {token} в {path.relative_to(root)}")

home = root / "wiki" / "Home.md"
if home.exists():
    home_text = home.read_text(encoding="utf-8")
    for page in sorted((root / "wiki").glob("*.md")):
        if page.name in {"Home.md", "README.md"}:
            continue
        if page.name not in home_text and page.stem not in home_text:
            errors.append(f"страница Wiki не достижима из Home.md: {page.name}")

if errors:
    print("Проверка документации: есть проблемы", file=sys.stderr)
    for item in errors:
        print(f"- {item}", file=sys.stderr)
    raise SystemExit(1)
print("Проверка документации: ok")
PY
