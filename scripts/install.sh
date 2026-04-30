#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"
INSTALL_EXTRAS="${INSTALL_EXTRAS:-api,adapters,redis}"

"${PYTHON_BIN}" - <<'PY'
import sys
version = sys.version_info[:2]
if not ((3, 11) <= version < (3, 13)):
    raise SystemExit(
        f"Cajeer Bots требует Python >=3.11,<3.13; найден {sys.version.split()[0]}. "
        "Укажите PYTHON_BIN=python3.12 или PYTHON_BIN=python3.11."
    )
PY

"${PYTHON_BIN}" -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[${INSTALL_EXTRAS}]"
[ -f .env ] || cp .env.example .env
python -m core fix-permissions >/dev/null || true
echo "Cajeer Bots установлен. Перед боевым запуском заполните .env."
