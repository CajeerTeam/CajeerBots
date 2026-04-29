#!/usr/bin/env bash
set -euo pipefail
TARGET="${1:-}"
[ ! -d .venv ] || . .venv/bin/activate
if [ -n "$TARGET" ]; then
  exec ${PYTHON_BIN:-python3} -m core run "$TARGET"
fi
exec ${PYTHON_BIN:-python3} -m core run
