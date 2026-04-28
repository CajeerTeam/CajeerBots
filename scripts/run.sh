#!/usr/bin/env bash
set -euo pipefail
MODE="${1:-all}"
[ ! -d .venv ] || . .venv/bin/activate
exec python -m core run "$MODE"
