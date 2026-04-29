#!/usr/bin/env bash
set -euo pipefail
[ ! -d .venv ] || . .venv/bin/activate
exec ${PYTHON_BIN:-python3} -m core doctor "$@"
