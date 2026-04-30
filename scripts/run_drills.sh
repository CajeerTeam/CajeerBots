#!/usr/bin/env bash
set -euo pipefail
PYTHON_BIN="${PYTHON_BIN:-python3}"
PYTHON_FLAGS="${PYTHON_FLAGS:--S}"
PY_CMD=("${PYTHON_BIN}")
if [ -n "${PYTHON_FLAGS}" ]; then
  # shellcheck disable=SC2206
  PY_CMD+=( ${PYTHON_FLAGS} )
fi
"${PY_CMD[@]}" -m core.release_checklist --file "${1:-release/checklist.yaml}"
