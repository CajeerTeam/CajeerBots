#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"
PYTHON_FLAGS="${PYTHON_FLAGS:--S}"
PY_CMD=("${PYTHON_BIN}")
if [ -n "${PYTHON_FLAGS}" ]; then
  # shellcheck disable=SC2206
  PY_CMD+=( ${PYTHON_FLAGS} )
fi

echo "Drill: local-memory self-test"
EVENT_SIGNING_SECRET="${EVENT_SIGNING_SECRET:-drill-secret}" API_TOKEN="${API_TOKEN:-drill-token}"   "${PY_CMD[@]}" -m core self-test --profile local-memory --offline

echo "Drill: worker crash / lease reclaim / retry"\n"${PY_CMD[@]}" scripts/chaos_worker_crash.py\n\necho "Drill: storage backend chaos preflight"\n"${PY_CMD[@]}" scripts/chaos_storage_backends.py\n\necho "Drill: release hygiene"\n"${PY_CMD[@]}" scripts/check_architecture.py
./scripts/check_docs.sh
./scripts/check_secrets.sh

echo "Fault drill завершён. Для Redis/PostgreSQL chaos используйте docker compose --profile integration и Runbook-Disaster-Recovery."
