#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"

echo "Drill: local-memory self-test"
EVENT_SIGNING_SECRET="${EVENT_SIGNING_SECRET:-drill-secret}" API_TOKEN="${API_TOKEN:-drill-token}"   "$PYTHON_BIN" -m core self-test --profile local-memory --offline

echo "Drill: release hygiene"
"$PYTHON_BIN" scripts/check_architecture.py
./scripts/check_docs.sh
./scripts/check_secrets.sh

echo "Fault drill завершён. Для Redis/PostgreSQL chaos используйте docker compose --profile integration и Runbook-Disaster-Recovery."
