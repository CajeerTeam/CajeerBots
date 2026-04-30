#!/usr/bin/env bash
set -euo pipefail

[ ! -d .venv ] || . .venv/bin/activate

PYTHON_BIN="${PYTHON_BIN:-python3}"

echo "Smoke: синтаксис"
"$PYTHON_BIN" -S scripts/check_syntax.py

echo "Smoke: runtime doctor offline"
EVENT_SIGNING_SECRET="${EVENT_SIGNING_SECRET:-smoke-secret}" API_TOKEN="${API_TOKEN:-smoke-token}" "$PYTHON_BIN" -m core doctor --offline --fix-permissions

if [ -n "${REDIS_URL:-}" ]; then
  echo "Smoke: Redis backend"
  EVENT_BUS_BACKEND=redis DELIVERY_BACKEND=redis DEAD_LETTER_BACKEND=redis IDEMPOTENCY_BACKEND=redis     EVENT_SIGNING_SECRET="${EVENT_SIGNING_SECRET:-smoke-secret}" API_TOKEN="${API_TOKEN:-smoke-token}"     "$PYTHON_BIN" -m core self-test
else
  echo "Smoke: REDIS_URL не задан, Redis-проверка пропущена"
fi

if [ -n "${DATABASE_ASYNC_URL:-}" ]; then
  echo "Smoke: PostgreSQL DB contract"
  "$PYTHON_BIN" -m core db check
else
  echo "Smoke: DATABASE_ASYNC_URL не задан, PostgreSQL-проверка пропущена"
fi
