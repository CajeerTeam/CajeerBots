#!/usr/bin/env bash
set -euo pipefail

[ ! -d .venv ] || . .venv/bin/activate

PYTHON_BIN="${PYTHON_BIN:-python3}"
PYTHON_FLAGS="${PYTHON_FLAGS:--S}"
export CAJEER_BOTS_ENV="${CAJEER_BOTS_ENV:-test}"
export EVENT_SIGNING_SECRET="${EVENT_SIGNING_SECRET:-smoke-secret}"
export API_TOKEN="${API_TOKEN:-smoke-token}"
export API_TOKEN_READONLY="${API_TOKEN_READONLY:-smoke-readonly}"
export API_TOKEN_METRICS="${API_TOKEN_METRICS:-smoke-metrics}"
export TELEGRAM_ENABLED="${TELEGRAM_ENABLED:-false}"
export DISCORD_ENABLED="${DISCORD_ENABLED:-false}"
export VKONTAKTE_ENABLED="${VKONTAKTE_ENABLED:-false}"
export FAKE_ENABLED="${FAKE_ENABLED:-true}"
export EVENT_BUS_BACKEND="${EVENT_BUS_BACKEND:-memory}"
export DELIVERY_BACKEND="${DELIVERY_BACKEND:-memory}"
export DEAD_LETTER_BACKEND="${DEAD_LETTER_BACKEND:-memory}"
export IDEMPOTENCY_BACKEND="${IDEMPOTENCY_BACKEND:-memory}"

PY_CMD=("${PYTHON_BIN}")
if [ -n "${PYTHON_FLAGS}" ]; then
  # shellcheck disable=SC2206
  PY_CMD+=( ${PYTHON_FLAGS} )
fi

echo "Smoke: синтаксис"
"${PY_CMD[@]}" scripts/check_syntax.py

echo "Smoke: runtime doctor offline"
"${PY_CMD[@]}" -m core doctor --offline --profile dev --fix-permissions

echo "Smoke: local-memory self-test"
"${PY_CMD[@]}" -m core self-test --profile local-memory --offline

if [ -n "${REDIS_URL:-}" ]; then
  echo "Smoke: Redis backend"
  EVENT_BUS_BACKEND=redis DELIVERY_BACKEND=redis DEAD_LETTER_BACKEND=redis IDEMPOTENCY_BACKEND=redis \
    "${PY_CMD[@]}" -m core self-test --profile staging --offline
else
  echo "Smoke: REDIS_URL не задан, Redis-проверка пропущена"
fi

if [ -n "${DATABASE_ASYNC_URL:-}" ]; then
  echo "Smoke: PostgreSQL DB contract"
  "${PY_CMD[@]}" -m core db check
else
  echo "Smoke: DATABASE_ASYNC_URL не задан, PostgreSQL-проверка пропущена"
fi
