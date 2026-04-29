#!/usr/bin/env bash
set -euo pipefail

PROFILE="${1:-static}"
export EVENT_SIGNING_SECRET="${EVENT_SIGNING_SECRET:-smoke-secret}"
export API_TOKEN="${API_TOKEN:-smoke-admin-token}"
export API_TOKEN_READONLY="${API_TOKEN_READONLY:-smoke-readonly-token}"
export API_TOKEN_METRICS="${API_TOKEN_METRICS:-smoke-metrics-token}"
export MODULES_ENABLED="${MODULES_ENABLED:-identity,rbac,logs,bridge}"
export PLUGINS_ENABLED="${PLUGINS_ENABLED:-example_plugin}"
export FAKE_ENABLED="${FAKE_ENABLED:-true}"
export FAKE_SCRIPT="${FAKE_SCRIPT:-/status|/help}"

python -m core doctor --offline --fix-permissions
python -m core adapters >/dev/null
python -m core modules >/dev/null
python -m core plugins >/dev/null
python -m core components >/dev/null
python -m core commands >/dev/null
python -m core db contract >/dev/null
python -m core update status >/dev/null
python scripts/check_syntax.py

if [ "$PROFILE" != "static" ]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker недоступен; выполнена только static smoke-проверка" >&2
    exit 0
  fi
  export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-cajeerbots-smoke}"
  docker compose --profile "$PROFILE" up -d --build
  trap 'docker compose --profile "$PROFILE" down -v' EXIT
  sleep 5
  curl -fsS http://127.0.0.1:8088/healthz >/dev/null
  curl -fsS http://127.0.0.1:8088/readyz >/dev/null || true
  curl -fsS -X POST http://127.0.0.1:8088/delivery/enqueue \
    -H "Authorization: Bearer ${API_TOKEN}" \
    -H 'Content-Type: application/json' \
    -d '{"adapter":"fake","target":"fake-chat","text":"smoke"}' >/dev/null || true
fi

echo "Smoke-проверка Cajeer Bots: успешно"
