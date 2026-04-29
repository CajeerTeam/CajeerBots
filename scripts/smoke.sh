#!/usr/bin/env bash
set -euo pipefail

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
python scripts/check_syntax.py

echo "Smoke-проверка Cajeer Bots: успешно"
