#!/usr/bin/env bash
set -euo pipefail

[ ! -d .venv ] || . .venv/bin/activate

PYTHON_BIN="${PYTHON_BIN:-python3}"
ALEMBIC_CONFIG="${ALEMBIC_CONFIG:-alembic.ini}"
REVISION="${1:-head}"

if [ "${REVISION}" = "--status" ] || [ "${REVISION}" = "status" ]; then
  exec "$PYTHON_BIN" -m core db current
fi

echo "Применение миграций Alembic до revision: ${REVISION}"
exec "$PYTHON_BIN" -m core db upgrade "${REVISION}"
