#!/usr/bin/env bash
set -euo pipefail
[ ! -d .venv ] || . .venv/bin/activate
exec python -m cajeer_bots migrate
