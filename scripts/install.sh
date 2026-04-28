#!/usr/bin/env bash
set -euo pipefail
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -e '.[api,adapters]'
[ -f .env ] || cp .env.example .env
echo "Cajeer Bots installed. Edit .env before production launch."
