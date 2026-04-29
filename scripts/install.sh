#!/usr/bin/env bash
set -euo pipefail
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -e '.[api,adapters,redis]'
[ -f .env ] || cp .env.example .env
echo "Cajeer Bots установлен. Перед боевым запуском заполните .env."
