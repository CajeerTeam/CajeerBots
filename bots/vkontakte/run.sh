#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$ROOT_DIR/.venv"

if [[ ! -f "$ROOT_DIR/.env" ]]; then
  if [[ -f "$ROOT_DIR/.env.example" ]]; then
    echo "[!] Файл .env не найден."
    echo "    Скопируй .env.example -> .env и заполни секреты или запусти: python3 setup_wizard.py"
  else
    echo "[!] Файл .env не найден. Запусти: python3 setup_wizard.py"
  fi
  exit 1
fi

if [[ ! -d "$VENV_DIR" ]]; then
  python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
pip install --disable-pip-version-check -q -r "$ROOT_DIR/requirements.txt"
exec python3 "$ROOT_DIR/main.py"
