#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

MODE="online"
BUILD_WHEELHOUSE="false"
WHEELHOUSE_DIR="${WHEELHOUSE_DIR:-wheelhouse}"
for arg in "$@"; do
  case "$arg" in
    --offline) MODE="offline" ;;
    --online) MODE="online" ;;
    --build-wheelhouse) BUILD_WHEELHOUSE="true" ;;
  esac
done

if ! command -v python3 >/dev/null 2>&1; then
  echo "[!] python3 не найден в PATH"
  exit 1
fi

if [[ ! -f .env ]]; then
  echo "[!] Для production archive ожидается существующий .env"
  exit 1
fi

if [[ ! -d .venv ]]; then
  echo "[i] Создаю виртуальное окружение..."
  python3 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate
export PIP_DISABLE_PIP_VERSION_CHECK=1

if [[ "$BUILD_WHEELHOUSE" == "true" ]]; then
  mkdir -p "$WHEELHOUSE_DIR"
  echo "[i] Скачиваю wheelhouse в $WHEELHOUSE_DIR ..."
  python -m pip download -r requirements.lock -d "$WHEELHOUSE_DIR"
fi

if [[ "$MODE" == "offline" ]]; then
  if [[ ! -d "$WHEELHOUSE_DIR" ]]; then
    echo "[!] OFFLINE mode требует каталог wheelhouse: $WHEELHOUSE_DIR"
    exit 1
  fi
  echo "[i] Устанавливаю зависимости из локального wheelhouse..."
  python -m pip install --no-index --find-links "$WHEELHOUSE_DIR" -r requirements.lock
else
  echo "[i] Устанавливаю зависимости из requirements.lock..."
  python -m pip install -r requirements.lock
fi

echo "[i] Подготавливаю runtime..."
python -m nmbot.main --prepare-runtime

echo "[✓] Bootstrap завершён"
