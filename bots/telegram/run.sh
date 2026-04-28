#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'EOF'
Usage:
  ./run.sh --bootstrap [--offline]
  ./run.sh --check-config
  ./run.sh --prepare-runtime
  ./run.sh --readiness-check
  ./run.sh --preflight-check
  ./run.sh --upgrade
  ./run.sh [bot args]
EOF
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ "${1:-}" == "--bootstrap" ]]; then
  shift
  exec "$ROOT_DIR/bootstrap.sh" "$@"
fi

if [[ ! -f .env ]]; then
  echo "[!] Файл .env не найден. Для production archive должен существовать рабочий .env."
  exit 1
fi

if [[ ! -d .venv ]]; then
  echo "[!] Виртуальное окружение отсутствует. Выполните: ./bootstrap.sh"
  exit 1
fi

# shellcheck disable=SC1091
source .venv/bin/activate

if [[ "${1:-}" == "--check-config" ]]; then
  echo "[i] Проверяю конфиг..."
  exec python3 -m nmbot.main --check-config
fi

if [[ "${1:-}" == "--prepare-runtime" ]]; then
  echo "[i] Подготавливаю runtime..."
  exec python3 -m nmbot.main --prepare-runtime
fi

if [[ "${1:-}" == "--readiness-check" ]]; then
  echo "[i] Проверяю readiness (read-only)..."
  exec python3 -m nmbot.main --readiness-check
fi

if [[ "${1:-}" == "--preflight-check" ]]; then
  echo "[i] Проверяю production archive..."
  exec python3 preflight_check.py --production-archive
fi

if [[ "${1:-}" == "--upgrade" ]]; then
  echo "[i] Выполняю safe upgrade flow..."
  python3 db_tools.py backup
  python3 -m nmbot.main --prepare-runtime
  exec python3 -m nmbot.main --readiness-check
fi

if [[ "${RUN_PREPARE_ON_START:-false}" == "true" ]]; then
  python3 -m nmbot.main --prepare-runtime >/dev/null
fi

echo "[i] Запускаю NMTelegramBot..."
exec python3 -m nmbot.main "$@"
