#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  echo "[!] Файл .env не найден. Для боевого архива он обязателен."
  exit 1
fi

DATA_DIR="${DATA_DIR:-/app/data}"
if ! mkdir -p "$DATA_DIR" 2>/dev/null || ! touch "$DATA_DIR/.write-test" 2>/dev/null; then
  FALLBACK_DATA_DIR="${TMPDIR:-/tmp}/nmdiscordbot/data"
  echo "[WARN] DATA_DIR is not writable/creatable: $DATA_DIR; using fallback $FALLBACK_DATA_DIR" >&2
  DATA_DIR="$FALLBACK_DATA_DIR"
  export DATA_DIR
  mkdir -p "$DATA_DIR"
else
  rm -f "$DATA_DIR/.write-test"
  export DATA_DIR
fi

LOG_DIR="${LOG_DIR:-${DATA_DIR}/logs}"
BACKUP_DIR="${BACKUP_DIR:-${DATA_DIR}/backups}"

if ! mkdir -p "$BACKUP_DIR" 2>/dev/null || ! touch "$BACKUP_DIR/.write-test" 2>/dev/null; then
  FALLBACK_BACKUP_DIR="${TMPDIR:-/tmp}/nmdiscordbot/backups"
  echo "[WARN] BACKUP_DIR is not writable/creatable: $BACKUP_DIR; using fallback $FALLBACK_BACKUP_DIR" >&2
  BACKUP_DIR="$FALLBACK_BACKUP_DIR"
  export BACKUP_DIR
  mkdir -p "$BACKUP_DIR"
else
  rm -f "$BACKUP_DIR/.write-test"
  export BACKUP_DIR
fi

if ! mkdir -p "$LOG_DIR" 2>/dev/null || ! touch "$LOG_DIR/.write-test" 2>/dev/null; then
  FALLBACK_LOG_DIR="${TMPDIR:-/tmp}/nmdiscordbot/logs"
  echo "[WARN] LOG_DIR is not writable/creatable: $LOG_DIR; using fallback $FALLBACK_LOG_DIR" >&2
  LOG_DIR="$FALLBACK_LOG_DIR"
  export LOG_DIR
  mkdir -p "$LOG_DIR"
else
  rm -f "$LOG_DIR/.write-test"
  export LOG_DIR
fi

if [[ ! -d .venv ]]; then
  echo "[!] Виртуальное окружение .venv не найдено. Сначала выполните: ./install.sh"
  exit 1
fi

source .venv/bin/activate
python - <<'PYCHECK'
import importlib.util, sys
required = ["discord", "aiohttp", "dotenv", "aiosqlite", "asyncpg", "redis"]
missing = [name for name in required if importlib.util.find_spec(name) is None]
if missing:
    print("[!] Не хватает Python-зависимостей: " + ", ".join(missing))
    print("[i] Выполните ./install.sh или ./upgrade.sh для установки/обновления зависимостей.")
    sys.exit(1)
PYCHECK
python -m nmbot.healthcheck --mode preflight
python -m nmbot.healthcheck --mode readiness

if [[ "${NM_RUN_MODE:-interactive}" == "systemd" ]]; then
  exec python main.py
fi

exec python main.py "$@"
