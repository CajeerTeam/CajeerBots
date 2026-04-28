#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

require_python311() {
  command -v python3 >/dev/null 2>&1 || { echo "[!] python3 не найден"; exit 1; }
  python3 - <<'PY'
import sys
sys.exit(0 if sys.version_info[:2] == (3, 11) else 1)
PY
  if [[ $? -ne 0 ]]; then
    echo "[!] Требуется Python 3.11.x"
    exit 1
  fi
}

check_free_space() {
  local min_mb="${MIN_FREE_DISK_MB:-256}"
  local avail_kb
  avail_kb=$(df -Pk "$ROOT_DIR" | awk 'NR==2 {print $4}')
  if [[ -n "$avail_kb" ]] && (( avail_kb < min_mb * 1024 )); then
    echo "[!] Недостаточно свободного места: требуется минимум ${min_mb} MB"
    exit 1
  fi
}

require_writable_paths() {
  local data_dir="${DATA_DIR:-/app/data}"
  local log_dir="${LOG_DIR:-${data_dir}/logs}"
  local backup_dir="${BACKUP_DIR:-${data_dir}/backups}"
  mkdir -p "$data_dir" "$log_dir" "$backup_dir"
  [[ -w "$data_dir" && -w "$log_dir" && -w "$backup_dir" ]] || { echo "[!] Нет прав на запись в runtime-директории BotHost: $data_dir, $log_dir, $backup_dir"; exit 1; }
}

require_python311
check_free_space
require_writable_paths

if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m nmbot.main --preflight
python -m nmbot.main --migrate-only
python -m nmbot.healthcheck --mode readiness

echo "[✓] Зависимости установлены и миграции применены."
echo "[i] Для штатного обновления используйте: ./upgrade.sh"
echo "[i] Для запуска используйте: ./run.sh"
