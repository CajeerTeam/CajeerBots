#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

abort() {
  echo "[!] $1"
  echo "[i] Обновление остановлено до применения миграций. Проверьте зависимости и повторите попытку."
  echo "[i] Подсказка: при критическом сбое вернитесь к предыдущему архиву и выполните ./install.sh заново."
  exit 1
}

command -v python3 >/dev/null 2>&1 || abort "python3 не найден"
python3 - <<'PY'
import sys
sys.exit(0 if sys.version_info[:2] == (3, 11) else 1)
PY
[[ $? -eq 0 ]] || abort "Требуется Python 3.11.x"

[[ -d .venv ]] || abort "Виртуальное окружение не найдено. Сначала выполните ./install.sh"
source .venv/bin/activate

python -m nmbot.main --preflight || abort "Preflight перед обновлением завершился ошибкой"
python -m pip install --upgrade pip || abort "Не удалось обновить pip"
pip install -r requirements.txt || abort "Не удалось установить/обновить Python-зависимости"
python -m nmbot.main --preflight || abort "Preflight после обновления зависимостей завершился ошибкой"
python -m nmbot.main --self-test || abort "Self-test после обновления зависимостей завершился ошибкой"
python -m nmbot.main --migrate-only || abort "Не удалось применить миграции"
python -m nmbot.main --schema-info || abort "Не удалось получить информацию о схеме"

echo "[✓] Обновление завершено. Теперь можно запускать ./run.sh"
