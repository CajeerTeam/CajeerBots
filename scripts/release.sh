#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python}"
VERSION="$(cat VERSION)"
NAME="CajeerBots-${VERSION}"
FORBIDDEN_PATTERN="Never""Mine|cajeer""_bots|cajeer""_core|nm""bot"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python не найден: ${PYTHON_BIN}" >&2
  exit 1
fi

if grep -RInE "$FORBIDDEN_PATTERN" \
  --exclude-dir=.git --exclude-dir=dist --exclude-dir=runtime --exclude-dir=__pycache__ \
  --exclude='*.zip' . >/tmp/cajeer-bots-forbidden.txt; then
  echo "Найдены запрещённые проектные или устаревшие термины:" >&2
  cat /tmp/cajeer-bots-forbidden.txt >&2
  exit 1
fi

for file in run.sh install.sh setup_wizard.py scripts/*.sh; do
  if [ -f "$file" ] && [ ! -x "$file" ]; then
    echo "Файл должен быть исполняемым: $file" >&2
    exit 1
  fi
done

"$PYTHON_BIN" -m compileall -q core bots modules plugins tests
find . -type d -name __pycache__ -prune -exec rm -rf {} +
EVENT_SIGNING_SECRET="${EVENT_SIGNING_SECRET:-release-secret}" API_TOKEN="${API_TOKEN:-release-token}" "$PYTHON_BIN" -m core doctor --offline
"$PYTHON_BIN" -m core adapters >/dev/null
"$PYTHON_BIN" -m core modules >/dev/null
"$PYTHON_BIN" -m core plugins >/dev/null
"$PYTHON_BIN" -m core commands >/dev/null

rm -rf dist
mkdir -p "dist/${NAME}"
cp -a README.md LICENSE VERSION pyproject.toml .env.example Dockerfile docker-compose.yml Makefile compatibility.yaml \
  core bots modules plugins scripts ops wiki install.sh run.sh setup_wizard.py main.py \
  "dist/${NAME}/"
(cd dist && tar -czf "${NAME}.tar.gz" "${NAME}" && sha256sum "${NAME}.tar.gz" > "${NAME}.tar.gz.sha256")
echo "Релиз создан: dist/${NAME}.tar.gz"
