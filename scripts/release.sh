#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"
PYTHON_FLAGS="${PYTHON_FLAGS:--S}"
# shellcheck disable=SC2206
PY_CMD=("${PYTHON_BIN}" ${PYTHON_FLAGS})
DRY_RUN=false
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    *) echo "Неизвестный аргумент: $arg" >&2; exit 2 ;;
  esac
done

VERSION="$(cat VERSION)"
NAME="CajeerBots-${VERSION}"
RELEASE_CHECK_EVENT_SIGNING_SECRET="${RELEASE_CHECK_EVENT_SIGNING_SECRET:-cb_evt_0123456789abcdef0123456789abcdef0123456789abcdef}"
RELEASE_CHECK_API_TOKEN="${RELEASE_CHECK_API_TOKEN:-cb_api_0123456789abcdef0123456789abcdef0123456789abcdef}"
RELEASE_CHECK_API_TOKEN_READONLY="${RELEASE_CHECK_API_TOKEN_READONLY:-cb_read_0123456789abcdef0123456789abcdef0123456789abcdef}"
RELEASE_CHECK_API_TOKEN_METRICS="${RELEASE_CHECK_API_TOKEN_METRICS:-cb_metrics_0123456789abcdef0123456789abcdef0123456789abcdef}"
FORBIDDEN_PATTERN="Never""Mine|cajeer""_bots|cajeer""_core|nm""bot"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python не найден: ${PYTHON_BIN}" >&2
  exit 1
fi

if [ -d migrations ]; then
  echo "Каталог migrations не должен входить в проект; используйте alembic/" >&2
  exit 1
fi

chmod +x scripts/*.sh
./scripts/clean_artifacts.sh

find . -type d \( -name __pycache__ -o -name .pytest_cache -o -name .mypy_cache -o -name .ruff_cache \) -prune -exec rm -rf {} +
if find . -type f \( -name '*.pyc' -o -name '*.pyo' \) -not -path './.git/*' -not -path './dist/*' -print | grep -q .; then
  echo "В исходниках найден Python bytecode" >&2
  find . -type f \( -name '*.pyc' -o -name '*.pyo' \) -not -path './.git/*' -not -path './dist/*' -print >&2
  exit 1
fi
if [ -f .env ]; then
  echo ".env не должен входить в релизный исходник; используйте .env.example" >&2
  exit 1
fi

if grep -RInE "$FORBIDDEN_PATTERN"   --exclude-dir=.git --exclude-dir=dist --exclude-dir=runtime --exclude-dir=__pycache__ --exclude-dir=.pytest_cache   --exclude='*.zip' . >/tmp/cajeer-bots-forbidden.txt; then
  echo "Найдены запрещённые проектные или устаревшие термины:" >&2
  cat /tmp/cajeer-bots-forbidden.txt >&2
  exit 1
fi

for file in scripts/*.sh; do
  if [ -f "$file" ] && [ ! -x "$file" ]; then
    echo "Файл должен быть исполняемым: $file" >&2
    exit 1
  fi
done

"${PY_CMD[@]}" -m core.versioning
"${PY_CMD[@]}" scripts/check_syntax.py
"${PY_CMD[@]}" scripts/check_architecture.py
./scripts/check_docs.sh
./scripts/check_secrets.sh
./scripts/run_drills.sh
EVENT_SIGNING_SECRET="${EVENT_SIGNING_SECRET:-${RELEASE_CHECK_EVENT_SIGNING_SECRET}}" API_TOKEN="${API_TOKEN:-${RELEASE_CHECK_API_TOKEN}}" "${PY_CMD[@]}" -m core doctor --offline --profile release-artifact
"${PY_CMD[@]}" -m core adapters >/dev/null
"${PY_CMD[@]}" -m core modules >/dev/null
"${PY_CMD[@]}" -m core plugins >/dev/null
"${PY_CMD[@]}" -m core commands >/dev/null
if [ "$DRY_RUN" = "true" ]; then
  EVENT_SIGNING_SECRET="${RELEASE_CHECK_EVENT_SIGNING_SECRET}" API_TOKEN="${RELEASE_CHECK_API_TOKEN}" API_TOKEN_READONLY="${RELEASE_CHECK_API_TOKEN_READONLY}" API_TOKEN_METRICS="${RELEASE_CHECK_API_TOKEN_METRICS}" "${PY_CMD[@]}" -m core release verify . >/dev/null
  echo "Release dry-run завершён: проверки исходного дерева пройдены, dist не собирался."
  exit 0
fi
if "$PYTHON_BIN" -m pytest -q; then
  echo "Тесты пройдены"
else
  echo "pytest недоступен или тесты не прошли" >&2
  exit 1
fi

rm -rf dist
mkdir -p "dist/${NAME}"
cp -a README.md LICENSE VERSION pyproject.toml .env.example Dockerfile docker-compose.yml Makefile compatibility.yaml alembic.ini \
  core bots modules plugins distributed scripts ops wiki alembic schemas release admin \
  "dist/${NAME}/"
cp -a configs "dist/${NAME}/"
cp -a configs/env/.env*.example "dist/${NAME}/"
chmod +x "dist/${NAME}/scripts"/*.sh
find "dist/${NAME}" -type d \( -name __pycache__ -o -name .pytest_cache -o -name .mypy_cache -o -name .ruff_cache \) -prune -exec rm -rf {} +
find "dist/${NAME}" -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete

(cd dist && tar --mode='u+rwX,go+rX' -czf "${NAME}.tar.gz" "${NAME}" && sha256sum "${NAME}.tar.gz" > "${NAME}.tar.gz.sha256")
"${PY_CMD[@]}" scripts/build_release_zip.py "dist/${NAME}" "dist/${NAME}.zip" "${NAME}"
(cd dist && sha256sum "${NAME}.zip" > "${NAME}.zip.sha256")

TAR_SHA256="$(cut -d' ' -f1 "dist/${NAME}.tar.gz.sha256")"
ZIP_SHA256="$(cut -d' ' -f1 "dist/${NAME}.zip.sha256")"
cat > "dist/${NAME}.release.json" <<JSON
{
  "name": "CajeerBots",
  "version": "${VERSION}",
  "channel": "${CAJEER_UPDATE_CHANNEL:-stable}",
  "python": ">=3.11,<3.13",
  "db_contract": "cajeer.bots.db.v1",
  "event_contract": "cajeer.bots.event.v1",
  "requires_migration": true,
  "required_alembic_revision": "${CAJEER_RELEASE_REQUIRED_ALEMBIC_REVISION:-head}",
  "artifacts": [
    {"name": "${NAME}.tar.gz", "sha256": "${TAR_SHA256}"},
    {"name": "${NAME}.zip", "sha256": "${ZIP_SHA256}"}
  ]
}
JSON
cat > "dist/${NAME}.provenance.json" <<JSON
{
  "name": "CajeerBots",
  "version": "${VERSION}",
  "builder": "scripts/release.sh",
  "source_date_epoch": "${SOURCE_DATE_EPOCH:-}",
  "checks": ["syntax", "architecture", "docs", "secrets", "doctor", "pytest", "release-verify-deep"]
}
JSON
cat > "dist/${NAME}.spdx.json" <<JSON
{"SPDXID":"SPDXRef-DOCUMENT","spdxVersion":"SPDX-2.3","name":"${NAME}","dataLicense":"CC0-1.0","documentNamespace":"https://cajeer.local/spdx/${NAME}","packages":[{"SPDXID":"SPDXRef-Package-CajeerBots","name":"CajeerBots","versionInfo":"${VERSION}","licenseConcluded":"Apache-2.0"}]}
JSON
cat > "dist/${NAME}.cyclonedx.json" <<JSON
{"bomFormat":"CycloneDX","specVersion":"1.5","version":1,"metadata":{"component":{"type":"application","name":"CajeerBots","version":"${VERSION}"}}}
JSON

SIGNATURE_REQUIRED="${RELEASE_SIGNATURE_REQUIRED:-false}"
if [ -n "${RELEASE_SIGNING_KEY:-}" ] && [ -f "${RELEASE_SIGNING_KEY:-}" ]; then
  openssl dgst -sha256 -sign "${RELEASE_SIGNING_KEY}" -out "dist/${NAME}.sig" "dist/${NAME}.tar.gz"
elif [ "$SIGNATURE_REQUIRED" = "true" ]; then
  echo "RELEASE_SIGNATURE_REQUIRED=true, но RELEASE_SIGNING_KEY не задан или файл отсутствует" >&2
  exit 1
fi

EVENT_SIGNING_SECRET="${RELEASE_CHECK_EVENT_SIGNING_SECRET}" API_TOKEN="${RELEASE_CHECK_API_TOKEN}" API_TOKEN_READONLY="${RELEASE_CHECK_API_TOKEN_READONLY}" API_TOKEN_METRICS="${RELEASE_CHECK_API_TOKEN_METRICS}" "${PY_CMD[@]}" -m core release verify "dist/${NAME}.tar.gz" --deep
EVENT_SIGNING_SECRET="${RELEASE_CHECK_EVENT_SIGNING_SECRET}" API_TOKEN="${RELEASE_CHECK_API_TOKEN}" API_TOKEN_READONLY="${RELEASE_CHECK_API_TOKEN_READONLY}" API_TOKEN_METRICS="${RELEASE_CHECK_API_TOKEN_METRICS}" "${PY_CMD[@]}" -m core release verify "dist/${NAME}.zip" --deep

echo "Релиз создан: dist/${NAME}.tar.gz и dist/${NAME}.zip"
