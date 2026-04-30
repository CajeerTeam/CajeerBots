#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"
VERSION="$(cat VERSION)"
NAME="CajeerBots-${VERSION}"
FORBIDDEN_PATTERN="Never""Mine|cajeer""_bots|cajeer""_core|nm""bot"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python не найден: ${PYTHON_BIN}" >&2
  exit 1
fi

if [ -d migrations ]; then
  echo "Каталог migrations не должен входить в проект; используйте alembic/" >&2
  exit 1
fi

chmod +x run.sh install.sh setup_wizard.py scripts/*.sh

if grep -RInE "$FORBIDDEN_PATTERN" \
  --exclude-dir=.git --exclude-dir=dist --exclude-dir=runtime --exclude-dir=__pycache__ --exclude-dir=.pytest_cache \
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

"$PYTHON_BIN" scripts/check_syntax.py
find . -type d -name __pycache__ -prune -exec rm -rf {} +
EVENT_SIGNING_SECRET="${EVENT_SIGNING_SECRET:-release-secret}" API_TOKEN="${API_TOKEN:-release-token}" "$PYTHON_BIN" -m core doctor --offline
"$PYTHON_BIN" -m core adapters >/dev/null
"$PYTHON_BIN" -m core modules >/dev/null
"$PYTHON_BIN" -m core plugins >/dev/null
"$PYTHON_BIN" -m core commands >/dev/null
if "$PYTHON_BIN" -m pytest -q; then
  echo "Тесты пройдены"
else
  echo "pytest недоступен или тесты не прошли" >&2
  exit 1
fi

rm -rf dist
mkdir -p "dist/${NAME}"
cp -a README.md LICENSE VERSION pyproject.toml .env.example Dockerfile docker-compose.yml Makefile compatibility.yaml alembic.ini \
  core bots modules plugins distributed scripts ops wiki alembic install.sh run.sh setup_wizard.py run.py \
  "dist/${NAME}/"
chmod +x "dist/${NAME}/run.sh" "dist/${NAME}/install.sh" "dist/${NAME}/setup_wizard.py" "dist/${NAME}/scripts"/*.sh
(cd dist && tar --mode='u+rwX,go+rX' -czf "${NAME}.tar.gz" "${NAME}" && sha256sum "${NAME}.tar.gz" > "${NAME}.tar.gz.sha256")
"$PYTHON_BIN" scripts/build_release_zip.py "dist/${NAME}" "dist/${NAME}.zip" "${NAME}"
(cd dist && sha256sum "${NAME}.zip" > "${NAME}.zip.sha256")
SHA256_VALUE="$(cut -d' ' -f1 "dist/${NAME}.tar.gz.sha256")"
cat > "dist/${NAME}.release.json" <<JSON
{
  "name": "CajeerBots",
  "version": "${VERSION}",
  "channel": "${CAJEER_UPDATE_CHANNEL:-stable}",
  "python": ">=3.11",
  "db_contract": "cajeer.bots.db.v1",
  "event_contract": "cajeer.bots.event.v1",
  "requires_migration": true,
  "required_alembic_revision": "${CAJEER_RELEASE_REQUIRED_ALEMBIC_REVISION:-head}",
  "artifacts": [
    {
      "name": "${NAME}.tar.gz",
      "sha256": "${SHA256_VALUE}"
    }
  ]
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

# Проверка итогового tar.gz, а не только рабочей директории.
TMP_RELEASE_CHECK="$(mktemp -d)"
tar -xzf "dist/${NAME}.tar.gz" -C "$TMP_RELEASE_CHECK"
(cd "$TMP_RELEASE_CHECK/${NAME}" && EVENT_SIGNING_SECRET=release-secret API_TOKEN=release-token "$PYTHON_BIN" -m core doctor --offline)
rm -rf "$TMP_RELEASE_CHECK"

echo "Релиз создан: dist/${NAME}.tar.gz"
