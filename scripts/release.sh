#!/usr/bin/env bash
set -euo pipefail
VERSION="$(cat VERSION)"
NAME="CajeerBots-${VERSION}"
rm -rf dist
mkdir -p "dist/${NAME}"
cp -a README.md LICENSE VERSION pyproject.toml .env.example Dockerfile docker-compose.yml core bots modules plugins scripts ops wiki "dist/${NAME}/"
(cd dist && tar -czf "${NAME}.tar.gz" "${NAME}" && sha256sum "${NAME}.tar.gz" > "${NAME}.tar.gz.sha256")
echo "Релиз создан: dist/${NAME}.tar.gz"
