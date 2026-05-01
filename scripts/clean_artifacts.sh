#!/usr/bin/env bash
set -euo pipefail

# Удаляет локальные build/runtime артефакты, которые не должны попадать в source/release archive.
find . -type d \( -name __pycache__ -o -name .pytest_cache -o -name .mypy_cache -o -name .ruff_cache \) -prune -exec rm -rf {} +
find . -type f \( -name '*.pyc' -o -name '*.pyo' \) -not -path './.git/*' -not -path './dist/*' -delete
chmod +x scripts/*.sh 2>/dev/null || true
