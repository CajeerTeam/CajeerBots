from __future__ import annotations

import re
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore


def read_version_file(root: Path) -> str:
    return (root / "VERSION").read_text(encoding="utf-8").strip()


def read_pyproject_version(root: Path) -> str:
    data = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    return str(data.get("project", {}).get("version", "")).strip()


def read_core_version(root: Path) -> str:
    text = (root / "core" / "__init__.py").read_text(encoding="utf-8")
    match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', text)
    return match.group(1).strip() if match else ""


def version_consistency_errors(root: Path) -> list[str]:
    versions = {
        "VERSION": read_version_file(root),
        "pyproject.toml": read_pyproject_version(root),
        "core/__init__.py": read_core_version(root),
    }
    expected = versions["VERSION"]
    errors = [f"{name}={value!r} не совпадает с VERSION={expected!r}" for name, value in versions.items() if value != expected]
    return errors


def main(argv: list[str] | None = None) -> int:
    root = Path.cwd()
    errors = version_consistency_errors(root)
    if errors:
        print("Проверка версии: есть проблемы")
        for error in errors:
            print(f"- {error}")
        return 1
    print(f"Проверка версии: ok ({read_version_file(root)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
