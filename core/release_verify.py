from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import tarfile
import tempfile
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable

REQUIRED_TOP_LEVEL = {
    "README.md",
    "LICENSE",
    "VERSION",
    "pyproject.toml",
    ".env.example",
    "Dockerfile",
    "docker-compose.yml",
    "Makefile",
    "compatibility.yaml",
    "alembic.ini",
    "core",
    "bots",
    "modules",
    "plugins",
    "distributed",
    "scripts",
    "ops",
    "wiki",
    "alembic",
    "install.sh",
    "run.sh",
    "setup_wizard.py",
}

EXECUTABLE_PATHS = {
    "run.sh",
    "install.sh",
    "setup_wizard.py",
    "scripts/doctor.sh",
    "scripts/install.sh",
    "scripts/migrate.sh",
    "scripts/release.sh",
    "scripts/run.sh",
    "scripts/smoke.sh",
    "scripts/smoke_integrations.sh",
}

FORBIDDEN_PARTS = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".git",
}

FORBIDDEN_FILENAMES = {".env"}


@dataclass
class ReleaseVerification:
    artifact: str
    ok: bool
    root: str | None = None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    checked_files: int = 0
    deep_checks: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _normalize_member(name: str) -> str:
    return name.replace("\\", "/").strip("/")


def _detect_root(names: Iterable[str]) -> str | None:
    roots = {item.split("/", 1)[0] for item in names if item and "/" in item}
    if len(roots) == 1:
        return next(iter(roots))
    return None


def _relative_to_root(name: str, root: str | None) -> str:
    name = _normalize_member(name)
    if root and (name == root or name.startswith(root + "/")):
        return name[len(root):].strip("/")
    return name


def _mode_from_zip(info: zipfile.ZipInfo) -> int:
    return (info.external_attr >> 16) & 0o777777


def _is_executable(mode: int) -> bool:
    return bool(mode & 0o111)


def _verify_member_list(members: dict[str, int | None], *, artifact: Path) -> ReleaseVerification:
    normalized = {_normalize_member(name): mode for name, mode in members.items() if _normalize_member(name)}
    root = _detect_root(normalized.keys())
    relative = {_relative_to_root(name, root): mode for name, mode in normalized.items() if _relative_to_root(name, root)}
    result = ReleaseVerification(str(artifact), ok=True, root=root, checked_files=len(relative))

    for required in sorted(REQUIRED_TOP_LEVEL):
        found = required in relative or any(item.startswith(required + "/") for item in relative)
        if not found:
            result.errors.append(f"обязательный файл/каталог отсутствует: {required}")

    for name, mode in sorted(relative.items()):
        parts = set(Path(name).parts)
        if parts & FORBIDDEN_PARTS:
            result.errors.append(f"запрещённый runtime/cache путь в артефакте: {name}")
        if Path(name).name in FORBIDDEN_FILENAMES:
            result.errors.append(f"секретный env-файл не должен входить в артефакт: {name}")
        if name.endswith(".pyc") or name.endswith(".pyo"):
            result.errors.append(f"байткод Python не должен входить в артефакт: {name}")

    for path in sorted(EXECUTABLE_PATHS):
        if path not in relative:
            continue
        mode = relative[path]
        if mode is None:
            result.warnings.append(f"нельзя проверить executable-bit для {path}: формат не хранит unix mode")
        elif not _is_executable(mode):
            result.errors.append(f"файл должен быть исполняемым в артефакте: {path}")

    result.ok = not result.errors
    return result


def _extract_artifact(artifact: Path, destination: Path) -> Path:
    if zipfile.is_zipfile(artifact):
        with zipfile.ZipFile(artifact) as archive:
            archive.extractall(destination)
    elif tarfile.is_tarfile(artifact):
        with tarfile.open(artifact) as archive:
            archive.extractall(destination)
    else:
        raise ValueError(f"неподдерживаемый формат артефакта: {artifact}")
    children = [item for item in destination.iterdir() if item.name not in {"__MACOSX"}]
    if len(children) == 1 and children[0].is_dir():
        return children[0]
    return destination


def _run_deep_checks(root: Path, *, python_bin: str = "python3") -> dict[str, object]:
    env = os.environ.copy()
    env.setdefault("EVENT_SIGNING_SECRET", "release-verify-secret")
    env.setdefault("API_TOKEN", "release-verify-token")
    env.setdefault("API_TOKEN_READONLY", "release-verify-readonly")
    env.setdefault("API_TOKEN_METRICS", "release-verify-metrics")
    commands = [
        [python_bin, "scripts/check_syntax.py"],
        [python_bin, "-m", "core", "doctor", "--offline", "--profile", "release-artifact"],
        ["bash", "scripts/smoke_integrations.sh"],
    ]
    results: list[dict[str, object]] = []
    for command in commands:
        completed = subprocess.run(command, cwd=root, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
        results.append({"command": command, "returncode": completed.returncode, "output": completed.stdout[-4000:]})
    return {"ok": all(item["returncode"] == 0 for item in results), "commands": results}


def verify_release_artifact(artifact: str | Path, *, deep: bool = False, python_bin: str = "python3") -> ReleaseVerification:
    artifact = Path(artifact)
    if not artifact.exists():
        return ReleaseVerification(str(artifact), ok=False, errors=["артефакт не найден"])

    if zipfile.is_zipfile(artifact):
        with zipfile.ZipFile(artifact) as archive:
            members = {info.filename: _mode_from_zip(info) for info in archive.infolist() if not info.is_dir()}
    elif tarfile.is_tarfile(artifact):
        with tarfile.open(artifact) as archive:
            members = {member.name: member.mode for member in archive.getmembers() if member.isfile()}
    else:
        return ReleaseVerification(str(artifact), ok=False, errors=["артефакт должен быть .zip или .tar.gz/.tgz"])

    result = _verify_member_list(members, artifact=artifact)
    if deep and result.ok:
        temp = Path(tempfile.mkdtemp(prefix="cajeer-bots-release-verify-"))
        try:
            root = _extract_artifact(artifact, temp)
            # zip extraction may drop execute bits depending on the platform; enforce only for deep runtime checks.
            for relative in EXECUTABLE_PATHS:
                path = root / relative
                if path.exists():
                    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            result.deep_checks = _run_deep_checks(root, python_bin=python_bin)
            if not result.deep_checks.get("ok"):
                result.errors.append("deep release checks failed")
        finally:
            shutil.rmtree(temp, ignore_errors=True)
    result.ok = not result.errors
    return result


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Проверить release artifact Cajeer Bots.")
    parser.add_argument("artifact")
    parser.add_argument("--deep", action="store_true", help="Распаковать артефакт и выполнить syntax/doctor/smoke проверки.")
    parser.add_argument("--python", default="python3", help="Python interpreter для deep-проверок.")
    args = parser.parse_args(argv)
    result = verify_release_artifact(args.artifact, deep=args.deep, python_bin=args.python)
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
