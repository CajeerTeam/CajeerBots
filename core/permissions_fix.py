from __future__ import annotations

from pathlib import Path

EXECUTABLE_ENTRYPOINTS: list[str] = []


def executable_paths(root: Path) -> list[Path]:
    paths = [root / item for item in EXECUTABLE_ENTRYPOINTS]
    scripts = root / "scripts"
    if scripts.exists():
        paths.extend(sorted(scripts.glob("*.sh")))
    return [path for path in paths if path.exists()]


def fix_permissions(root: Path) -> list[str]:
    changed: list[str] = []
    for path in executable_paths(root):
        mode = path.stat().st_mode
        if not mode & 0o111:
            path.chmod(mode | 0o755)
            changed.append(str(path.relative_to(root)))
    return changed
