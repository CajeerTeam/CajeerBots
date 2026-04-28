from __future__ import annotations

import argparse
import json
import os
import stat
import tarfile
import zipfile
from pathlib import Path
from typing import Iterable

ROOT_DIR = Path(__file__).resolve().parent.parent
EXECUTABLE_PATHS = {"install.sh", "run.sh", "upgrade.sh", "setup_wizard.py"}
SKIP_DIRS = {".git", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", "dist"}
PUBLIC_DENYLIST = {".env"}
REQUIRED_MANIFEST_PATH = ROOT_DIR / "release_required_files.json"


def _load_required_manifest(root: Path = ROOT_DIR) -> dict[str, object]:
    manifest_path = root / "release_required_files.json"
    if not manifest_path.exists():
        return {"required_files": [], "executable_files": sorted(EXECUTABLE_PATHS), "forbidden_paths": []}
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {"required_files": [], "executable_files": sorted(EXECUTABLE_PATHS), "forbidden_paths": []}
    if not isinstance(payload, dict):
        return {"required_files": [], "executable_files": sorted(EXECUTABLE_PATHS), "forbidden_paths": []}
    return payload


class ReleasePackError(ValueError):
    pass


def _iter_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*"):
        rel = path.relative_to(root).as_posix()
        parts = set(path.relative_to(root).parts)
        if path.is_dir():
            continue
        if parts.intersection(SKIP_DIRS):
            continue
        files.append(path)
    return sorted(files, key=lambda item: item.relative_to(root).as_posix())


def _read_changed_files(root: Path, changed_file: Path | None, inline: str | None) -> list[Path]:
    names: list[str] = []
    if changed_file is not None:
        names.extend(line.strip() for line in changed_file.read_text(encoding="utf-8").splitlines())
    if inline:
        names.extend(item.strip() for item in inline.split(","))
    result: list[Path] = []
    for name in names:
        if not name or name.startswith("#"):
            continue
        path = (root / name).resolve()
        if root.resolve() not in path.parents and path != root.resolve():
            raise ReleasePackError(f"Changed file escapes archive root: {name}")
        if not path.exists() or not path.is_file():
            raise ReleasePackError(f"Changed file does not exist: {name}")
        result.append(path)
    return sorted(dict.fromkeys(result), key=lambda item: item.relative_to(root).as_posix())


def _apply_mode_policy(root: Path, files: Iterable[Path], *, mode: str) -> list[Path]:
    selected: list[Path] = []
    for path in files:
        rel = path.relative_to(root).as_posix()
        if mode == "public-release" and rel in PUBLIC_DENYLIST:
            continue
        selected.append(path)
    if mode == "public-release" and (root / ".env").exists():
        raise ReleasePackError("public-release отказался собираться: найден .env с production-секретами")
    if mode == "private-production" and not (root / ".env").exists():
        raise ReleasePackError("private-production требует реальный .env в корне архива")
    return selected


def _external_attr_for(path: Path, root: Path) -> int:
    rel = path.relative_to(root).as_posix()
    mode = stat.S_IMODE(path.stat().st_mode)
    if rel in EXECUTABLE_PATHS:
        mode |= stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
    return (stat.S_IFREG | mode) << 16


def _tar_filter(root: Path):
    def _filter(info: tarfile.TarInfo) -> tarfile.TarInfo:
        rel = info.name
        if rel in EXECUTABLE_PATHS:
            info.mode |= stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        return info
    return _filter


def build_release_archive(
    *,
    root: Path = ROOT_DIR,
    output_dir: Path | None = None,
    mode: str = "private-production",
    archive_format: str = "zip",
    changed_file: Path | None = None,
    inline_changed_files: str | None = None,
    name: str | None = None,
) -> list[Path]:
    root = root.resolve()
    output_dir = (output_dir or root / "dist").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if mode not in {"private-production", "public-release", "patch-changed-files"}:
        raise ReleasePackError("mode must be one of: private-production, public-release, patch-changed-files")
    if archive_format not in {"zip", "tar.gz", "both"}:
        raise ReleasePackError("format must be one of: zip, tar.gz, both")

    if mode == "patch-changed-files":
        files = _read_changed_files(root, changed_file, inline_changed_files)
        if not files:
            raise ReleasePackError("patch-changed-files requires --changed-file or --changed-files")
        # Never include real secrets in patch archives unless the caller explicitly lists another file name.
        files = [path for path in files if path.relative_to(root).as_posix() != ".env"]
    else:
        files = _iter_files(root)
    files = _apply_mode_policy(root, files, mode=mode)

    archive_base = name or f"NMDiscordBot-{mode}"
    outputs: list[Path] = []
    formats = ["zip", "tar.gz"] if archive_format == "both" else [archive_format]
    for fmt in formats:
        if fmt == "zip":
            target = output_dir / f"{archive_base}.zip"
            with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for path in files:
                    rel = path.relative_to(root).as_posix()
                    info = zipfile.ZipInfo(rel)
                    info.external_attr = _external_attr_for(path, root)
                    info.compress_type = zipfile.ZIP_DEFLATED
                    zf.writestr(info, path.read_bytes())
            outputs.append(target)
        else:
            target = output_dir / f"{archive_base}.tar.gz"
            with tarfile.open(target, "w:gz") as tf:
                for path in files:
                    rel = path.relative_to(root).as_posix()
                    tf.add(path, arcname=rel, filter=_tar_filter(root))
            outputs.append(target)
    return outputs



def check_archive(path: Path, *, mode: str = "private-production") -> dict[str, object]:
    errors: list[str] = []
    warnings: list[str] = []
    if not path.exists():
        return {"ok": False, "errors": [f"archive not found: {path}"], "warnings": warnings, "archive": str(path)}

    names: list[str] = []
    executable_modes: dict[str, str] = {}
    suffix = ''.join(path.suffixes[-2:]) if path.name.endswith('.tar.gz') else path.suffix
    try:
        if suffix == '.zip':
            with zipfile.ZipFile(path) as zf:
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    names.append(info.filename)
                    mode_bits = (info.external_attr >> 16) & 0o777
                    rel_name = info.filename.split('/', 1)[1] if '/' in info.filename else info.filename
                    if rel_name in EXECUTABLE_PATHS:
                        executable_modes[info.filename] = oct(mode_bits)
                        if not (mode_bits & stat.S_IXUSR):
                            errors.append(f"{info.filename} is not executable inside ZIP")
        elif path.name.endswith('.tar.gz'):
            with tarfile.open(path, 'r:gz') as tf:
                for member in tf.getmembers():
                    if not member.isfile():
                        continue
                    names.append(member.name)
                    rel_name = member.name.split('/', 1)[1] if '/' in member.name else member.name
                    if rel_name in EXECUTABLE_PATHS:
                        executable_modes[member.name] = oct(member.mode & 0o777)
                        if not (member.mode & stat.S_IXUSR):
                            errors.append(f"{member.name} is not executable inside tar.gz")
        else:
            errors.append("unsupported archive format; expected .zip or .tar.gz")
    except Exception as exc:
        errors.append(f"failed to read archive: {exc}")

    raw_names = list(names)
    if raw_names:
        first_parts = [name.split('/', 1)[0] for name in raw_names if '/' in name]
        if first_parts and len(first_parts) == len(raw_names) and len(set(first_parts)) == 1:
            prefix = first_parts[0] + '/'
            names = [name[len(prefix):] for name in raw_names]
        else:
            prefix = ''
    else:
        prefix = ''
    name_set = set(names)
    normalized_executable_modes: dict[str, str] = {}
    for key, value in executable_modes.items():
        normalized_key = key[len(prefix):] if prefix and key.startswith(prefix) else key
        normalized_executable_modes[normalized_key] = value
    executable_modes = normalized_executable_modes
    for forbidden in ('.github/', 'tests/', '__pycache__/'):
        if any(name == forbidden.rstrip('/') or name.startswith(forbidden) for name in name_set):
            errors.append(f"forbidden path exists in archive: {forbidden}")
    if any(name.endswith('.pyc') for name in name_set):
        errors.append("archive contains .pyc files")
    if '.env.example' in name_set:
        errors.append(".env.example must not be present")
    if mode == 'private-production' and '.env' not in name_set:
        errors.append("private-production archive must contain .env")
    if mode == 'public-release' and '.env' in name_set:
        errors.append("public-release archive must not contain .env")
    manifest = _load_required_manifest(ROOT_DIR)
    required_files = manifest.get('required_files') if isinstance(manifest, dict) else []
    if not isinstance(required_files, list):
        required_files = []
    if mode != 'patch-changed-files':
        if 'release_required_files.json' not in name_set:
            errors.append('release_required_files.json is missing from archive')
        for required in required_files:
            if isinstance(required, str) and required not in name_set:
                errors.append(f"required production-safety file missing: {required}")
        for required in ('nmbot/config_schema.py', 'nmbot/release_check.py', 'nmbot/schema_doctor.py', 'nmbot/release_pack.py', 'nmbot/env_doctor.py', 'nmbot/discord_bindings.py', 'nmbot/bridge_doctor.py'):
            if required not in name_set:
                errors.append(f"required production-safety file missing: {required}")
    executable_files = manifest.get('executable_files') if isinstance(manifest, dict) else []
    if not isinstance(executable_files, list) or not executable_files:
        executable_files = sorted(EXECUTABLE_PATHS)
    for executable in sorted(str(item) for item in executable_files):
        if executable not in name_set:
            warnings.append(f"expected executable file missing: {executable}")

    return {
        "ok": not errors,
        "archive": str(path),
        "mode": mode,
        "file_count": len(names),
        "root_prefix": prefix,
        "executable_modes": executable_modes,
        "required_files_checked": len(required_files),
        "canonical_release_packaging": True,
        "errors": errors,
        "warnings": warnings,
    }

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NMDiscordBot release packer")
    parser.add_argument("--root", default=str(ROOT_DIR), help="archive root")
    parser.add_argument("--output-dir", default="dist", help="output directory")
    parser.add_argument("--mode", choices=["private-production", "public-release", "patch-changed-files"], default="private-production")
    parser.add_argument("--format", choices=["zip", "tar.gz", "both"], default="both")
    parser.add_argument("--changed-file", help="newline-separated changed file list for patch-changed-files mode")
    parser.add_argument("--changed-files", help="comma-separated changed files for patch-changed-files mode")
    parser.add_argument("--name", help="archive base name without extension")
    parser.add_argument("--check-archive", help="check an existing .zip or .tar.gz archive instead of building")
    parser.add_argument("--check-mode", choices=["private-production", "public-release", "patch-changed-files"], default="private-production")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    root = Path(args.root).resolve()
    if args.check_archive:
        report = check_archive(Path(args.check_archive).resolve(), mode=args.check_mode)
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if report.get("ok") else 4
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = root / output_dir
    try:
        outputs = build_release_archive(
            root=root,
            output_dir=output_dir,
            mode=args.mode,
            archive_format=args.format,
            changed_file=Path(args.changed_file).resolve() if args.changed_file else None,
            inline_changed_files=args.changed_files,
            name=args.name,
        )
    except ReleasePackError as exc:
        print(f"[!] {exc}")
        return 2
    for path in outputs:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
