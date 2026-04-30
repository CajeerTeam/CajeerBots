#!/usr/bin/env python3
from __future__ import annotations

import stat
import sys
import zipfile
from pathlib import Path

EXECUTABLES = {
    'run.sh',
    'install.sh',
    'setup_wizard.py',
    'scripts/doctor.sh',
    'scripts/install.sh',
    'scripts/migrate.sh',
    'scripts/release.sh',
    'scripts/run.sh',
    'scripts/smoke.sh',
    'scripts/smoke_integrations.sh',
}


def add_tree(source_dir: Path, zip_path: Path, archive_root: str) -> None:
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(source_dir.rglob('*')):
            if path.is_dir():
                continue
            rel = path.relative_to(source_dir).as_posix()
            info = zipfile.ZipInfo.from_file(path, f'{archive_root}/{rel}')
            mode = 0o755 if rel in EXECUTABLES or (rel.startswith('scripts/') and rel.endswith('.sh')) else 0o644
            info.external_attr = (stat.S_IFREG | mode) << 16
            with path.open('rb') as fh:
                zf.writestr(info, fh.read(), zipfile.ZIP_DEFLATED)


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print('usage: build_release_zip.py <source-dir> <zip-path> <archive-root>', file=sys.stderr)
        return 2
    add_tree(Path(argv[1]), Path(argv[2]), argv[3])
    return 0


if __name__ == '__main__':
    raise SystemExit(main(sys.argv))
