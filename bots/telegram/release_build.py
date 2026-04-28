#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import subprocess
import sys
import zipfile
from fnmatch import fnmatch
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent
DEFAULT_IGNORE = {'.git', '__pycache__', '.pytest_cache', '.mypy_cache', '.venv'}
MODES = {'clean-release', 'production-package', 'cutover-package', 'rollback-package'}


def _load_ignore_patterns() -> list[str]:
    path = ROOT / '.releaseignore'
    if not path.exists():
        return []
    patterns: list[str] = []
    for raw in path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        patterns.append(line)
    return patterns


def _match_ignore(rel_posix: str, patterns: list[str]) -> bool:
    parts = rel_posix.split('/')
    if any(part in DEFAULT_IGNORE for part in parts):
        return True
    for pattern in patterns:
        normalized = pattern.rstrip('/')
        if not normalized:
            continue
        if '/' not in normalized and fnmatch(parts[-1], normalized):
            return True
        if fnmatch(rel_posix, normalized) or fnmatch(rel_posix, normalized + '/**'):
            return True
        if rel_posix == normalized or rel_posix.startswith(normalized + '/'):
            return True
    return False


def iter_files(mode: str = 'clean-release') -> list[Path]:
    patterns = _load_ignore_patterns()
    if mode == 'production-package':
        patterns = [p for p in patterns if p not in {'.env', '.env.*'}]
    out: list[Path] = []
    for path in ROOT.rglob('*'):
        if not path.is_file():
            continue
        rel = path.relative_to(ROOT).as_posix()
        if _match_ignore(rel, patterns):
            continue
        out.append(path)
    return sorted(out)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def _run_checks(mode: str) -> None:
    preflight_cmd = [sys.executable, 'preflight_check.py']
    if mode == 'production-package':
        preflight_cmd.append('--production-archive')
    subprocess.run(preflight_cmd, cwd=ROOT, check=True)
    if mode in {'cutover-package', 'rollback-package'} and os.getenv('DATABASE_URL', '').startswith(('postgres://', 'postgresql://')):
        subprocess.run([sys.executable, 'preflight_check.py', '--live-backends'], cwd=ROOT, check=True)
    subprocess.run([sys.executable, '-m', 'compileall', '-q', 'nmbot', 'main.py', 'db_tools.py', 'setup_wizard.py', 'preflight_check.py', 'qa_smoke.py', 'qa_contract_nm_auth.py'], cwd=ROOT, check=True)
    subprocess.run([sys.executable, 'qa_smoke.py'], cwd=ROOT, check=True)
    subprocess.run([sys.executable, 'qa_contract_nm_auth.py'], cwd=ROOT, check=True)


def build_archive(dest: Path, *, mode: str = 'clean-release') -> dict[str, object]:
    files = iter_files(mode=mode)
    manifest = {
        'archive': str(dest),
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'mode': mode,
        'file_count': len(files),
        'includes_env_file': any(path.relative_to(ROOT).as_posix() == '.env' for path in files),
        'files': [],
    }
    with zipfile.ZipFile(dest, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            rel = path.relative_to(ROOT)
            zf.write(path, rel)
            manifest['files'].append({'path': rel.as_posix(), 'sha256': _sha256(path), 'size': path.stat().st_size})
    return manifest


def _verify_manifest(manifest_path: Path, signature_path: Path, *, strict: bool = False) -> int:
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    errors = []
    for item in manifest.get('files', []):
        p = ROOT / item['path']
        if not p.exists():
            errors.append({'missing': item['path']})
            continue
        if _sha256(p) != item['sha256']:
            errors.append({'checksum_mismatch': item['path']})
    sig_key = os.getenv('RELEASE_MANIFEST_HMAC_KEY', '')
    if strict and sig_key:
        expected = hmac.new(sig_key.encode('utf-8'), manifest_path.read_text(encoding='utf-8').encode('utf-8'), hashlib.sha256).hexdigest()
        got = signature_path.read_text(encoding='utf-8').strip() if signature_path.exists() else ''
        if expected != got:
            errors.append({'signature': 'mismatch'})
    print(json.dumps({'ok': not errors, 'errors': errors, 'files': len(manifest.get('files', []))}, ensure_ascii=False, indent=2))
    return 0 if not errors else 1


def main() -> int:
    parser = argparse.ArgumentParser(description='Strict release builder for NMTelegramBot')
    parser.add_argument('--output-dir', default='dist')
    parser.add_argument('--name', default='NMTelegramBot-release.zip')
    parser.add_argument('--skip-checks', action='store_true')
    parser.add_argument('--mode', choices=sorted(MODES), default='clean-release')
    parser.add_argument('--verify-manifest', default='', help='path to manifest json for verification')
    parser.add_argument('--signature', default='', help='path to manifest signature')
    parser.add_argument('--strict-signature', action='store_true')
    args = parser.parse_args()
    if args.verify_manifest:
        return _verify_manifest(Path(args.verify_manifest), Path(args.signature) if args.signature else Path(args.verify_manifest).with_suffix('.sig'), strict=args.strict_signature)
    if not args.skip_checks:
        _run_checks(args.mode)
    out = ROOT / args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    dest = out / args.name
    manifest = build_archive(dest, mode=args.mode)
    manifest_path = out / (dest.stem + '.manifest.json')
    manifest_text = json.dumps(manifest, ensure_ascii=False, indent=2)
    manifest_path.write_text(manifest_text, encoding='utf-8')
    signature_path = out / (dest.stem + '.manifest.sig')
    sig_key = os.getenv('RELEASE_MANIFEST_HMAC_KEY', '').encode('utf-8')
    signature_path.write_text(hmac.new(sig_key, manifest_text.encode('utf-8'), hashlib.sha256).hexdigest() if sig_key else '', encoding='utf-8')
    print(json.dumps({'archive': str(dest), 'manifest': str(manifest_path), 'signature': str(signature_path), 'files': manifest['file_count'], 'mode': args.mode}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
