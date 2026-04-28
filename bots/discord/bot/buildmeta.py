from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT_DIR = Path(__file__).resolve().parent.parent
BUILD_INFO_PATH = ROOT_DIR / 'build_info.json'
PYPROJECT_PATH = ROOT_DIR / 'pyproject.toml'
CHANGE_JOURNAL_PATH = ROOT_DIR / 'change_journal.json'
LAYOUT_PATH = ROOT_DIR / 'templates' / 'server_layout.json'


def load_build_info() -> dict[str, Any]:
    try:
        payload = json.loads(BUILD_INFO_PATH.read_text(encoding='utf-8'))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def read_pyproject_version() -> str:
    try:
        for line in PYPROJECT_PATH.read_text(encoding='utf-8').splitlines():
            if line.strip().startswith('version = '):
                return line.split('=', 1)[1].strip().strip('"').strip("'")
    except Exception:
        return ''
    return ''




def load_change_journal() -> dict[str, Any]:
    try:
        payload = json.loads(CHANGE_JOURNAL_PATH.read_text(encoding='utf-8'))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}



def _read_json_file(path: str | Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding='utf-8'))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def read_content_meta(content_path: str | Path) -> dict[str, Any]:
    payload = _read_json_file(content_path)
    meta = payload.get('meta') if isinstance(payload, dict) else {}
    return meta if isinstance(meta, dict) else {}


def read_layout_meta(layout_path: str | Path = LAYOUT_PATH) -> dict[str, Any]:
    payload = _read_json_file(layout_path)
    meta = payload.get('meta') if isinstance(payload, dict) else {}
    return meta if isinstance(meta, dict) else {}

def read_content_schema_version(content_path: str | Path) -> int:
    try:
        payload = json.loads(Path(content_path).read_text(encoding='utf-8'))
    except Exception:
        return 0
    meta = payload.get('meta') if isinstance(payload, dict) else {}
    try:
        return int((meta or {}).get('content_schema_version') or 0)
    except Exception:
        return 0


def build_runtime_drift_report(settings: Any, runtime_version: str) -> dict[str, Any]:
    build_info = load_build_info()
    pyproject_version = read_pyproject_version()
    content_path = getattr(settings, 'discord_content_file_path', ROOT_DIR / 'templates' / 'content.json')
    content_schema = read_content_schema_version(content_path)
    change_journal = load_change_journal()
    build_version = str(build_info.get('version') or '').strip()
    build_schema = int(build_info.get('content_schema_version') or 0)
    content_meta = read_content_meta(content_path)
    layout_meta = read_layout_meta()
    change_journal_version = str(change_journal.get('version') or '').strip()
    required_schema = int(getattr(settings, 'content_schema_version_required', 0) or 0)
    errors: list[str] = []
    warnings: list[str] = []
    if pyproject_version and pyproject_version != runtime_version:
        errors.append(f'Version drift: pyproject.toml={pyproject_version}, runtime={runtime_version}.')
    if build_version and build_version != runtime_version:
        errors.append(f'Version drift: build_info.json={build_version}, runtime={runtime_version}.')
    if not content_schema:
        errors.append('Не удалось определить schema_version у templates/content.json.')
    content_version = str(content_meta.get('version') or '').strip()
    runtime_version_declared = str(content_meta.get('runtime_version') or '').strip()
    canonical_path = str(content_meta.get('canonical_path') or '').strip()
    layout_schema = int(layout_meta.get('layout_schema_version') or 0) if layout_meta else 0
    alias_binding_version = int(layout_meta.get('alias_binding_version') or 0) if layout_meta else 0
    permission_matrix_version = int(layout_meta.get('permission_matrix_version') or 0) if layout_meta else 0
    if required_schema and content_schema and required_schema != content_schema:
        errors.append(f'Content schema drift: required={required_schema}, content={content_schema}.')
    if build_schema and content_schema and build_schema != content_schema:
        errors.append(f'Content schema drift: build_info={build_schema}, content={content_schema}.')
    if content_version and content_version != runtime_version:
        errors.append(f'Content version drift: content.json={content_version}, runtime={runtime_version}.')
    if runtime_version_declared and runtime_version_declared != runtime_version:
        errors.append(f'Content runtime_version drift: content.json={runtime_version_declared}, runtime={runtime_version}.')
    if canonical_path and canonical_path not in {'templates/content.json', './templates/content.json'}:
        warnings.append(f'Content canonical_path drift: {canonical_path}.')
    required_layout_schema = int(getattr(settings, 'layout_schema_version_required', 0) or 0)
    if required_layout_schema and layout_schema and layout_schema < required_layout_schema:
        errors.append(f'Layout schema drift: required={required_layout_schema}, layout={layout_schema}.')
    if int(getattr(settings, 'alias_binding_version_required', 0) or 0) and alias_binding_version < int(getattr(settings, 'alias_binding_version_required', 0) or 0):
        errors.append(f'Layout alias binding version drift: required={int(getattr(settings, "alias_binding_version_required", 0) or 0)}, layout={alias_binding_version}.')
    if int(getattr(settings, 'permission_matrix_version_required', 0) or 0) and permission_matrix_version < int(getattr(settings, 'permission_matrix_version_required', 0) or 0):
        errors.append(f'Layout permission matrix drift: required={int(getattr(settings, "permission_matrix_version_required", 0) or 0)}, layout={permission_matrix_version}.')
    journal_schema = int(change_journal.get('content_schema_version') or 0) if isinstance(change_journal, dict) else 0
    if journal_schema and content_schema and journal_schema != content_schema:
        errors.append(f'Change journal schema drift: change_journal.json={journal_schema}, content={content_schema}.')
    if change_journal_version and change_journal_version != runtime_version:
        warnings.append(f'Change journal drift: change_journal.json={change_journal_version}, runtime={runtime_version}.')
    return {
        'ok': not errors,
        'errors': errors,
        'warnings': warnings,
        'runtime_version': runtime_version,
        'pyproject_version': pyproject_version,
        'build_version': build_version,
        'content_schema_version': content_schema,
        'required_content_schema_version': required_schema,
        'build_content_schema_version': build_schema,
        'content_path': str(content_path),
        'content_version': content_version,
        'content_runtime_version': runtime_version_declared,
        'layout_schema_version': layout_schema,
        'alias_binding_version': alias_binding_version,
        'permission_matrix_version': permission_matrix_version,
        'change_journal': str(CHANGE_JOURNAL_PATH),
        'change_journal_version': change_journal_version,
        'data_dir': str(getattr(settings, 'data_dir', '')),
        'log_dir': str(getattr(settings, 'log_dir', '')),
        'backup_dir': str(getattr(settings, 'backup_dir', '')),
        'shared_dir': str(getattr(settings, 'shared_dir', '')),
        'shared_dir_available': bool(getattr(settings, 'shared_dir', Path('/nonexistent')).exists()) if getattr(settings, 'shared_dir', None) else False,
    }


def canonical_release_version() -> str:
    """Canonical release version comes from pyproject.toml."""
    return read_pyproject_version()


def build_schema_snapshot(settings: Any, runtime_version: str) -> dict[str, Any]:
    report = build_runtime_drift_report(settings, runtime_version)
    report['version_source'] = 'pyproject.toml'
    report['strict_runtime_precheck'] = bool(getattr(settings, 'strict_runtime_precheck', False))
    return report
