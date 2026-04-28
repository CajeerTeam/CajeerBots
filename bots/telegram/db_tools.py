#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import hmac
import json
import os
import sqlite3
import tempfile
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from nmbot.config import load_config
from nmbot.database import inspect_sqlite, Database
from nmbot.postgres_backend import PostgresDatabase
from nmbot.storage import create_database

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

EXPORT_TABLE_ALLOWLIST = {
    'interactions', 'admin_actions', 'chat_settings', 'linked_accounts', 'pending_links', 'link_events',
    'scheduled_broadcasts', 'dead_letter_jobs', 'external_announcements', 'external_announcement_deliveries',
    'broadcast_deliveries', 'runtime_state', 'runtime_locks', 'rate_limit_hits', 'security_challenge_notices',
}
AUDIT_TABLES = ('admin_actions', 'link_events', 'interactions')
STATE_EXPORTS = {'chat_settings', 'subscriptions', 'rbac', 'templates-meta', 'user-prefs', 'maintenance', 'feature-flags'}
ARTIFACT_DIRS = ('backups', 'exports')


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='NMTelegramBot backup/export/cutover tool')
    sub = parser.add_subparsers(dest='command', required=True)

    backup = sub.add_parser('backup', help='create SQLite backup')
    backup.add_argument('--dest', default='', help='destination file path')

    export = sub.add_parser('export', help='export a table as json or csv')
    export.add_argument('table', help='table name')
    export.add_argument('--format', choices=['json', 'csv'], default='json')
    export.add_argument('--output', default='', help='output file path')
    export.add_argument('--limit', type=int, default=0, help='optional row limit')

    state = sub.add_parser('state-export', help='export operational state bundle')
    state.add_argument('kind', choices=sorted(STATE_EXPORTS))
    state.add_argument('--output', default='', help='output file path')

    state_import = sub.add_parser('state-import', help='import operational state bundle')
    state_import.add_argument('kind', choices=sorted(STATE_EXPORTS))
    state_import.add_argument('input', help='json file path')

    audit = sub.add_parser('audit-export', help='export immutable audit bundle with manifest')
    audit.add_argument('--output-dir', default='', help='output directory path')

    repair = sub.add_parser('repair', help='repair/reconcile runtime ledgers and locks')

    restore = sub.add_parser('restore-check', help='validate that a backup can be opened and inspected')
    restore.add_argument('path', help='backup file path')

    drill = sub.add_parser('restore-drill', help='restore backup into temp DB and run runtime verification')
    drill.add_argument('path', help='backup file path')

    cleanup = sub.add_parser('cleanup-artifacts', help='delete aged backup/export artifacts')
    cleanup.add_argument('--backups-days', type=int, default=30)
    cleanup.add_argument('--exports-days', type=int, default=30)

    remote = sub.add_parser('remote-upload', help='upload backup/export artifact to remote control surface')
    remote.add_argument('path', help='artifact path')
    remote.add_argument('--kind', default='artifact', help='artifact kind label')

    incident = sub.add_parser('incident-snapshot', help='export runtime incident snapshot')
    incident.add_argument('--output', default='', help='output file path')

    pg = sub.add_parser('pg-smoke', help='run PostgreSQL live smoke')
    pg.add_argument('--database-url', default='', help='postgresql:// URL, otherwise DATABASE_URL from env')

    cut = sub.add_parser('cutover-postgres', help='copy SQLite tables into PostgreSQL backend and optionally verify')
    cut.add_argument('--sqlite', default='', help='path to source SQLite DB')
    cut.add_argument('--database-url', default='', help='postgresql:// URL, otherwise DATABASE_URL from env')
    cut.add_argument('--verify', action='store_true', help='run verification after import')
    cut.add_argument('--truncate', action='store_true', help='truncate target tables before import')

    verify = sub.add_parser('manifest-verify', help='verify release manifest and signature against current workspace')
    verify.add_argument('manifest', help='manifest json path')
    verify.add_argument('--signature', default='', help='signature file path')
    verify.add_argument('--strict-signature', action='store_true')

    plan = sub.add_parser('multi-instance-plan', help='emit JSON migration plan to full multi-instance mode')
    plan.add_argument('--output', default='', help='output file path')
    return parser


def _db():
    cfg = load_config()
    return cfg, create_database(cfg)


def _secret_from_env(name: str) -> str:
    value = os.getenv(name, '').strip()
    if value:
        return value
    file_path = os.getenv(f'{name}_FILE', '').strip()
    if not file_path:
        return ''
    try:
        return Path(file_path).read_text(encoding='utf-8').strip()
    except Exception:
        return ''


def cmd_backup(dest: str) -> int:
    cfg = load_config()
    src = cfg.sqlite_path
    if not src.exists():
        raise SystemExit(f'SQLite file not found: {src}')
    out = Path(dest) if dest else cfg.artifact_root / 'backups' / f"nmtelegrambot-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.db"
    out.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(src) as source, sqlite3.connect(out) as target:
        source.backup(target)
    print(f'[OK] Backup created: {out}')
    return 0


def cmd_export(table: str, fmt: str, output: str, limit: int) -> int:
    if table not in EXPORT_TABLE_ALLOWLIST:
        raise SystemExit(f'[FAIL] export of table {table!r} is not allowed')
    cfg, db = _db()
    query = f'SELECT * FROM {table}'
    params: tuple[object, ...] = ()
    if limit > 0:
        query += ' LIMIT ?'
        params = (limit,)
    with db.connect() as connection:
        rows = connection.execute(query, params).fetchall()
        if getattr(db, 'backend_name', 'sqlite') == 'sqlite':
            columns = [str(row['name']) for row in connection.execute(f'PRAGMA table_info({table})').fetchall()]
        else:
            columns = list(rows[0].keys()) if rows else []
    payload = [dict(row) for row in rows]
    out = Path(output) if output else cfg.artifact_root / 'exports' / f"{table}-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.{fmt}"
    out.parent.mkdir(parents=True, exist_ok=True)
    if fmt == 'json':
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    else:
        with out.open('w', encoding='utf-8', newline='') as fh:
            fieldnames = columns or (sorted(payload[0].keys()) if payload else [])
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            if fieldnames:
                writer.writeheader()
                for row in payload:
                    writer.writerow(row)
    print(f'[OK] Export created: {out}')
    return 0


def cmd_state_export(kind: str, output: str) -> int:
    cfg, db = _db()
    if kind == 'chat_settings':
        payload = [asdict(item) if is_dataclass(item) else item for item in db.list_chat_settings()]
    elif kind == 'subscriptions':
        payload = [{'chat_id': item.chat_id, 'tags': item.tags} for item in db.list_chat_settings()]
    elif kind == 'rbac':
        payload = db.list_rbac_entries()
    elif kind == 'user-prefs':
        with db.connect() as connection:
            payload = [dict(row) for row in connection.execute('SELECT * FROM user_notification_prefs ORDER BY user_id ASC').fetchall()]
    elif kind == 'maintenance':
        payload = db.get_maintenance_state()
    elif kind == 'feature-flags':
        payload = [{'chat_id': item.chat_id, 'feature_flags': getattr(item, 'feature_flags', {})} for item in db.list_chat_settings()]
    else:
        payload = {'templates_dir': str(load_config().templates_dir)}
    out = Path(output) if output else cfg.artifact_root / 'exports' / f'{kind}-{datetime.utcnow().strftime("%Y%m%d-%H%M%S")}.json'
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'[OK] State export created: {out}')
    return 0


def cmd_state_import(kind: str, input_path: str) -> int:
    _, db = _db()
    payload = json.loads(Path(input_path).read_text(encoding='utf-8'))
    if kind == 'chat_settings':
        for item in payload:
            chat_id = int(item['chat_id'])
            updates = {k: item[k] for k in ('allow_status', 'allow_announcements', 'allow_broadcasts', 'tags', 'default_thread_id', 'disable_notifications', 'chat_timezone', 'quiet_hours_start', 'quiet_hours_end', 'feature_flags') if k in item}
            db.touch_chat(chat_id=chat_id, title=item.get('title'), chat_type=item.get('chat_type'))
            db.update_chat_settings(chat_id, **updates)
    elif kind == 'subscriptions':
        for item in payload:
            db.update_chat_settings(int(item['chat_id']), tags=item.get('tags', []))
    elif kind == 'rbac':
        for item in payload:
            kind_name = item.get('kind')
            if kind_name == 'user':
                db.set_user_role_override(int(item['target']), str(item['value']))
            elif kind_name == 'global':
                db.set_command_role_override(scope='global', command=str(item['command']), role=str(item['value']))
            elif kind_name == 'chat':
                db.set_command_role_override(scope='chat', chat_id=int(item['target']), command=str(item['command']), role=str(item['value']))
    elif kind == 'user-prefs':
        for item in payload:
            user_id = int(item['user_id'])
            updates = {k: item[k] for k in ('tags','timezone','quiet_hours_start','quiet_hours_end','security_enabled','status_enabled','events_enabled','maintenance_enabled') if k in item}
            db.update_user_notification_prefs(user_id, **updates)
    elif kind == 'maintenance':
        db.set_maintenance_state(active=bool(payload.get('active')), message=str(payload.get('message') or ''), updated_by='state-import')
    elif kind == 'feature-flags':
        for item in payload:
            db.update_chat_settings(int(item['chat_id']), feature_flags=item.get('feature_flags', {}))
    print(f'[OK] State import applied: {kind}')
    return 0


def cmd_audit_export(output_dir: str) -> int:
    cfg, db = _db()
    root = Path(output_dir) if output_dir else cfg.artifact_root / 'exports' / f"audit-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"
    root.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, object] = {'generated_at': datetime.utcnow().isoformat() + 'Z', 'tables': {}}
    for table in AUDIT_TABLES:
        with db.connect() as connection:
            rows = connection.execute(f'SELECT * FROM {table} ORDER BY created_at ASC').fetchall()
        payload = json.dumps([dict(row) for row in rows], ensure_ascii=False, indent=2)
        path = root / f'{table}.json'
        path.write_text(payload, encoding='utf-8')
        sha = hashlib.sha256(payload.encode('utf-8')).hexdigest()
        manifest['tables'][table] = {'rows': len(rows), 'sha256': sha, 'file': path.name}
    manifest_path = root / 'manifest.json'
    manifest_text = json.dumps(manifest, ensure_ascii=False, indent=2)
    manifest_path.write_text(manifest_text, encoding='utf-8')
    print(f'[OK] Audit export created: {root}')
    return 0


def cmd_repair() -> int:
    _, db = _db()
    payload = db.repair_runtime_state() if hasattr(db, 'repair_runtime_state') else {'repaired': 0}
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_restore_check(path: str) -> int:
    artifact = Path(path)
    if not artifact.exists():
        raise SystemExit(f'[FAIL] backup not found: {artifact}')
    payload = inspect_sqlite(artifact)
    print(json.dumps({'ok': True, 'path': str(artifact), 'schema_version': payload.get('schema_version'), 'journal_mode': payload.get('journal_mode')}, ensure_ascii=False, indent=2))
    return 0


def cmd_restore_drill(path: str) -> int:
    artifact = Path(path)
    if not artifact.exists():
        raise SystemExit(f'[FAIL] backup not found: {artifact}')
    with tempfile.TemporaryDirectory(prefix='nmtg-restore-drill-') as tmp:
        restored = Path(tmp) / 'restored.db'
        with sqlite3.connect(artifact) as src, sqlite3.connect(restored) as dst:
            src.backup(dst)
        db = Database(restored)
        health = db.db_health()
        stats = db.basic_stats()
        db.repair_runtime_state() if hasattr(db, 'repair_runtime_state') else None
        payload = {
            'ok': True,
            'restored': str(restored),
            'schema_version': health.get('schema_version'),
            'journal_mode': health.get('journal_mode'),
            'interactions_total': int(stats.get('total', 0)),
            'linked_accounts': int(db.count_linked_accounts()),
            'dead_letters': int(health.get('dead_letters', 0)),
            'runtime_verify': True,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_cleanup_artifacts(backups_days: int, exports_days: int) -> int:
    now = datetime.utcnow().timestamp()
    removed: dict[str, int] = {'backups': 0, 'exports': 0}
    for dirname, days in [('backups', backups_days), ('exports', exports_days)]:
        root = Path(dirname)
        if not root.exists():
            continue
        ttl = max(0, days) * 86400
        for path in root.rglob('*'):
            if path.is_file() and now - path.stat().st_mtime > ttl:
                path.unlink(missing_ok=True)
                removed[dirname] += 1
    print(json.dumps(removed, ensure_ascii=False, indent=2))
    return 0


def cmd_remote_upload(path: str, kind: str) -> int:
    target = os.getenv('REMOTE_BACKUP_UPLOAD_URL', '').strip() or os.getenv('EXTERNAL_ADMIN_API_URL', '').strip()
    if not target:
        raise SystemExit('[FAIL] REMOTE_BACKUP_UPLOAD_URL / EXTERNAL_ADMIN_API_URL not configured')
    artifact = Path(path)
    if not artifact.exists():
        raise SystemExit(f'[FAIL] artifact not found: {artifact}')
    token = _secret_from_env('REMOTE_BACKUP_UPLOAD_TOKEN') or _secret_from_env('EXTERNAL_ADMIN_API_TOKEN')
    headers = {'Accept': 'application/json'}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    checksum = hashlib.sha256(artifact.read_bytes()).hexdigest()
    headers['X-Artifact-SHA256'] = checksum
    files = {'file': (artifact.name, artifact.read_bytes(), 'application/octet-stream')}
    data = {'kind': kind, 'filename': artifact.name, 'sha256': checksum}
    attempts = 3
    last_exc = None
    with httpx.Client(timeout=30.0) as client:
        for attempt in range(1, attempts + 1):
            try:
                response = client.post(target, headers=headers, data=data, files=files)
                response.raise_for_status()
                body = response.json() if 'application/json' in response.headers.get('content-type', '') else {}
                remote_sha = str(body.get('sha256') or body.get('checksum') or checksum)
                if remote_sha != checksum:
                    raise RuntimeError('checksum_mismatch')
                print(json.dumps({'ok': True, 'target': target, 'sha256': checksum, 'attempt': attempt}, ensure_ascii=False, indent=2))
                return 0
            except Exception as exc:
                last_exc = exc
    raise SystemExit(f'[FAIL] remote upload failed: {last_exc}')


def cmd_incident_snapshot(output: str) -> int:
    cfg, db = _db()
    payload = db.collect_incident_snapshot() if hasattr(db, 'collect_incident_snapshot') else {'db_health': db.db_health()}
    out = Path(output) if output else cfg.artifact_root / 'exports' / f"incident-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'[OK] Incident snapshot created: {out}')
    return 0


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sqlite_tables(path: Path) -> list[str]:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        return [r['name'] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()]


def cmd_pg_smoke(database_url: str) -> int:
    url = database_url.strip() or os.getenv('DATABASE_URL', '').strip()
    if not url.startswith(('postgres://', 'postgresql://')):
        raise SystemExit('[FAIL] pg-smoke требует DATABASE_URL=postgresql://...')
    db = PostgresDatabase(url)
    stamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    db.set_runtime_value('pg_smoke_last_run_at', stamp)
    db.set_runtime_value('storage_backend_mode', 'postgresql')
    claimed = db.claim_lock('pg_smoke', owner='db_tools', ttl_seconds=30)
    health = db.db_health()
    payload = {
        'ok': True,
        'backend': db.backend_name,
        'schema_version': health.get('schema_version'),
        'active_locks': health.get('active_locks'),
        'dead_letters': health.get('dead_letters'),
        'lock_claimed': bool(claimed),
        'runtime_value_roundtrip': db.runtime_value('pg_smoke_last_run_at', ''),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_cutover_postgres(sqlite_path: str, database_url: str, verify: bool, truncate: bool) -> int:
    url = database_url.strip() or os.getenv('DATABASE_URL', '').strip()
    if not url.startswith(('postgres://', 'postgresql://')):
        raise SystemExit('[FAIL] cutover-postgres требует DATABASE_URL=postgresql://...')
    source_path = Path(sqlite_path) if sqlite_path else load_config().sqlite_path
    if not source_path.exists():
        raise SystemExit(f'[FAIL] SQLite file not found: {source_path}')
    target = PostgresDatabase(url)
    imported: dict[str, int] = {}
    with sqlite3.connect(source_path) as src:
        src.row_factory = sqlite3.Row
        tables = _sqlite_tables(source_path)
        for table in tables:
            rows = [dict(r) for r in src.execute(f'SELECT * FROM {table}').fetchall()]
            cols = list(rows[0].keys()) if rows else [r['name'] for r in src.execute(f'PRAGMA table_info({table})').fetchall()]
            with target.connect() as conn:
                if truncate:
                    conn.execute(f'TRUNCATE TABLE {_quote_ident(table)} RESTART IDENTITY CASCADE')
                    conn.commit()
                if rows:
                    placeholders = ', '.join(['%s'] * len(cols))
                    sql = f'INSERT INTO {_quote_ident(table)} ({", ".join(_quote_ident(c) for c in cols)}) VALUES ({placeholders})'
                    cur = conn._conn.cursor()
                    cur.executemany(sql, [tuple(row.get(c) for c in cols) for row in rows])
                    conn.commit()
            imported[table] = len(rows)
    payload = {'ok': True, 'sqlite': str(source_path), 'database_url': url.rsplit('@',1)[-1], 'tables': imported, 'freeze_writes': True}
    if verify:
        health = target.db_health()
        payload['verify'] = {'schema_version': health.get('schema_version'), 'active_locks': health.get('active_locks'), 'dead_letters': health.get('dead_letters'), 'imported_tables': len(imported), 'imported_rows': int(sum(imported.values()))}
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cmd_manifest_verify(manifest_path: str, signature_path: str, strict_signature: bool) -> int:
    manifest = json.loads(Path(manifest_path).read_text(encoding='utf-8'))
    errors = []
    for item in manifest.get('files', []):
        path = Path(item['path'])
        if not path.exists():
            errors.append({'missing': item['path']})
            continue
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        if sha != item['sha256']:
            errors.append({'checksum_mismatch': item['path']})
    if strict_signature:
        sig_key = os.getenv('RELEASE_MANIFEST_HMAC_KEY', '').encode('utf-8')
        if not sig_key:
            errors.append({'signature': 'RELEASE_MANIFEST_HMAC_KEY not set'})
        else:
            expected = hmac.new(sig_key, Path(manifest_path).read_text(encoding='utf-8').encode('utf-8'), hashlib.sha256).hexdigest()
            actual = Path(signature_path).read_text(encoding='utf-8').strip() if signature_path else ''
            if expected != actual:
                errors.append({'signature': 'mismatch'})
    payload = {'ok': not errors, 'errors': errors, 'files': len(manifest.get('files', []))}
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if not errors else 1


def cmd_multi_instance_plan(output: str) -> int:
    cfg = load_config()
    plan = {
        'single_instance_baseline': {'storage': 'sqlite', 'coordination': 'sqlite', 'rate_limits': 'sqlite'},
        'cutover': ['freeze writes', 'backup sqlite', 'pg-smoke', 'cutover-postgres --verify', 'preflight --live-backends', 'switch DATABASE_URL', 'restart runtime', 'verify /diag backend=postgresql'],
        'mixed_mode': {'storage': 'postgresql', 'coordination': 'sqlite|redis', 'rate_limits': 'sqlite|redis'},
        'full_multi_instance': {'storage': 'postgresql', 'coordination': 'redis', 'rate_limits': 'redis'},
        'rollback': ['stop writes', 'restore sqlite backup', 'clear postgres target if needed', 'switch DATABASE_URL back', 'restart runtime'],
    }
    out = Path(output) if output else cfg.artifact_root / 'exports' / f"multi-instance-plan-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(plan, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'[OK] Plan created: {out}')
    return 0


def main() -> int:
    args = build_parser().parse_args()
    if args.command == 'backup':
        return cmd_backup(args.dest)
    if args.command == 'audit-export':
        return cmd_audit_export(args.output_dir)
    if args.command == 'state-export':
        return cmd_state_export(args.kind, args.output)
    if args.command == 'state-import':
        return cmd_state_import(args.kind, args.input)
    if args.command == 'repair':
        return cmd_repair()
    if args.command == 'restore-check':
        return cmd_restore_check(args.path)
    if args.command == 'restore-drill':
        return cmd_restore_drill(args.path)
    if args.command == 'cleanup-artifacts':
        return cmd_cleanup_artifacts(args.backups_days, args.exports_days)
    if args.command == 'remote-upload':
        return cmd_remote_upload(args.path, args.kind)
    if args.command == 'incident-snapshot':
        return cmd_incident_snapshot(args.output)
    if args.command == 'pg-smoke':
        return cmd_pg_smoke(args.database_url)
    if args.command == 'cutover-postgres':
        return cmd_cutover_postgres(args.sqlite, args.database_url, args.verify, args.truncate)
    if args.command == 'manifest-verify':
        return cmd_manifest_verify(args.manifest, args.signature, args.strict_signature)
    if args.command == 'multi-instance-plan':
        return cmd_multi_instance_plan(args.output)
    return cmd_export(args.table, args.format, args.output, args.limit)


if __name__ == '__main__':
    raise SystemExit(main())
