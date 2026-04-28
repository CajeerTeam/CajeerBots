#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import importlib.util
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

try:
    import redis as redis_mod  # type: ignore
except Exception:  # pragma: no cover
    redis_mod = None

try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / '.env', override=False)

from nmbot.database import LATEST_SCHEMA_VERSION
from nmbot.event_contracts import EXTERNAL_ADMIN_CONTRACT_VERSION
from nmbot.services.server_api import SecurityChallenge


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding='utf-8')


def _check_postgres(database_url: str) -> tuple[bool, str]:
    if not database_url:
        return True, 'postgres not configured'
    if not database_url.startswith(('postgres://', 'postgresql://')):
        return True, 'postgres not selected'
    if psycopg is None:
        return False, 'psycopg недоступен'
    try:
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT 1')
                cur.fetchone()
        return True, 'ok'
    except Exception as exc:  # pragma: no cover
        return False, str(exc)


def _check_redis(redis_url: str) -> tuple[bool, str]:
    if not redis_url:
        return True, 'redis not configured'
    if redis_mod is None:
        return False, 'redis package недоступен'
    try:
        client = redis_mod.Redis.from_url(redis_url, decode_responses=True)
        client.ping()
        return True, 'ok'
    except Exception as exc:  # pragma: no cover
        return False, str(exc)


def _import_smoke() -> tuple[bool, dict[str, str], list[str]]:
    checked = [
        'nmbot.storage',
        'nmbot.database',
        'nmbot.services.server_api',
    ]
    warnings: list[str] = []
    if importlib.util.find_spec('telegram') is not None:
        checked.insert(0, 'nmbot.main')
    else:
        warnings.append('telegram dependency not installed; nmbot.main import smoke skipped')
    loaded: dict[str, str] = {}
    for name in checked:
        importlib.import_module(name)
        loaded[name] = 'ok'
    probe = SecurityChallenge(challenge_id='preflight', player_name='NeverMine')
    if probe.challenge_id != 'preflight':
        raise RuntimeError('security challenge constructor smoke failed')
    return True, loaded, warnings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--production-archive', action='store_true')
    parser.add_argument('--live-backends', action='store_true')
    parser.add_argument('--verify-readme-only', action='store_true')
    args = parser.parse_args()
    report = {'ok': True, 'errors': [], 'warnings': [], 'backend_checks': {}, 'import_smoke': {}}

    readme = _read('README.md')
    build = json.loads(_read('build_info.json'))
    pyproject = _read('pyproject.toml')
    versions = {
        'README': re.search(r'NMTelegramBot\s+([0-9.]+)', readme).group(1),
        'build_info': str(build.get('version', '')),
        'pyproject': re.search(r'version = "([0-9.]+)"', pyproject).group(1),
    }
    if len(set(versions.values())) != 1:
        report['errors'].append({'version_mismatch': versions})

    build_schema = int(build.get('schema_version', -1))
    if build_schema != LATEST_SCHEMA_VERSION:
        report['errors'].append({'schema_version_mismatch': {'build_info': build_schema, 'code': LATEST_SCHEMA_VERSION}})
    required_schema = int(build.get('compatibility', {}).get('required_schema_version', -1))
    if required_schema != LATEST_SCHEMA_VERSION:
        report['errors'].append({'required_schema_version_mismatch': {'build_info': required_schema, 'code': LATEST_SCHEMA_VERSION}})

    expected_admin_contract = str(EXTERNAL_ADMIN_CONTRACT_VERSION)
    actual_admin_contract = str(build.get('compatibility', {}).get('external_admin_contract_version', ''))
    if actual_admin_contract != expected_admin_contract:
        report['errors'].append({'external_admin_contract_version_mismatch': {'build_info': actual_admin_contract, 'code': expected_admin_contract}})

    required = ['README.md', 'build_info.json', 'release_build.py', 'db_tools.py']
    for rel in required:
        if not (ROOT / rel).exists():
            report['errors'].append({'missing_file': rel})

    forbidden_refs = ['ops/OPERATIONS.md', 'ops/RELEASE_MODES.md', 'ops/MULTI_INSTANCE_MIGRATION.md', 'ops/EXTERNAL_ADMIN_CONTRACT.md', 'qa_suite', 'qa_fake_telegram']
    for marker in forbidden_refs:
        if marker in readme:
            report['errors'].append({'readme_forbidden_reference': marker})

    if not args.verify_readme_only:
        for cmd in [('bash', '-n', 'run.sh'), ('bash', '-n', 'bootstrap.sh')]:
            subprocess.run(cmd, cwd=ROOT, check=True)
        subprocess.run([sys.executable, '-m', 'compileall', '-q', 'nmbot', 'main.py', 'db_tools.py', 'setup_wizard.py', 'preflight_check.py', 'qa_smoke.py', 'qa_contract_nm_auth.py'], cwd=ROOT, check=True)
        ok, loaded, import_warnings = _import_smoke()
        report['import_smoke'] = loaded
        report['warnings'].extend(import_warnings)
        if not ok:
            report['errors'].append({'import_smoke': 'failed'})

    if args.production_archive and not (ROOT / '.env').exists():
        report['warnings'].append('production archive has no .env')

    bot_mode = os.getenv('BOT_MODE', '').strip().lower()
    webhook_url = (os.getenv('WEBHOOK_URL', '').strip() or os.getenv('PUBLIC_HTTP_SERVER_URL', '').strip())
    webhook_listen = os.getenv('WEBHOOK_LISTEN', '').strip()
    webhook_port = (os.getenv('WEBHOOK_PORT', '').strip() or os.getenv('PORT', '').strip())
    if bot_mode == 'webhook':
        if not webhook_url:
            report['errors'].append({'bothost_webhook': 'WEBHOOK_URL/PUBLIC_HTTP_SERVER_URL is required for webhook mode'})
        elif not webhook_url.startswith('https://'):
            report['errors'].append({'bothost_webhook': 'BotHost Telegram webhook must use HTTPS public URL'})
        if webhook_listen in {'127.0.0.1', 'localhost'}:
            report['warnings'].append('WEBHOOK_LISTEN is local-only; BotHost web app should listen on 0.0.0.0')
        if webhook_port and webhook_port != '8080':
            report['warnings'].append(f'WEBHOOK_PORT/PORT is {webhook_port}; current BotHost app port is expected to be 8080')
    is_bothost_profile = 'bothost' in webhook_url.lower() or 'bothost' in os.getenv('DOMAIN', '').lower()
    if is_bothost_profile and os.getenv('DATA_DIR', '').strip() and os.getenv('DATA_DIR', '').strip() != '/app/data':
        report['warnings'].append('DATA_DIR differs from BotHost persistent storage path /app/data')
    if is_bothost_profile and os.getenv('SHARED_DIR', '').strip() and os.getenv('SHARED_DIR', '').strip() != '/app/shared':
        report['warnings'].append('SHARED_DIR differs from BotHost shared storage path /app/shared')

    database_url = os.getenv('DATABASE_URL', '').strip()
    redis_url = os.getenv('REDIS_URL', '').strip()
    if args.live_backends:
        ok, msg = _check_postgres(database_url)
        report['backend_checks']['postgresql'] = {'ok': ok, 'message': msg}
        if not ok:
            report['errors'].append({'postgresql': msg})
        ok, msg = _check_redis(redis_url)
        report['backend_checks']['redis'] = {'ok': ok, 'message': msg}
        if not ok:
            report['errors'].append({'redis': msg})
    else:
        if database_url.startswith(('postgres://', 'postgresql://')):
            report['warnings'].append('DATABASE_URL указывает на PostgreSQL; для live-проверки добавь --live-backends или db_tools.py pg-smoke')
        if redis_url:
            report['warnings'].append('REDIS_URL задан; для live-проверки Redis добавь --live-backends')

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if not report['errors'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
