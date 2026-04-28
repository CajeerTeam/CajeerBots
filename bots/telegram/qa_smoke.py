#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import tempfile
from pathlib import Path

from nmbot.config import load_config
from nmbot.services.server_api import SecurityChallenge, ServerStatusClient
from nmbot.storage import create_database


def _import_smoke() -> tuple[list[str], list[str]]:
    imported: list[str] = []
    warnings: list[str] = []
    modules = [
        'nmbot.storage',
        'nmbot.database',
        'nmbot.services.server_api',
    ]
    if importlib.util.find_spec('telegram') is not None:
        modules.insert(0, 'nmbot.main')
    else:
        warnings.append('telegram dependency not installed; nmbot.main import smoke skipped')
    for name in modules:
        __import__(name)
        imported.append(name)
    return imported, warnings


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--live-backends', action='store_true')
    args = parser.parse_args()

    imported, warnings = _import_smoke()
    cfg = load_config()
    backend = 'sqlite'

    if not args.live_backends:
        tmpdir = Path(tempfile.mkdtemp(prefix='nmtg-qa-db-'))
        cfg.database_url = ''
        cfg.sqlite_path = tmpdir / 'qa_smoke.db'
        db = create_database(cfg)
        health = db.db_health()
        db.set_runtime_value('qa_smoke_last_run_at', '1')
        shadow_lock = bool(db.acquire_leader_lock(name='qa_smoke', owner='qa', ttl_seconds=15))
        with tempfile.TemporaryDirectory(prefix='nmtg-smoke-') as tmp:
            snap = Path(tmp) / 'snapshot.json'
            snap.write_text(json.dumps({'health': health}, ensure_ascii=False), encoding='utf-8')
            snapshot_written = snap.exists()
    else:
        db = create_database(cfg)
        health = db.db_health()
        db.set_runtime_value('qa_smoke_last_run_at', '1')
        shadow_lock = bool(db.acquire_leader_lock(name='qa_smoke', owner='qa', ttl_seconds=15))
        snapshot_written = True
        backend = getattr(db, 'backend_name', 'sqlite')

    challenge = SecurityChallenge(challenge_id='qa', player_name='NeverMine')
    status_client = ServerStatusClient(
        cfg.server_status_url,
        cfg.request_timeout_seconds,
        cache_ttl_seconds=cfg.status_cache_seconds,
        retry_attempts=cfg.status_retry_attempts,
        retry_backoff_seconds=cfg.status_retry_backoff_seconds,
        bearer_token=cfg.server_api_bearer_token,
        hmac_secret=cfg.server_api_hmac_secret,
        request_id_header=cfg.server_api_request_id_header,
        announcement_feed_url=cfg.announcement_feed_url,
        link_verify_url=cfg.link_verify_url,
        strict_schemas=cfg.strict_api_schemas,
        security_status_url=cfg.security_status_url,
        security_challenges_url=cfg.security_challenges_url,
        security_2fa_action_url=cfg.security_2fa_action_url,
        security_recovery_url=cfg.security_recovery_url,
        security_sessions_url=cfg.security_sessions_url,
        security_session_action_url=cfg.security_session_action_url,
        circuit_threshold=cfg.external_api_circuit_threshold,
        circuit_reset_seconds=cfg.external_api_circuit_reset_seconds,
    )

    payload = {
        'config_ok': True,
        'backend': getattr(db, 'backend_name', backend),
        'schema': health.get('schema_version'),
        'import_smoke': imported,
        'runtime_value_written': db.runtime_value('qa_smoke_last_run_at', '') == '1',
        'lock_claimed': shadow_lock,
        'snapshot_written': snapshot_written,
        'security_challenge_ctor': bool(challenge.challenge_id == 'qa' and challenge.player_name == 'NeverMine'),
        'security_configured': status_client.security_is_configured(),
        'warnings': warnings,
    }
    print(payload)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
