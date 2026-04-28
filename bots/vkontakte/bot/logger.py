from __future__ import annotations

import logging
from typing import Any

from nmbot.remote_log_handler import RemoteLogHandler

_REMOTE_HANDLER: RemoteLogHandler | None = None


def _level_value(level: str) -> int:
    return getattr(logging, str(level).upper(), logging.INFO)


def configure_logging(config: Any) -> None:
    global _REMOTE_HANDLER

    level_name = getattr(config, 'log_level', config)
    level_value = _level_value(str(level_name))

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level_value)

    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s')
    stream = logging.StreamHandler()
    stream.setLevel(level_value)
    stream.setFormatter(formatter)
    root.addHandler(stream)

    _REMOTE_HANDLER = None
    if isinstance(config, str):
        return

    if not bool(getattr(config, 'remote_logs_enabled', False)):
        return

    url = str(getattr(config, 'remote_logs_url', '') or '').strip()
    token = str(getattr(config, 'remote_logs_token', '') or '').strip()
    if not url or not token:
        logging.getLogger(__name__).warning(
            'Remote logs requested but REMOTE_LOGS_URL/REMOTE_LOGS_TOKEN is not fully configured'
        )
        return

    remote_handler = RemoteLogHandler(
        url=url,
        token=token,
        project=str(getattr(config, 'remote_logs_project', 'NeverMine') or 'NeverMine'),
        bot=str(getattr(config, 'remote_logs_bot', 'NMVKBot') or 'NMVKBot'),
        environment=str(getattr(config, 'remote_logs_environment', 'production') or 'production'),
        batch_size=int(getattr(config, 'remote_logs_batch_size', 25) or 25),
        flush_interval=float(getattr(config, 'remote_logs_flush_interval', 5.0) or 5.0),
        timeout=float(getattr(config, 'remote_logs_timeout', 3.0) or 3.0),
        level=_level_value(str(getattr(config, 'remote_logs_level', 'INFO') or 'INFO')),
        sign_requests=bool(getattr(config, 'remote_logs_sign_requests', False)),
        spool_dir=(str(getattr(config, 'remote_logs_spool_dir', '') or '').strip() or None),
        max_spool_files=int(getattr(config, 'remote_logs_max_spool_files', 200) or 200),
    )
    remote_handler.setFormatter(formatter)
    root.addHandler(remote_handler)
    _REMOTE_HANDLER = remote_handler
    logging.getLogger(__name__).info('Remote logs enabled: %s', url)


def get_remote_logs_diagnostics() -> dict[str, Any]:
    if _REMOTE_HANDLER is None:
        return {'enabled': False}
    data = _REMOTE_HANDLER.diagnostics()
    data['enabled'] = True
    data['url'] = _REMOTE_HANDLER.url
    data['project'] = _REMOTE_HANDLER.project
    data['bot'] = _REMOTE_HANDLER.bot
    data['environment'] = _REMOTE_HANDLER.environment
    return data
