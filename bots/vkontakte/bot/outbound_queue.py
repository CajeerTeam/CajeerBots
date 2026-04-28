from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any

from nmbot.bridge import prepare_discord_request, send_prepared_request
from nmbot.storage import Storage

LOGGER = logging.getLogger(__name__)


class OutboundDeliveryService:
    def __init__(self, settings: Any, storage: Storage) -> None:
        self.settings = settings
        self.storage = storage
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._worker, name='NMVKBotOutbound', daemon=True)
        self._thread.start()
        LOGGER.info('Outbound delivery worker started')

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        LOGGER.info('Outbound delivery worker stopped')

    def send_or_queue(self, event: dict[str, Any]) -> str:
        prepared = prepare_discord_request(self.settings, event)
        if prepared is None:
            LOGGER.debug('Discord outbound disabled for event %s', event.get('event_id'))
            return 'skipped'
        try:
            send_prepared_request(prepared['url'], prepared['body_json'], prepared['headers'], prepared['timeout'])
            return 'sent'
        except Exception as exc:
            LOGGER.warning('Immediate outbound delivery failed for %s: %s', event.get('event_id'), exc)
            self.storage.enqueue_outbound(
                event_id=str(event.get('event_id') or ''),
                target_url=prepared['url'],
                body_json=prepared['body_json'],
                headers_json=prepared['headers_json'],
            )
            return 'queued'

    def _write_dead_letter(self, record: Any, reason: str) -> str:
        base = Path(self.settings.shared_path('dead-letter', 'outbound'))
        base.mkdir(parents=True, exist_ok=True)
        path = base / f"{int(time.time())}-{record.event_id}.json"
        payload = {
            'row_id': record.row_id,
            'event_id': record.event_id,
            'target_url': record.target_url,
            'body_json': record.body_json,
            'headers_json': record.headers_json,
            'attempts': record.attempts,
            'reason': reason,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return str(path)

    def _worker(self) -> None:
        interval = int(getattr(self.settings, 'outbound_worker_interval_seconds', 5) or 5)
        base_delay = int(getattr(self.settings, 'outbound_retry_base_seconds', 10) or 10)
        max_delay = int(getattr(self.settings, 'outbound_retry_max_seconds', 300) or 300)
        timeout = int(getattr(self.settings, 'bridge_timeout_seconds', 5) or 5)
        max_attempts = int(getattr(self.settings, 'outbound_max_attempts', 8) or 8)
        while not self._stop.is_set():
            try:
                records = self.storage.fetch_due_outbound()
                for record in records:
                    try:
                        send_prepared_request(record.target_url, record.body_json, record.headers_json, timeout)
                        self.storage.mark_outbound_success(record.row_id)
                    except Exception as exc:
                        attempts = record.attempts + 1
                        http_status = getattr(getattr(exc, 'response', None), 'status_code', None)
                        if attempts >= max_attempts:
                            dead_path = self._write_dead_letter(record, str(exc))
                            self.storage.mark_outbound_dead(
                                record.row_id,
                                attempts=attempts,
                                reason=str(exc),
                                http_status=http_status,
                                dead_letter_path=dead_path,
                            )
                            LOGGER.error('Outbound event %s moved to dead-letter after %s attempts', record.event_id, attempts)
                            continue
                        delay = min(base_delay * (2 ** max(0, attempts - 1)), max_delay)
                        self.storage.mark_outbound_failure(
                            record.row_id,
                            attempts=attempts,
                            delay_seconds=delay,
                            error=str(exc),
                            http_status=http_status,
                        )
                        LOGGER.warning('Retry failed for outbound event %s: %s', record.event_id, exc)
            except Exception:
                LOGGER.exception('Outbound worker loop failed')
            self._stop.wait(interval)
