from __future__ import annotations

import inspect

from core.adapters.telegram import TelegramAdapter


def test_telegram_dispatcher_builder_does_not_collide_with_instance_state():
    source = inspect.getsource(TelegramAdapter)
    assert "def _build_dispatcher" in source
    assert "_dispatcher_instance" in source
    assert "self._dispatcher()" not in source
    assert "self._dispatcher =" not in source
