from __future__ import annotations

import os
import subprocess
import sys


def test_integration_smoke_script_is_safe_without_external_services() -> None:
    env = {
        **os.environ,
        "REDIS_URL": "",
        "DATABASE_ASYNC_URL": "",
        "EVENT_SIGNING_SECRET": "test-event-secret",
        "API_TOKEN": "test-api-token",
        "CAJEER_BOTS_ENV": "test",
        "TELEGRAM_ENABLED": "false",
        "DISCORD_ENABLED": "false",
        "VKONTAKTE_ENABLED": "false",
        "FAKE_ENABLED": "true",
    }
    result = subprocess.run(
        ["bash", "scripts/smoke_integrations.sh"],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout
