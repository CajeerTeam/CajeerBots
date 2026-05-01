from __future__ import annotations

from core.commands import build_default_commands


def test_support_command_defers_granular_rbac_to_support_module():
    command = build_default_commands().get("support")
    assert command is not None
    assert command.permission is None
