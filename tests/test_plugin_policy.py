from __future__ import annotations

from pathlib import Path

from core.plugin_policy import validate_plugin_import_policy


def test_builtin_plugins_use_public_sdk_only():
    for manifest in sorted(Path("plugins").glob("*/plugin.json")):
        result = validate_plugin_import_policy(manifest.parent)
        assert result.ok, result.errors
