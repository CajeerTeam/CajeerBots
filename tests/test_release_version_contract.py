from __future__ import annotations

from pathlib import Path

from core.versioning import version_consistency_errors


def test_project_version_sources_are_consistent():
    assert version_consistency_errors(Path.cwd()) == []
