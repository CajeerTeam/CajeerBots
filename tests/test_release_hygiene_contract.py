from pathlib import Path


def test_release_script_cleans_bytecode_and_copies_admin():
    script = Path("scripts/release.sh").read_text(encoding="utf-8")
    assert "__pycache__" in script
    assert "*.pyc" in script or "'*.pyc'" in script
    assert "chmod +x" in script
    assert "clean_artifacts.sh" in script
    assert " admin " in script or " admin \\" in script
    assert "configs/env/.env*.example" in script


def test_packaging_includes_admin_and_observability_files():
    manifest = Path("MANIFEST.in").read_text(encoding="utf-8")
    pyproject = Path("pyproject.toml").read_text(encoding="utf-8")
    assert "recursive-include admin" in manifest
    assert "share/cajeer-bots/admin" in pyproject
    assert "share/cajeer-bots/ops/prometheus" in pyproject
    assert "share/cajeer-bots/ops/grafana" in pyproject
