import json
from pathlib import Path


def test_plugin_package_schema_declares_signed_package_contract():
    schema = json.loads(Path("schemas/plugin-package.schema.json").read_text(encoding="utf-8"))
    assert "signature" in schema["properties"]
    assert "capabilities" in schema["required"]
    assert "artifacts" in schema["required"]
    assert "compatibility" in schema["properties"]
    assert "migrations" in schema["properties"]


def test_catalog_has_enable_disable_lifecycle_hooks():
    catalog = Path("core/catalog.py").read_text(encoding="utf-8")
    assert "enable_plugin" in catalog
    assert "disable_plugin" in catalog
    assert "plugin-package.schema.json" in catalog or "package" in catalog.lower()
