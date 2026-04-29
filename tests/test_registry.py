from pathlib import Path

from core.config import Settings
from core.registry import Dependency, Registry


def test_registry_loads_default_manifests(monkeypatch):
    monkeypatch.setenv("MODULES_ENABLED", "identity,rbac,logs,bridge")
    monkeypatch.setenv("PLUGINS_ENABLED", "example_plugin")
    registry = Registry(Path.cwd())
    settings = Settings.from_env()
    assert {item.id for item in registry.adapters()} == {"telegram", "discord", "vkontakte"}
    assert "bridge" in {item.id for item in registry.modules()}
    assert registry.validate(settings=settings) == []


def test_plugin_dependency_is_namespaced():
    registry = Registry(Path.cwd())
    plugin = next(item for item in registry.plugins() if item.id == "example_plugin")
    assert "module:bridge" in plugin.requires
    assert Dependency.parse("module:bridge").normalized() == "module:bridge"


def test_registry_has_load_order():
    registry = Registry(Path.cwd())
    order = [item.key() for item in registry.load_order()]
    assert order.index("module:identity") < order.index("module:rbac")
    assert order.index("module:logs") < order.index("module:bridge")
