from __future__ import annotations

import importlib
from typing import Any


def import_symbol(spec: str) -> Any:
    """Лениво импортировать символ по строке ``module:attribute``."""
    if ":" not in spec:
        raise ValueError(f"entrypoint должен иметь формат module:object: {spec}")
    module_name, attr_name = spec.split(":", 1)
    module = importlib.import_module(module_name)
    value: Any = module
    for part in attr_name.split("."):
        value = getattr(value, part)
    return value
