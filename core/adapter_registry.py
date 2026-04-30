from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from core.imports import import_symbol

if TYPE_CHECKING:  # pragma: no cover
    from core.adapters.base import BotAdapter


@dataclass(frozen=True)
class AdapterFactorySpec:
    adapter_id: str
    class_path: str


ADAPTER_FACTORY_SPECS: dict[str, AdapterFactorySpec] = {
    "telegram": AdapterFactorySpec("telegram", "core.adapters.telegram:TelegramAdapter"),
    "discord": AdapterFactorySpec("discord", "core.adapters.discord:DiscordAdapter"),
    "vkontakte": AdapterFactorySpec("vkontakte", "core.adapters.vkontakte:VkontakteAdapter"),
    "fake": AdapterFactorySpec("fake", "core.adapters.fake:FakeAdapter"),
}


def adapter_ids() -> set[str]:
    return set(ADAPTER_FACTORY_SPECS)


def load_adapter_class(adapter_id: str) -> type["BotAdapter"]:
    spec = ADAPTER_FACTORY_SPECS.get(adapter_id)
    if spec is None:
        raise KeyError(f"неизвестный adapter id: {adapter_id}")
    return import_symbol(spec.class_path)
