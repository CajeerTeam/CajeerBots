from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.events import CajeerEvent
from core.imports import import_symbol


@dataclass(frozen=True)
class WebhookProviderSpec:
    adapter_id: str
    update_mapper: str | None = None
    callback_wrapper: str | None = None


WEBHOOK_PROVIDERS: dict[str, WebhookProviderSpec] = {
    "telegram": WebhookProviderSpec("telegram", update_mapper="bots.telegram.bot.mapper:update_to_event"),
    "vkontakte": WebhookProviderSpec("vkontakte", callback_wrapper="bots.vkontakte.bot.thin:VkontakteThinWrapper"),
}


def provider_ids() -> set[str]:
    return set(WEBHOOK_PROVIDERS)


def telegram_update_to_event(body: dict[str, object]) -> CajeerEvent:
    provider = WEBHOOK_PROVIDERS["telegram"]
    if provider.update_mapper is None:
        raise RuntimeError("telegram webhook mapper не настроен")
    mapper = import_symbol(provider.update_mapper)
    return mapper(body)


async def vkontakte_callback_event(token: str, body: dict[str, object]) -> CajeerEvent:
    provider = WEBHOOK_PROVIDERS["vkontakte"]
    if provider.callback_wrapper is None:
        raise RuntimeError("vkontakte webhook wrapper не настроен")
    wrapper_cls: Any = import_symbol(provider.callback_wrapper)
    wrapper = wrapper_cls(token)
    return await wrapper.callback_event(body)
