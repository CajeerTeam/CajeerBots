"""Публичный SDK Cajeer Bots для модулей и плагинов.

Модули и внешние плагины должны импортировать стабильные контракты отсюда,
а не из внутренних runtime-модулей ядра.
"""
from core.sdk.events import CajeerEvent, message_event
from core.sdk.plugins import PluginBase, PluginContext, PluginRoute
from core.sdk.delivery import DeliveryMessage
from core.sdk.permissions import PermissionSet

__all__ = ["CajeerEvent", "message_event", "PluginBase", "PluginContext", "PluginRoute", "DeliveryMessage", "PermissionSet"]
