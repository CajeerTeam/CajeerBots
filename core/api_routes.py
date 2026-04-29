from __future__ import annotations

from dataclasses import asdict, dataclass, field

KNOWN_SCOPES = {
    "public",
    "webhook",
    "system.read",
    "system.admin",
    "system.metrics",
    "system.commands.dispatch",
    "system.delivery.enqueue",
    "system.events.publish",
    "system.events.retry",
    "system.runtime.stop",
    "system.update.read",
    "system.update.apply",
    "system.update.rollback",
}

SCOPE_ALIASES = {
    "readonly": "system.read",
    "metrics": "system.metrics",
    "admin": "system.admin",
}


@dataclass(frozen=True)
class RouteSpec:
    method: str
    path: str
    summary: str
    auth_scope: str = "system.admin"
    request_schema: dict[str, object] = field(default_factory=dict)
    response_schema: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


ROUTES: tuple[RouteSpec, ...] = (
    RouteSpec("GET", "/healthz", "Проверка процесса", "public"),
    RouteSpec("GET", "/readyz", "Проверка готовности", "public"),
    RouteSpec("GET", "/metrics", "Prometheus metrics", "system.metrics"),
    RouteSpec("GET", "/version", "Версии runtime и контрактов", "system.read"),
    RouteSpec("GET", "/adapters", "Список адаптеров", "system.read"),
    RouteSpec("GET", "/modules", "Список модулей", "system.read"),
    RouteSpec("GET", "/plugins", "Список плагинов", "system.read"),
    RouteSpec("GET", "/components", "Runtime-компоненты", "system.read"),
    RouteSpec("GET", "/events", "Снимок событий", "system.read"),
    RouteSpec("GET", "/routes", "Снимок router", "system.read"),
    RouteSpec("GET", "/dead-letters", "Dead letter queue", "system.read"),
    RouteSpec("GET", "/commands", "Команды", "system.read"),
    RouteSpec("GET", "/config/summary", "Безопасное резюме конфигурации", "system.read"),
    RouteSpec("GET", "/adapter-status", "Состояние адаптеров", "system.read"),
    RouteSpec("GET", "/worker-status", "Состояние worker", "system.read"),
    RouteSpec("GET", "/bridge-status", "Состояние bridge", "system.read"),
    RouteSpec("GET", "/status/dependencies", "Глубокая диагностика backend-ов", "system.read"),
    RouteSpec("GET", "/audit", "Audit trail", "system.read"),
    RouteSpec("GET", "/updates/status", "Статус update subsystem", "system.update.read"),
    RouteSpec("GET", "/updates/plan", "План обновления для Cajeer Workspace", "system.update.read"),
    RouteSpec("GET", "/updates/history", "История обновлений", "system.update.read"),
    RouteSpec("POST", "/commands/dispatch", "Отправить команду в router", "system.commands.dispatch"),
    RouteSpec("POST", "/delivery/enqueue", "Поставить сообщение в delivery queue", "system.delivery.enqueue"),
    RouteSpec("POST", "/dead-letters/retry", "Повторить dead letters", "system.events.retry"),
    RouteSpec("POST", "/events/publish", "Опубликовать событие", "system.events.publish"),
    RouteSpec("POST", "/runtime/stop", "Остановить runtime", "system.runtime.stop"),
    RouteSpec("POST", "/updates/check", "Проверить обновления", "system.update.read"),
    RouteSpec("POST", "/updates/plan", "Построить план обновления", "system.update.read"),
    RouteSpec("POST", "/updates/apply", "Применить staged/latest-обновление", "system.update.apply"),
    RouteSpec("POST", "/updates/rollback", "Откатить обновление", "system.update.rollback"),
    RouteSpec("POST", "/webhooks/telegram", "Telegram webhook", "webhook"),
    RouteSpec("POST", "/webhooks/vkontakte", "VK Callback API webhook", "webhook"),
)


def canonical_scope(scope: str) -> str:
    return SCOPE_ALIASES.get(scope, scope)


def readonly_paths() -> set[str]:
    return {item.path for item in ROUTES if item.method == "GET" and item.auth_scope in {"system.read", "system.update.read"}}


def openapi_document(version: str, contract: str) -> dict[str, object]:
    paths: dict[str, object] = {}
    for route in ROUTES:
        methods = paths.setdefault(route.path, {})
        methods[route.method.lower()] = {
            "summary": route.summary,
            "x-auth-scope": route.auth_scope,
            "responses": {"200": {"description": "OK"}},
        }
    return {"openapi": "3.1.0", "info": {"title": "Cajeer Bots API", "version": version, "x-contract": contract}, "paths": paths, "x-known-scopes": sorted(KNOWN_SCOPES)}
