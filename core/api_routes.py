from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class RouteSpec:
    method: str
    path: str
    summary: str
    auth_scope: str = "admin"
    request_schema: dict[str, object] = field(default_factory=dict)
    response_schema: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


ROUTES: tuple[RouteSpec, ...] = (
    RouteSpec("GET", "/healthz", "Проверка процесса", "public"),
    RouteSpec("GET", "/readyz", "Проверка готовности", "public"),
    RouteSpec("GET", "/metrics", "Prometheus metrics", "metrics"),
    RouteSpec("GET", "/version", "Версии runtime и контрактов", "readonly"),
    RouteSpec("GET", "/adapters", "Список адаптеров", "readonly"),
    RouteSpec("GET", "/modules", "Список модулей", "readonly"),
    RouteSpec("GET", "/plugins", "Список плагинов", "readonly"),
    RouteSpec("GET", "/components", "Runtime-компоненты", "readonly"),
    RouteSpec("GET", "/events", "Снимок событий", "readonly"),
    RouteSpec("GET", "/routes", "Снимок router", "readonly"),
    RouteSpec("GET", "/dead-letters", "Dead letter queue", "readonly"),
    RouteSpec("GET", "/commands", "Команды", "readonly"),
    RouteSpec("GET", "/config/summary", "Безопасное резюме конфигурации", "readonly"),
    RouteSpec("GET", "/adapter-status", "Состояние адаптеров", "readonly"),
    RouteSpec("GET", "/worker-status", "Состояние worker", "readonly"),
    RouteSpec("GET", "/bridge-status", "Состояние bridge", "readonly"),
    RouteSpec("GET", "/status/dependencies", "Глубокая диагностика backend-ов", "readonly"),
    RouteSpec("GET", "/audit", "Audit trail", "readonly"),
    RouteSpec("GET", "/updates/status", "Статус update subsystem", "readonly"),
    RouteSpec("GET", "/updates/history", "История обновлений", "readonly"),
    RouteSpec("POST", "/commands/dispatch", "Отправить команду в router", "system.commands.dispatch"),
    RouteSpec("POST", "/delivery/enqueue", "Поставить сообщение в delivery queue", "system.delivery.enqueue"),
    RouteSpec("POST", "/dead-letters/retry", "Повторить dead letters", "system.events.retry"),
    RouteSpec("POST", "/events/publish", "Опубликовать событие", "system.events.publish"),
    RouteSpec("POST", "/runtime/stop", "Остановить runtime", "system.runtime.stop"),
    RouteSpec("POST", "/updates/check", "Проверить обновления", "system.update.read"),
    RouteSpec("POST", "/updates/apply", "Применить staged-обновление", "system.update.apply"),
    RouteSpec("POST", "/updates/rollback", "Откатить обновление", "system.update.rollback"),
    RouteSpec("POST", "/webhooks/telegram", "Telegram webhook", "webhook"),
    RouteSpec("POST", "/webhooks/vkontakte", "VK Callback API webhook", "webhook"),
)


def readonly_paths() -> set[str]:
    return {item.path for item in ROUTES if item.method == "GET" and item.auth_scope == "readonly"}


def openapi_document(version: str, contract: str) -> dict[str, object]:
    paths: dict[str, object] = {}
    for route in ROUTES:
        methods = paths.setdefault(route.path, {})
        methods[route.method.lower()] = {
            "summary": route.summary,
            "x-auth-scope": route.auth_scope,
            "responses": {"200": {"description": "OK"}},
        }
    return {"openapi": "3.1.0", "info": {"title": "Cajeer Bots API", "version": version, "x-contract": contract}, "paths": paths}
