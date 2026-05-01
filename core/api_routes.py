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
    handler_id: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


ROUTES: tuple[RouteSpec, ...] = (
    RouteSpec("GET", "/livez", "Проверка живости процесса", "public"),
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
    RouteSpec("GET", "/admin", "Минимальная веб-панель управления", "system.read"),
    RouteSpec("GET", "/admin/app.js", "JS веб-панели", "system.read"),
    RouteSpec("GET", "/admin/style.css", "CSS веб-панели", "system.read"),
    RouteSpec("POST", "/commands/dispatch", "Отправить команду в router", "system.commands.dispatch", request_schema={"required": ["command"], "properties": {"command": "str", "payload": "dict"}}),
    RouteSpec("POST", "/delivery/enqueue", "Поставить сообщение в delivery queue", "system.delivery.enqueue", request_schema={"required": ["adapter", "target", "text"], "properties": {"adapter": "str", "target": "str", "text": "str", "max_attempts": "int", "trace_id": "str"}}),
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


def route_key(method: str, path: str) -> str:
    return f"{method.upper()} {path}"


ROUTE_INDEX = {route_key(item.method, item.path): item for item in ROUTES}


def route_for(method: str, path: str) -> RouteSpec | None:
    return ROUTE_INDEX.get(route_key(method, path))


def readonly_paths() -> set[str]:
    return {item.path for item in ROUTES if item.method == "GET" and item.auth_scope in {"system.read", "system.update.read"}}


def _route_attr(route: object, name: str, default: str = "") -> str:
    value = getattr(route, name, None)
    if value is None and getattr(route, "route", None) is not None:
        value = getattr(getattr(route, "route"), name, None)
    return str(value if value is not None else default)


def openapi_document(version: str, contract: str, plugin_routes: object | None = None) -> dict[str, object]:
    paths: dict[str, object] = {}
    for route in ROUTES:
        methods = paths.setdefault(route.path, {})
        methods[route.method.lower()] = {
            "summary": route.summary,
            "x-auth-scope": route.auth_scope,
            "operationId": route.handler_id or (route.method.lower() + "_" + route.path.strip("/").replace("/", "_")),
            "responses": {"200": {"description": "OK"}},
        }
    for route in list(plugin_routes or []):
        path = _route_attr(route, "path")
        method = _route_attr(route, "method", "GET").lower()
        if not path:
            continue
        methods = paths.setdefault(path, {})
        plugin_id = _route_attr(route, "plugin_id", "external")
        methods[method] = {
            "summary": _route_attr(route, "summary", "Plugin route"),
            "x-auth-scope": _route_attr(route, "auth_scope", "system.admin"),
            "x-plugin-id": plugin_id,
            "operationId": "plugin_" + plugin_id.replace("-", "_") + "_" + method + "_" + path.strip("/").replace("/", "_").replace("-", "_"),
            "responses": {"200": {"description": "OK"}},
        }
    return {"openapi": "3.1.0", "info": {"title": "Cajeer Bots API", "version": version, "x-contract": contract}, "paths": paths, "x-known-scopes": sorted(KNOWN_SCOPES)}


def handler_registry_key(method: str, path: str) -> str:
    route = route_for(method, path)
    if route is None:
        return ""
    return route.handler_id or (route.method.lower() + "_" + route.path.strip("/").replace("/", "_").replace("-", "_"))


def validate_request_body(method: str, path: str, body: dict[str, object]) -> list[str]:
    route = route_for(method, path)
    if route is None or not route.request_schema:
        return []
    errors: list[str] = []
    required = route.request_schema.get("required", [])
    if isinstance(required, list):
        for key in required:
            if str(key) not in body or body.get(str(key)) in {None, ""}:
                errors.append(f"{key} обязателен")
    properties = route.request_schema.get("properties", {})
    if isinstance(properties, dict):
        for key, expected in properties.items():
            if key not in body or body[key] is None:
                continue
            value = body[key]
            if expected == "str" and not isinstance(value, str):
                errors.append(f"{key} должен быть строкой")
            elif expected == "dict" and not isinstance(value, dict):
                errors.append(f"{key} должен быть объектом")
            elif expected == "int" and not isinstance(value, int):
                errors.append(f"{key} должен быть числом")
            elif expected == "bool" and not isinstance(value, bool):
                errors.append(f"{key} должен быть boolean")
    return errors
