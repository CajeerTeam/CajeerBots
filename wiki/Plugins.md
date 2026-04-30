# Плагины

Плагин — подключаемое расширение Cajeer Bots, которое не меняет ядро и взаимодействует с платформой через `core.sdk`.

## Структура

```text
plugins/<id>/
├── plugin.json
├── runtime.py
└── README.md
```

Минимальный manifest:

```json
{
  "id": "example_api_route",
  "name": "Пример API route-плагина",
  "version": "1.0.0",
  "type": "plugin",
  "entrypoint": "runtime:ExampleApiRoutePlugin",
  "permissions": ["api.route.register", "config.read"],
  "compatibility": {
    "db_contract": "cajeer.bots.db.v1",
    "event_contract": "cajeer.bots.event.v1",
    "platform": ">=0.10,<1.0"
  }
}
```

Проверка:

```bash
python3 -m core plugins --validate plugins/example_api_route/plugin.json
```

## Permissions

Permissions теперь являются runtime gate, а не только metadata.

| Permission | Что разрешает |
|---|---|
| `events.read` | получать события в `on_event`/`on_command` |
| `events.publish` | публиковать события через `context.events.publish()` |
| `delivery.enqueue` | ставить сообщения в delivery queue |
| `api.route.register` | регистрировать API routes |
| `scheduler.jobs.register` | регистрировать scheduled jobs |
| `audit.write` | писать audit от имени плагина |
| `config.read` | читать безопасное резюме конфигурации |

Если permission отсутствует, ComponentManager помечает компонент как failed или API dispatcher возвращает `403`.

## API routes

Плагин может зарегистрировать route:

```python
from core.sdk import PluginBase
from core.sdk.plugins import PluginRoute

class ExampleApiRoutePlugin(PluginBase):
    def register_api_routes(self, context):
        return [PluginRoute("GET", "/plugins/example-api-route", "Пример route", "system.read")]

    async def handle_api_route(self, request, context):
        return {"ok": True, "plugin": self.id}
```

После запуска компонента route появляется в:

```text
GET /routes
GET /openapi.json
```

И вызывается как обычный endpoint с проверкой scope.

## Scheduled jobs

Плагин может зарегистрировать задачу:

```python
class ExampleSchedulerPlugin(PluginBase):
    def register_scheduled_jobs(self, context):
        return [{
            "name": "example.tick",
            "interval_seconds": 60,
            "job_type": "event.publish",
            "payload": {
                "source": "example_scheduler",
                "type": "plugin.example_scheduler.tick",
                "payload": {"plugin": self.id}
            }
        }]
```

В local mode задача выполняется in-process. В production при наличии `DATABASE_ASYNC_URL` задача дополнительно upsert'ится в `scheduled_jobs`, чтобы её мог claim'ить worker.

## Lifecycle hooks

Поддерживаемые hooks:

```text
on_install
on_enable
on_disable
on_upgrade
on_uninstall
on_start
on_event
on_command
on_stop
register_api_routes
register_scheduled_jobs
```

## Контракт SDK

Плагины должны импортировать только из `core.sdk`:

```python
from core.sdk import CajeerEvent, PluginBase
from core.sdk.plugins import PluginRoute, PluginRequest
```

Не рекомендуется импортировать внутренние модули `core.runtime`, `core.delivery`, `core.event_bus` напрямую: такие импорты не считаются стабильным контрактом.


## Import policy и signed catalog

Плагины считаются переносимыми только если импортируют публичный SDK:

```python
from core.sdk import PluginBase, CajeerEvent
from core.sdk.plugins import PluginRoute
```

Прямые импорты `core.runtime`, `core.delivery`, `bots.*`, `modules.*` и `distributed.*` блокируются `scripts/check_architecture.py` и `cajeer-bots plugins --validate`.

Runtime catalog поддерживает подпись записей через HMAC SHA-256. Для проверяемого каталога задайте:

```env
PLUGIN_CATALOG_SIGNING_SECRET=<long-random-secret>
```

Поле `signature` в `catalog.lock` проверяется вместе с `sha256`. Для локальных доверенных плагинов допускаются `source=local` или `source=manual`, но для стороннего каталога должны использоваться `sha256` и `signature`.
