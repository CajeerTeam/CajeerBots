# Архитектура Cajeer Bots

Cajeer Bots строится как универсальная платформа для нескольких транспортов и расширений. Цель архитектуры — запускать Telegram, Discord, VK и дополнительные адаптеры через единый runtime, общий event contract, delivery queue, audit и production-проверки.

## Слои

```text
core/          стабильное ядро, runtime, API, delivery, event bus, scheduler
bots/          транспортные адаптеры и platform-specific mapper'ы
modules/       официальные функциональные модули платформы
plugins/       подключаемые расширения через SDK и manifest
schemas/       JSON Schema для manifest-контрактов
alembic/       PostgreSQL schema migrations
scripts/       release, smoke, drill и операторские проверки
wiki/          эксплуатационная документация и runbooks
```

## Правила зависимостей

Обязательные правила:

```text
modules/plugins -> core.sdk.*
core -> не импортирует modules/* и plugins/*
adapters -> не импортируют modules/* и plugins/*
plugins -> не изменяют core напрямую
runtime -> собирает компоненты через Registry и ComponentManager
```

Текущий `core` ещё содержит встроенные adapter factories для штатных транспортов. Это допустимый bootstrap-контур для встроенных адаптеров, но внешние расширения должны подключаться через manifest/registry и SDK.

Проверка выполняется командой:

```bash
python3 -S scripts/check_architecture.py
```

## Runtime lifecycle

1. `Settings.from_env()` загружает `.env` автоматически, если файл есть в корне проекта.
2. `Runtime` создаёт registry, event bus, delivery, dead letters, idempotency, audit, token registry, worker и component manager.
3. `ComponentManager.start()` загружает включённые модули и плагины.
4. Плагины регистрируют API routes и scheduled jobs только при наличии нужных permissions.
5. В зависимости от target запускается API, worker, bridge или адаптеры.
6. При остановке вызываются `on_stop`/`on_disable`, адаптеры получают stop, фоновые задачи отменяются.

## Event flow

```text
Adapter/Webhook/API
  -> CajeerEvent
  -> idempotency guard
  -> event bus
  -> EventRouter
  -> command registry / modules / plugins
  -> delivery queue
  -> adapter send
  -> audit / metrics / outbound trace
```

## API flow

Production API работает через `core.asgi.create_app()` и `AsyncApiDispatcher`.

- `/livez` проверяет живость процесса.
- `/readyz` проверяет runtime, registry, storage backends и зависимости.
- `/openapi.json` публикует встроенные и plugin API routes.
- Plugin routes диспетчеризуются через `runtime.plugin_routes`.
- Scope-based auth применяется и к встроенным, и к plugin routes.

## Scheduler flow

Есть два режима scheduled jobs:

1. **Local/memory** — jobs регистрируются в `runtime.scheduler` и выполняются in-process.
2. **PostgreSQL production** — plugin jobs upsert'ятся в `shared.scheduled_jobs`, worker claim'ит due jobs через lease и `FOR UPDATE SKIP LOCKED`.

Worker выполняет:

```text
delivery.process_once()
scheduler.process_due() или runtime.scheduler.run_once()
```

## Storage modes

Поддерживаются backends:

```text
event_bus: memory / redis / postgres
delivery: memory / redis / postgres
dead_letters: memory / redis / postgres
idempotency: memory / redis / postgres
```

Production-контур должен проверяться через:

```bash
docker compose --profile integration up --build --abort-on-container-exit
```

## Release architecture

Релиз считается валидным только если он собран release pipeline, а не обычным архиватором.

```bash
scripts/release.sh
python3 -m core release verify dist/CajeerBots-<version>.zip --deep
python3 -m core release checklist --file release/checklist.yaml
```

Release artifact не должен содержать `.env`, `__pycache__`, `.pytest_cache`, `*.pyc` и должен сохранять executable-bit у entrypoints.


## Adapter/Webhook registry

`core` не импортирует конкретные Telegram/VK/Discord реализации напрямую. Runtime получает adapter class через `core.adapter_registry`, а webhook mapping — через `core.webhook_registry`. Это сохраняет правило: ядро знает только идентификатор адаптера и контракт фабрики, но не зависит от `bots.*` на уровне статических импортов.

Проверка выполняется командой:

```bash
python3 -S scripts/check_architecture.py
```

Она блокирует:
- прямые импорты `bots.*` из `core`;
- прямые импорты `modules`/`plugins` из `core`;
- импорт внутренних `core.*` API из плагинов, кроме `core.sdk.*`;
- возврат `ADAPTER_CLASSES` в `core/runtime.py`.
