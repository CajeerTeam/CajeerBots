# Cajeer Bots

**Cajeer Bots** — универсальная русскоязычная платформа для запуска, управления и расширения ботов в разных мессенджерах и сервисах.

## Архитектурные правила

1. Local mode — основной режим по умолчанию. Он запускает всех ботов сразу или каждый адаптер отдельно.
2. Distributed mode — дополнительный функционал. Он не требуется для обычного запуска.
3. Telegram, Discord и ВКонтакте — транспортные адаптеры, а не отдельные продукты.
4. Общая логика находится в `modules` и `plugins`.
5. Все пользовательские тексты, документация, CLI-описания и примеры должны быть на русском языке.
6. PostgreSQL используется как единая база данных платформы. Встроенные миграции не поставляются: схема управляется внешним эксплуатационным слоем по контракту из GitHub Wiki.
7. Межботовое взаимодействие строится вокруг единого контракта событий и шины событий.
8. Каждый адаптер может запускаться отдельно через `cajeer-bots run <adapter>`, `python -m core run <adapter>` или standalone-пакет `bot` внутри каталога адаптера.
9. Основная документация готовится для GitHub Wiki в каталоге `wiki/`.

## Быстрый старт

```bash
cp .env.example .env
./scripts/install.sh
./scripts/doctor.sh --offline
./scripts/run.sh all
```

После установки предпочтительный пользовательский интерфейс — console script:

```bash
cajeer-bots run all
cajeer-bots run telegram
cajeer-bots doctor --offline
cajeer-bots modules
cajeer-bots plugins
```

`python -m core ...` остаётся техническим режимом для разработки и аварийного запуска без установки пакета.

## Local mode

`CAJEER_BOTS_MODE=local` — базовый режим. Цель запуска задаётся CLI-командой или `CAJEER_BOTS_DEFAULT_TARGET`.

```bash
cajeer-bots run all
cajeer-bots run telegram
cajeer-bots run discord
cajeer-bots run vkontakte
cajeer-bots run worker
cajeer-bots run api
cajeer-bots run bridge
```

## Distributed mode

Distributed mode выключен по умолчанию и не влияет на local mode.

```env
DISTRIBUTED_ENABLED=false
```

Каркас distributed mode находится в `distributed/`: протоколы событий, команд, ack, heartbeat, node security, Runtime Agent и Core Server primitives.

## HTTP API

Минимальный API-режим доступен без внешних веб-фреймворков:

```bash
cajeer-bots run api
```

Публичные маршруты:

```text
GET /healthz
GET /readyz
```

`/metrics` публичен только при `METRICS_PUBLIC=true`, иначе требует `API_TOKEN_METRICS` или `API_TOKEN`.

Административные и диагностические маршруты требуют `Authorization: Bearer <API_TOKEN>` или read-only токен, если маршрут только на чтение.

```text
GET  /version
GET  /adapters
GET  /modules
GET  /plugins
GET  /events
GET  /routes
GET  /dead-letters
GET  /commands
GET  /config/summary
GET  /adapter-status
GET  /worker-status
GET  /bridge-status
GET  /status/dependencies
POST /commands/dispatch
POST /delivery/enqueue
POST /dead-letters/retry
POST /events/publish
POST /runtime/stop
```

## Структура

```text
CajeerBots/
├── core/         # ядро платформы, CLI, runtime, конфигурация, события, registry
├── bots/         # адаптеры Telegram, Discord и ВКонтакте
├── modules/      # официальные модули платформы
├── plugins/      # расширения платформы
├── distributed/  # дополнительный распределённый режим
├── scripts/      # install/run/doctor/release
├── ops/          # примеры systemd/nginx/docker
└── wiki/         # страницы для GitHub Wiki
```

## Документация

Основная документация находится в GitHub Wiki. Исходники страниц лежат в каталоге `wiki/`.
