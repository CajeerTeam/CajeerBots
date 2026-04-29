# Cajeer Bots

**Cajeer Bots** — универсальная русскоязычная платформа для запуска, управления и расширения ботов в разных мессенджерах и сервисах.

## Архитектурные правила

1. Telegram, Discord и ВКонтакте — транспортные адаптеры, а не отдельные продукты.
2. Общая логика находится в `modules` и `plugins`.
3. Все пользовательские тексты, документация, CLI-описания и примеры должны быть на русском языке.
4. PostgreSQL используется как единая база данных платформы. Встроенные миграции не поставляются: схема управляется внешним эксплуатационным слоем по контракту из GitHub Wiki.
5. Межботовое взаимодействие строится вокруг единого контракта событий и шины событий.
6. Каждый адаптер может запускаться отдельно через `cajeer-bots run <adapter>`, `python -m core run <adapter>` или standalone-пакет `bot` внутри каталога адаптера.
7. Основная документация готовится для GitHub Wiki в каталоге `wiki/`.

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

## Режимы запуска

```bash
cajeer-bots run all
cajeer-bots run telegram
cajeer-bots run discord
cajeer-bots run vkontakte
cajeer-bots run worker
cajeer-bots run api
cajeer-bots run bridge
```

## HTTP API

Минимальный API-режим доступен без внешних веб-фреймворков:

```bash
cajeer-bots run api
```

Базовые маршруты:

```text
GET /healthz
GET /readyz
GET /version
GET /adapters
GET /modules
GET /plugins
GET /events
GET /commands
GET /config/summary
GET /adapter-status
```

## Структура

```text
CajeerBots/
├── core/        # ядро платформы, CLI, runtime, конфигурация, события, registry
├── bots/        # адаптеры Telegram, Discord и ВКонтакте
├── modules/     # официальные модули платформы
├── plugins/     # расширения платформы
├── scripts/     # install/run/doctor/release
├── ops/         # примеры systemd/nginx/docker
└── wiki/        # страницы для GitHub Wiki
```

## Документация

Основная документация находится в GitHub Wiki. Исходники страниц лежат в каталоге `wiki/`.
