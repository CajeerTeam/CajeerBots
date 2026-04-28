# Cajeer Bots

**Cajeer Bots** — универсальная русскоязычная платформа для запуска, управления и расширения ботов в разных мессенджерах и сервисах.

## Архитектурные правила

1. Telegram, Discord и ВКонтакте — адаптеры транспорта, а не отдельные продукты.
2. Общая логика находится в `modules` и `plugins`.
3. Все пользовательские тексты, документация, CLI-описания и примеры должны быть на русском языке.
4. PostgreSQL используется как единая база данных платформы. Миграции в этом каркасе не поставляются и должны управляться внешним эксплуатационным слоем.
5. Межботовое взаимодействие строится вокруг единого контракта событий.
6. Каждый адаптер может запускаться отдельно через `python -m core run <adapter>` или через standalone-пакет `bot` внутри каталога адаптера.
7. Основная документация готовится для GitHub Wiki в каталоге `wiki/`.

## Быстрый старт

```bash
cp .env.example .env
./scripts/install.sh
./scripts/doctor.sh --offline
./scripts/run.sh all
```

## Режимы запуска

```bash
python -m core run all
python -m core run telegram
python -m core run discord
python -m core run vkontakte
python -m core run worker
python -m core run api
python -m core run bridge
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
