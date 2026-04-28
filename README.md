# Cajeer Bots Platform

**Cajeer Bots** — единая платформа для запуска Telegram, Discord, VKontakte и будущих ботов как одного сервиса или по отдельности.

## Архитектурные правила

1. Telegram/Discord/VK — transport adapters, а не отдельные продукты.
2. Бизнес-логика находится в `modules` и `plugins`.
3. PostgreSQL одна, но изоляция идёт через schemas: `shared`, `telegram`, `discord`, `vkontakte`, `modules_*`, `plugins_*`.
4. Межботовое взаимодействие идёт через `shared.event_bus`, `shared.event_outbox`, `shared.event_inbox`, `shared.event_dead_letters`.
5. Identity, RBAC, logs, event contracts и migrations общие.
6. Каждый adapter может запускаться отдельно через `python -m cajeer_bots run <adapter>` или через legacy-shim `bots/<adapter>/nmbot/main.py`.

## Быстрый старт

```bash
cp .env.example .env
./scripts/install.sh
./scripts/doctor.sh --offline
./scripts/run.sh all
```

## Режимы

```bash
python -m cajeer_bots run all
python -m cajeer_bots run telegram
python -m cajeer_bots run discord
python -m cajeer_bots run vkontakte
python -m cajeer_bots run worker
python -m cajeer_bots run api
python -m cajeer_bots run bridge
```

## Структура

```text
CajeerBots-platform/
├── cajeer_bots/       # platform core/runtime/contracts
├── bots/              # standalone wrappers + adapter manifests
├── modules/           # official modules
├── plugins/           # optional integrations
├── migrations/        # PostgreSQL schemas
├── scripts/           # install/run/migrate/doctor/release
├── ops/               # systemd/nginx/docker examples
└── docs/              # architecture/deployment docs
```
