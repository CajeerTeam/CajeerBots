# Развёртывание

Поддерживаются запуск через Python, systemd и Docker Compose.


## Production-контракт запуска

1. `cp production-профиль .env`
2. Заполнить PostgreSQL/Redis DSN и секреты.
3. `python -m core db upgrade head`
4. `python -m core rbac bootstrap-owner --backend db --platform telegram --user-id <id>`
5. `python -m core doctor --profile production`
6. `./scripts/run.sh api` или Docker Compose.

В Docker `/app/runtime` должен быть persistent volume. Временным остаётся только `/tmp`.
