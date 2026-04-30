# Конфигурация

Основная конфигурация задаётся через `.env`. Все пользовательские комментарии и описания должны быть на русском языке.


## Профили окружения

Проект использует scripts-only layout: запускайте `./scripts/install.sh`, `./scripts/run.sh`, `python scripts/setup_wizard.py`. Корневые wrappers намеренно отсутствуют.

Доступны профили: `.env.local.example`, `.env.docker.example`, production-профиль окружения. В production включайте `WEBHOOK_HMAC_REQUIRED=true`, `WEBHOOK_TIMESTAMP_REQUIRED=true`, `WEBHOOK_REPLAY_CACHE=redis` и strict persistence для stateful-модулей.
