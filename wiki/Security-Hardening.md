# Security Hardening

Эта страница фиксирует production baseline для Cajeer Bots.

## Production doctor

Перед запуском production обязательно:

```bash
python3 -m core doctor --profile production
```

Doctor должен падать при:

```text
пустом EVENT_SIGNING_SECRET
placeholder API_TOKEN
METRICS_PUBLIC=true
WEBHOOK_REPLAY_PROTECTION=false
API_BIND=0.0.0.0 без API_BEHIND_REVERSE_PROXY=true
webhook-режиме Telegram без TELEGRAM_WEBHOOK_SECRET
включённом VK Callback API без VK_CALLBACK_SECRET
```

## API tokens

Production-рекомендация — scoped token registry:

```bash
python3 -m core tokens create --id admin --scope system.admin
python3 -m core tokens create --id readonly --scope system.read
python3 -m core tokens create --id metrics --scope system.metrics
```

Файл registry хранит sha256-хэши, а не исходные токены. Env-токены допустимы для bootstrap/dev, но для долгоживущей production-инсталляции предпочтителен registry.

## Webhook security

Поддерживаются три уровня защиты:

1. Native provider secret:
   - Telegram: `X-Telegram-Bot-Api-Secret-Token`
   - VK: `secret` в callback body
2. Optional HMAC:
   - `X-Cajeer-Signature: sha256=<hex>`
   - подпись считается по raw body и `EVENT_SIGNING_SECRET`
3. Replay guard:
   - nonce/request-id/timestamp/body digest
   - TTL задаётся `WEBHOOK_REPLAY_TTL_SECONDS`

HMAC является optional: если `X-Cajeer-Signature` отсутствует, запрос проходит native-проверки провайдера. Если header присутствует, подпись обязана быть валидной.

## Plugin permissions

Plugin permissions enforcement включён на runtime-уровне:

```text
api.route.register -> register_api_routes
scheduler.jobs.register -> register_scheduled_jobs
events.read -> on_event/on_command
events.publish -> context.events.publish
delivery.enqueue -> context.delivery.enqueue
audit.write -> context.audit.write
config.read -> context.safe_config
```

Это не полноценная sandbox-изоляция Python-кода. Это контрактная защита SDK и раннее выявление некорректных плагинов. Непроверенные сторонние плагины нельзя считать доверенными.

## Docker hardening

Dockerfile запускает приложение от non-root пользователя `cajeer`.

Compose-сервисы приложения должны иметь:

```yaml
read_only: true
user: "cajeer"
tmpfs:
  - /tmp
  - /app/runtime
security_opt:
  - no-new-privileges:true
cap_drop:
  - ALL
```

Для постоянных runtime-данных используйте отдельные volumes и выдавайте их только тем сервисам, которым это нужно.

## Release security gates

Перед публикацией:

```bash
scripts/check_secrets.sh
python3 -m core release checklist --file release/checklist.yaml
python3 -m core release verify dist/CajeerBots-<version>.zip --deep
```

Release artifact не должен содержать:

```text
.env
private keys
реальные Telegram/Discord/VK tokens
DATABASE_URL с паролем
__pycache__
*.pyc
.pytest_cache
```

## Operational drills

Исполняемые drill-тесты:

```bash
scripts/run_drills.sh
scripts/fault_drill.sh
docker compose --profile integration up --build --abort-on-container-exit
```

Если drill не проходит, релиз нельзя считать production-ready.


## Source archive vs release artifact

`CajeerBots-main.zip` — исходный архив, а не production artifact. Он может не хранить Unix executable-bit. Production-релиз должен собираться только через `scripts/release.sh`, после чего оба артефакта проверяются:

```bash
python3 -m core release verify dist/CajeerBots-<version>.zip --deep
python3 -m core release verify dist/CajeerBots-<version>.tar.gz --deep
```

## Python support policy

Production support matrix ограничена Python 3.11–3.12. Python 3.13 не заявлен в `requires-python` до прохождения отдельной CI matrix и compatibility-drill.

## Исполняемые chaos/drill-тесты

`release/checklist.yaml` содержит исполняемые `drill_commands`. Они запускаются через:

```bash
./scripts/run_drills.sh
```

В состав входят проверки версии, архитектуры, документации, secret-scan, smoke integrations, worker crash/lease reclaim/retry и preflight для storage chaos.
