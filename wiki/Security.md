# Безопасность

Ключевые требования: секреты только через окружение, подпись событий, минимальные права доступа, аудит действий.

## Scoped API tokens

Помимо env-токенов поддерживается file-backed registry `runtime/secrets/api_tokens.json`, где хранятся только SHA-256 хэши токенов.

```bash
cajeer-bots tokens create --id admin-2026-04 --scope system.admin --prefix cb_admin_
cajeer-bots tokens revoke admin-2026-04
cajeer-bots tokens list
```


## Webhook security

Для production включайте HMAC и timestamp: `WEBHOOK_HMAC_REQUIRED=true`, `WEBHOOK_TIMESTAMP_REQUIRED=true`. Replay cache лучше держать в Redis: `WEBHOOK_REPLAY_CACHE=redis`. Метрики `cajeerbots_webhook_rejected_total` и `cajeerbots_rbac_denied_total` помогают видеть отказы.

## RBAC bootstrap

Первого владельца создавайте в БД: `python -m core rbac bootstrap-owner --backend db --platform telegram --user-id <id>`. `--backend auto` сначала пробует PostgreSQL, затем локальный cache.
