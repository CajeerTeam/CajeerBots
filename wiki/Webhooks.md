# Webhooks

## Production-профили webhooks

Для прямых Telegram/VK callback-запросов используйте `configs/env/.env.production.direct-webhook.example`:
`WEBHOOK_PROFILE=direct`, `WEBHOOK_HMAC_REQUIRED=false`, `WEBHOOK_TIMESTAMP_REQUIRED=false`.

Для схемы через доверенный gateway/reverse proxy используйте
`configs/env/.env.production.gateway-signed-webhook.example`:
`WEBHOOK_PROFILE=gateway-signed`, `WEBHOOK_HMAC_REQUIRED=true`,
`WEBHOOK_TIMESTAMP_REQUIRED=true`.
