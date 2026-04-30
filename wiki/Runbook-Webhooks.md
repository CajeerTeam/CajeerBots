# Runbook: Webhooks

## Назначение

Webhook endpoints принимают Telegram/VK события через API.

## Production baseline

- `TELEGRAM_WEBHOOK_SECRET` обязателен для Telegram webhook.
- `VK_CALLBACK_SECRET` обязателен для VK Callback API.
- `WEBHOOK_REPLAY_PROTECTION=true`.
- API должен быть за TLS reverse proxy.

## Проверка

```bash
curl -f http://127.0.0.1:8088/livez
curl -f http://127.0.0.1:8088/readyz
```


## Strict webhook mode

При `WEBHOOK_HMAC_REQUIRED=true` запрос без подписи отклоняется. При `WEBHOOK_TIMESTAMP_REQUIRED=true` устаревшие и повторные запросы отклоняются replay-защитой. Для нескольких экземпляров используйте Redis replay cache.
