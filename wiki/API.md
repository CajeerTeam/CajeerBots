# API

Текущий HTTP API реализован на стандартном `http.server`, потому что это минимальный control-plane без внешнего web framework.

Публичные маршруты:

```text
GET /healthz
GET /readyz
POST /webhooks/telegram
```

`POST /webhooks/telegram` проверяет `X-Telegram-Bot-Api-Secret-Token`, если задан `TELEGRAM_WEBHOOK_SECRET`.

Административные маршруты требуют `Authorization: Bearer <API_TOKEN>`.

Дальнейшее направление для production control-plane — ASGI-слой:

```text
FastAPI / Starlette
uvicorn
OpenAPI из кода
middleware: request-id, rate-limit, audit, CORS, auth scopes
```

## Webhook gateway 0.10.0

`POST /webhooks/telegram` принимает Telegram update и ставит ответ в delivery queue через общий command response pipeline.

`POST /webhooks/vkontakte` принимает VK Callback API payload. Для `type=confirmation` возвращается `VK_CONFIRMATION_CODE`; для остальных событий проверяется `VK_CALLBACK_SECRET`, если он задан.
