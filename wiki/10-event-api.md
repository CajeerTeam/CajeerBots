# Event API

Event API в CajeerBots `0.9.0` — durable слой доменных и интеграционных событий.

## Основные сущности

- `event_topics` — список topics и metadata retention.
- `event_records` — неизменяемый журнал опубликованных событий.
- `event_subscriptions` — правила fanout доставки.
- `event_subscription_deliveries` — состояние delivery/retry.
- `event_dead_letters` — события, которые не удалось доставить после лимита попыток.

## Target types

- `outbox` — создаёт запись в outbox для последующей webhook-доставки.
- `webhook_endpoint` — отправляет событие напрямую в существующий webhook endpoint.
- `extension_hook` — ставит hook в extension runtime queue.
- `log` — помечает delivery как обработанную, полезно для диагностики.

## Pattern matching

Поддерживаются:

- `*` — все события;
- `bot.*` — события с prefix `bot.`;
- `system.audit` — точное совпадение;
- маски с `*` внутри строки.

## CLI

```bash
pnpm cajeer events topics:list
pnpm cajeer events publish --type system.audit --payload '{"action":"test"}'
pnpm cajeer events subscriptions:list
pnpm cajeer events deliveries:list --status failed
pnpm cajeer events delivery:retry --id <delivery-id>
pnpm cajeer events dead-letter:list
```
