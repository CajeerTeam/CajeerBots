# Local mode

Local mode — основной режим Cajeer Bots по умолчанию.

## Обязательные свойства

- запускает всех включённых ботов сразу;
- запускает Telegram отдельно;
- запускает Discord отдельно;
- запускает ВКонтакте отдельно;
- может запускать локальные `api`, `worker`, `bridge`;
- не требует Core Server, Runtime Agent, `NODE_ID`, `NODE_SECRET`, WebSocket gateway или broker.

## Конфигурация

```env
CAJEER_BOTS_MODE=local
CAJEER_BOTS_DEFAULT_TARGET=all
EVENT_BUS_BACKEND=memory
```

Для многопроцессного local-запуска через Docker Compose используйте `EVENT_BUS_BACKEND=redis`.

## Команды

```bash
cajeer-bots run all
cajeer-bots run telegram
cajeer-bots run discord
cajeer-bots run vkontakte
```
