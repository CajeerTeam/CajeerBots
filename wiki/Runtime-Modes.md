# Режимы запуска

Cajeer Bots разделяет архитектурный режим и цель запуска.

```env
CAJEER_BOTS_MODE=local
CAJEER_BOTS_DEFAULT_TARGET=all
```

`local` и `distributed` — архитектурные режимы. `all`, `telegram`, `discord`, `vkontakte`, `api`, `worker`, `bridge` — цели запуска.

## Local mode

Local mode — основной режим по умолчанию.

```bash
cajeer-bots run all
cajeer-bots run telegram
cajeer-bots run discord
cajeer-bots run vkontakte
```

Он не требует Core Server, Runtime Agent, `NODE_ID`, `NODE_SECRET` или broker.

## Служебные local-процессы

```bash
cajeer-bots run api
cajeer-bots run worker
cajeer-bots run bridge
```

## Distributed mode

Distributed mode является дополнительным функционалом и включается явно.

```bash
cajeer-bots distributed doctor --offline
cajeer-bots distributed protocol
```

## Standalone-запуск адаптеров

```bash
python bots/telegram/main.py
python bots/discord/main.py
python bots/vkontakte/main.py
```

Или из каталога конкретного адаптера:

```bash
cd bots/telegram
python -m bot.main
```
