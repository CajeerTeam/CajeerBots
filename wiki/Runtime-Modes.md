# Режимы запуска

Предпочтительный интерфейс после установки пакета:

```bash
cajeer-bots run all
```

Технический режим для разработки:

```bash
python -m core run all
```

## Доступные режимы

- `all` — все включённые адаптеры.
- `telegram` — только Telegram.
- `discord` — только Discord.
- `vkontakte` — только ВКонтакте.
- `api` — HTTP API платформы.
- `worker` — фоновые задачи.
- `bridge` — шина событий.

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
