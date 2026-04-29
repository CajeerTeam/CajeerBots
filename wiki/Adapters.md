# Адаптеры

Адаптер отвечает за связь с конкретной внешней платформой и преобразование внешних событий во внутренний формат Cajeer Bots.

## Capabilities 0.10.0

Capabilities адаптеров синхронизированы с manifests:

- Telegram: `messages.receive`, `messages.send`, `files.receive`, `webhooks`, `health`, `events.publish`.
- Discord: `messages.receive`, `messages.send`, `files.receive`, `roles`, `reactions`, `slash_commands`, `health`, `events.publish`.
- VK: `messages.receive`, `messages.send`, `files.receive`, `webhooks`, `health`, `events.publish`.

Discord по умолчанию работает через slash commands; чтение обычных сообщений требует `DISCORD_MESSAGE_CONTENT_ENABLED=true`.
