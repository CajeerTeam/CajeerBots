# DevTools и Bot Scenario Runner

CajeerBots `0.13.0` добавляет DX-слой без внешней веб-панели. Инструменты работают через API, CLI и команды внутри ботов.

## Возможности

- debug sessions;
- debug events;
- provider-update simulator для Telegram/VK/Discord;
- bot scenarios;
- scenario runs;
- assertions по outbound-сообщениям;
- импорт/экспорт сценариев вместе с community.

## CLI

```bash
pnpm cajeer dev
pnpm cajeer devtools sessions:start --community <id> --title "Проверка команд"
pnpm cajeer devtools scenarios:list --community <id>
pnpm cajeer devtools scenarios:run --community <id> --scenario system.help-smoke --account <botAccountId>
pnpm cajeer devtools simulate:update --community <id> --account <botAccountId> --provider telegram --type command --command debug
```

## Scenario definition

```json
{
  "defaultUserExternalId": "dev-user",
  "steps": [
    { "type": "command", "command": "help", "chatExternalId": "dev-chat" },
    { "type": "assert_outbound", "chatExternalId": "dev-chat", "expectOutboundTextContains": "CajeerBots" }
  ]
}
```

`assert_outbound` ищет реальные записи `bot_outbound_messages`, поэтому сценарий можно использовать для проверки runtime-обработки команд.
