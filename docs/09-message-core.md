# Message Core

Message Core в версии `0.8.0` — рабочий слой сообщений, кнопок и форм без внешней веб-панели.

## Возможности

- `locale_catalogs` — локализация по `communityId`, `namespace`, `locale`.
- `message_button_layouts` — provider-aware кнопки для Telegram, VK и Discord.
- `message_form_definitions` — определения форм для bot-интерфейсов.
- `message_form_submissions` — журнал отправленных форм с результатом валидации.
- `message_render_logs` — журнал render/preview.

## API

```text
GET /api/communities/:communityId/message-core/catalogs
PUT /api/communities/:communityId/message-core/catalogs
GET /api/communities/:communityId/message-core/button-layouts
PUT /api/communities/:communityId/message-core/button-layouts
GET /api/communities/:communityId/message-core/forms
PUT /api/communities/:communityId/message-core/forms
POST /api/communities/:communityId/message-core/forms/:code/submit
POST /api/communities/:communityId/message-core/render
GET /api/communities/:communityId/message-core/render-logs
```

## CLI

```bash
cajeer message-core catalog:list --community <communityId>
cajeer message-core catalog:set --community <communityId> --namespace system --locale ru --file messages.ru.json
cajeer message-core buttons:set --community <communityId> --code system.main --file buttons.json
cajeer message-core forms:set --community <communityId> --code support.request --title "Обращение" --fields fields.json
cajeer message-core render --community <communityId> --template system.status --provider telegram
```

## Формы

Поддерживаемые типы полей:

- `text`
- `number`
- `boolean`
- `select`
- `multiselect`

Для `text` и `number` работают `required`, `min`, `max`; для `text` дополнительно `pattern`; для `select` и `multiselect` проверяется список `options`.

## Provider buttons

Message Core преобразует один layout в формат provider:

- Telegram — `reply_markup.inline_keyboard`;
- VK — `keyboard` с callback/open_link actions;
- Discord — `components` с buttons.
