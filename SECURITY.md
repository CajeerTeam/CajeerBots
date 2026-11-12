# Политика безопасности

## Поддерживаемые версии

Целевая версия: `1.0.0` (Stable / Limited Production). Публикация подписанного релиза и запуск production требуют прохождения обязательных release gates.

| Версия | Статус поддержки |
| --- | --- |
| `1.0.0` | Security fixes; limited production after verification |
| `1.0.0-rc.6` | Historical release candidate |
| LTS-ветки | Будут объявлены отдельно после стабилизации API |
| Устаревшие pre-release сборки | Не поддерживаются после выхода более новой pre-release версии, если не указано иное |

Политика LTS и security releases фиксируется в `ROADMAP.md`, `CHANGELOG.md` и релизных заметках.

## Как сообщить об уязвимости

Не публикуйте детали уязвимости в issue, discussions, публичных чатах или pull/merge request.

Отправьте сообщение на security@cajeer.ru.

Включите в письмо:

1. краткое описание уязвимости;
2. затронутую версию, commit или release;
3. затронутые компоненты: API, bot-runtime, CLI, SDK, extension system, registry, webhooks, auth/RBAC, storage, queue, outbox и т.д.;
4. шаги воспроизведения;
5. ожидаемое и фактическое поведение;
6. возможное влияние на пользователей;
7. proof-of-concept, если он безопасен и не раскрывает чужие данные;
8. ваши контактные данные для уточнений.

## Что считается уязвимостью

К уязвимостям относятся, в частности:

- обход RBAC, permission scopes, API token scopes или JWT-проверок;
- утечка секретов, токенов, webhook signing keys или конфигурации;
- SSRF, RCE, SQL injection, command injection, path traversal;
- обход signature validation для webhook/events/registry payloads;
- нарушение idempotency, приводящее к повторной выдаче прав, оплат, действий или событий;
- небезопасная обработка расширений, manifest, hooks, permissions или sandbox;
- privilege escalation в CLI, registry, modules/plugins или bot-runtime;
- небезопасная работа с очередями, outbox, storage или импортом/экспортом;
- уязвимости 2FA, rate limit, audit log или session/token lifecycle.

## Процесс обработки

1. Команда подтверждает получение сообщения.
2. Уязвимость проверяется и классифицируется по влиянию.
3. При необходимости запрашиваются дополнительные сведения.
4. Исправление готовится в приватном рабочем процессе.
5. Выпускается security release или advisory.
6. После исправления допускается согласованное публичное раскрытие.

## Coordinated disclosure

Пожалуйста, дайте проекту разумное время на исправление до публичной публикации деталей. Если уязвимость активно эксплуатируется, укажите это в теме письма и начале описания.

## Ключевые контакты

- security@cajeer.ru — уязвимости и security disclosure
- abuse@cajeer.ru — злоупотребления и вредоносная активность
- legal@cajeer.ru — правовые вопросы
- admin@cajeer.ru — административные вопросы проекта
