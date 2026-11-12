# CajeerBots — 1.0.0-rc.3

Русскоязычная self-hosted платформа TypeScript для Telegram, VK и Discord. RC3 реализует Security & Reliability в существующих приложениях: изоляцию, аутентификацию webhook и надёжную доставку. См. [ROADMAP](ROADMAP.md) и [отчёт проверки](RELEASE_VALIDATION_1.0.0-rc.3.md).

## Сначала: обязательный lockfile

Требуются Node.js **22.16.0**, pnpm **9.15.9**, PostgreSQL 17 и Redis 7. Исходный архив не содержал `pnpm-lock.yaml`. В текущем патче прямые версии зафиксированы, однако настоящий lockfile нельзя было получить без доступа к npm registry. Не подменяйте его пустым файлом. На машине с доступом к registry один раз выполните:

```bash
npm install --global pnpm@9.15.9
pnpm deps:lock
pnpm install --frozen-lockfile
```

Зафиксируйте сгенерированный `pnpm-lock.yaml` в Git. Все последующие CI/Docker сборки требуют его и выполняют только frozen install. Для выпуска без изменения зависимостей повторно `deps:lock` не запускайте.

## Локальная установка и проверка

```bash
cp .env.example .env
# PostgreSQL и Redis на localhost
docker compose up -d --wait postgres redis
pnpm prisma:validate
pnpm build
pnpm prisma:deploy
pnpm prisma:seed
pnpm typecheck
pnpm lint
pnpm test
pnpm test:stabilization
pnpm test:e2e
```

`pnpm build` генерирует Prisma Client и компилирует workspace-пакеты в порядке зависимостей; `pnpm typecheck` проверяет их и исходники корня. Необходимые артефакты: `apps/api/dist/main.js`, `apps/bot-runtime/dist/main.js`, `apps/worker/dist/main.js`, `apps/cli/dist/main.js`. Команды запуска из корня:

```bash
pnpm start:api
# в других терминалах:
pnpm start:bot
pnpm start:worker
pnpm cajeer --version
```

`pnpm dev` запускает три собранных сервиса. При изменении исходников повторите `pnpm build`. Для полного тестового прогона используйте отдельную пустую БД, никогда production:

```bash
CAJEER_SMOKE_ISOLATED=1 pnpm smoke:runtime
```

Smoke поднимает отдельные процессы API, bot-runtime и worker на 3300/3301/3302, проверяет CLI и авторизацию, затем завершает сервисы. Не подключайте настоящие bot accounts к тестовой БД.

## Docker Compose — стенд production

```bash
pnpm setup:production https://bots.example.org
# .env.production создаётся с правами 0600; при наличии файла команда откажется его перезаписывать.
docker compose --env-file .env.production -f docker-compose.prod.yml config --quiet
docker compose --env-file .env.production -f docker-compose.prod.yml build
docker compose --env-file .env.production -f docker-compose.prod.yml up -d --wait
```

Сервисы используют один образ `cajeerbots:1.0.0-rc.3`. Сначала запускаются PostgreSQL и Redis, затем `migrate` выполняет `prisma migrate deploy` и идемпотентный seed. После успешной миграции запускается API, затем bot-runtime и worker. API и bot-runtime публикуются только на localhost. Worker health доступен внутри сети Compose.

```bash
curl --fail http://127.0.0.1:3000/api/health
curl --fail http://127.0.0.1:3101/runtime/health
docker compose --env-file .env.production -f docker-compose.prod.yml exec -T worker node -e "fetch('http://127.0.0.1:3102/worker/health').then(r=>process.exit(r.ok?0:1))"
docker compose --env-file .env.production -f docker-compose.prod.yml exec -T api node apps/cli/dist/main.js --version
```

При обновлении существующей инсталляции не создавайте новые секреты и не удаляйте volumes. Сохраните рабочие `DATABASE_URL`, `SECRETS_ENCRYPTION_KEY`, пароли и все ключи из прежнего `.env.production`. Сделайте резервную копию перед запуском миграций. Если использовались порт 3001 и старые имена переменных, приведите их к текущему конфигу; проверьте nginx. Исторические SQL-миграции не изменены.

## Ограничения RC3

- Настройте уникальные webhook credentials, HTTPS и проведите полный прогон RC3 перед публикацией. См. [порядок обновления и проверки](RELEASE_VALIDATION_1.0.0-rc.3.md).
- Полный build, Prisma migrate deploy, E2E и Compose ещё требуют прогона с установленными зависимостями; выпуск не подтверждён как production-ready.
- Для production поддерживается PostgreSQL, Redis и локальное хранилище как базовый профиль проверки. Альтернативные драйверы и масштабирование не подтверждены этим релизом.

Лицензия: [Apache-2.0](LICENSE), [NOTICE](NOTICE). Вопросы разработки: dev@cajeer.ru, безопасность: security@cajeer.ru.
