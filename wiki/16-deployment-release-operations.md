# Deployment & Release Operations

CajeerBots 0.15.0 добавляет рабочий слой production deployment без обязательной внешней веб-панели.

## Что входит

- DB-backed deployment targets.
- История deployment checks.
- Журнал deployment runs.
- Release artifacts и release manifest.
- Генератор `.env.production`.
- Валидатор production env.
- Генератор Nginx reverse proxy config.
- Генератор systemd units.
- `docker-compose.prod.yml` с migration job и healthchecks.

## Быстрый production-сценарий

```bash
pnpm cajeer deployment env:generate --app-url https://bots.example.cajeer.ru --out .env.production
pnpm cajeer deployment env:validate .env.production
pnpm cajeer deployment nginx:config --server-name bots.example.cajeer.ru --out docker/nginx/cajeerbots.conf
pnpm cajeer deployment checks:run --root .
docker compose -f docker-compose.prod.yml --env-file .env.production up -d --build
```

## API

Основной префикс: `/api/deployment`.

- `GET /targets`
- `PUT /targets`
- `POST /targets/:targetId/check`
- `POST /checks/run`
- `GET /checks`
- `POST /runs`
- `POST /runs/:id/finish`
- `POST /env/render`
- `POST /env/validate`
- `POST /nginx/render`
- `POST /systemd/render`
- `POST /release-manifest`
- `GET /release-artifacts`

## Permissions

- `deployment:read`
- `deployment:write`
- `deployment:run`
- `deployment:release`

## Docker Compose production

`docker-compose.prod.yml` содержит:

- PostgreSQL 17;
- Redis;
- migration job;
- API;
- bot-runtime;
- worker;
- healthchecks;
- named volumes;
- non-root runtime user.

## Release manifest

Release manifest имеет schema `cajeerbots.release.manifest.v1` и содержит список файлов с `sha256` и размером. Он используется как машинно-читаемый слой перед публикацией архивов через GitFlic Releases/Registry.
