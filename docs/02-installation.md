# Установка

```bash
cp .env.example .env
docker compose up -d postgres redis
pnpm install
pnpm prisma:generate
pnpm dev
```

Позже установка будет доступна через CLI installer:

```bash
cajeer install
```
