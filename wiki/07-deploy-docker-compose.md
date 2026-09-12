# Deploy через Docker Compose

На первом этапе основной способ деплоя — Docker Compose.

```bash
docker compose up -d postgres redis
docker compose --profile app up -d --build
```

Kubernetes планируется позже отдельной фазой roadmap.
