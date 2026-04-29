# Distributed mode

Distributed mode — дополнительный функционал. Он не требуется для обычного local-запуска.

## Назначение

Distributed mode разделяет платформу на:

- Core Server / Control Plane;
- Runtime Agent / Worker Node;
- защищённый транспорт событий и команд;
- heartbeat runtime-нод;
- локальную очередь и degraded mode.

## Включение

```env
CAJEER_BOTS_MODE=distributed
DISTRIBUTED_ENABLED=true
DISTRIBUTED_ROLE=agent
CORE_SERVER_URL=https://core.example.local
NODE_ID=runtime-01
NODE_SECRET=change-me-node-secret
```

Если `DISTRIBUTED_ENABLED=false`, distributed-переменные не требуются.
