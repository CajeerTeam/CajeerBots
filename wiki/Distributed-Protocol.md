# Протокол distributed mode

Базовые схемы:

```text
cajeer.bots.event.v1
cajeer.bots.command.v1
cajeer.bots.ack.v1
cajeer.bots.heartbeat.v1
```

Runtime Agent отправляет события и heartbeat. Core Server отправляет команды. Runtime Agent подтверждает исполнение через ack.
