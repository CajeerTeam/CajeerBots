class Gateway:
    """Заглушка шлюза distributed mode: WebSocket/gRPC/NATS подключаются отдельным этапом."""

    def health(self) -> dict[str, object]:
        return {"ok": True, "status": "шлюз распределённого режима доступен"}
