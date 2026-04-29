class VkontakteCallback:
    """Каркас Callback API ВКонтакте."""

    async def handle(self, payload: dict[str, object]) -> dict[str, object]:
        return {"ok": True, "payload": payload}
