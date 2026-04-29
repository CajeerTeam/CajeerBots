from __future__ import annotations


def slash_command_payload(name: str, options: dict[str, object] | None = None) -> dict[str, object]:
    return {"name": name, "options": options or {}}


def default_slash_commands() -> list[dict[str, object]]:
    return [
        {"name": "status", "description": "Показать состояние Cajeer Bots"},
        {"name": "help", "description": "Показать команды Cajeer Bots"},
        {"name": "support", "description": "Создать обращение в поддержку"},
        {"name": "announce", "description": "Создать объявление"},
        {"name": "moderation", "description": "Открыть инструменты модерации"},
    ]
