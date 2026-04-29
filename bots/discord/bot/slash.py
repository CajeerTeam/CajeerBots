def slash_command_payload(name: str, options: dict[str, object] | None = None) -> dict[str, object]:
    return {"name": name, "options": options or {}}
