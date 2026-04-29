ALLOWED_COMMAND_TYPES = {
    "message.send",
    "message.edit",
    "message.delete",
    "adapter.restart",
    "health.ping",
}


def is_allowed_command(command_type: str) -> bool:
    return command_type in ALLOWED_COMMAND_TYPES
