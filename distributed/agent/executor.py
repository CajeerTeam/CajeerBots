from __future__ import annotations

from distributed.security.allowlist import is_allowed_command


class CommandExecutor:
    async def execute(self, command: dict[str, object]) -> dict[str, object]:
        command_type = str(command.get("type", ""))
        if not is_allowed_command(command_type):
            return {"ok": False, "error": f"команда запрещена: {command_type}"}
        return {"ok": True, "status": "команда принята", "type": command_type}
