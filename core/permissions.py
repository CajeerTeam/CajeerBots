from dataclasses import dataclass

@dataclass(frozen=True)
class Permission:
    key: str
    description: str

CORE_PERMISSIONS = [
    Permission("bots.runtime.run", "Run Cajeer Bots runtime"),
    Permission("bots.runtime.restart", "Restart bot adapters"),
    Permission("bots.modules.configure", "Enable or disable modules"),
    Permission("bots.plugins.configure", "Enable or disable plugins"),
    Permission("bots.events.read", "Read event bus entries"),
    Permission("bots.events.retry", "Retry dead-letter events"),
    Permission("bots.logs.read", "Read logs"),
    Permission("bots.announce.create", "Create announcements"),
    Permission("bots.support.reply", "Reply to support tickets"),
]

def has_permission(grants: set[str], permission: str) -> bool:
    return "*" in grants or permission in grants
