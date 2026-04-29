from __future__ import annotations

import secrets


def generate_secret_urlsafe(bytes_count: int = 32) -> str:
    return secrets.token_urlsafe(bytes_count)


def generate_env_block() -> str:
    values = {
        "EVENT_SIGNING_SECRET": generate_secret_urlsafe(),
        "API_TOKEN": generate_secret_urlsafe(),
        "API_TOKEN_READONLY": generate_secret_urlsafe(),
        "API_TOKEN_METRICS": generate_secret_urlsafe(),
        "NODE_SECRET": generate_secret_urlsafe(),
        "CAJEER_WORKSPACE_TOKEN": generate_secret_urlsafe(),
    }
    return "\n".join(f"{key}={value}" for key, value in values.items()) + "\n"
