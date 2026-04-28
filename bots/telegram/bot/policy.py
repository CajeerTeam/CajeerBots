from __future__ import annotations

from typing import Any

SENSITIVE_POLICIES = {
    'maintenance.on': {'min_role': 'admin', 'second_approval': True},
    'maintenance.off': {'min_role': 'admin', 'second_approval': True},
    'security.revoke_all': {'min_role': 'owner', 'second_approval': True},
    'security.trusted_clear': {'min_role': 'owner', 'second_approval': True},
}


def policy_for(action: str) -> dict[str, Any]:
    return dict(SENSITIVE_POLICIES.get(action, {}))
