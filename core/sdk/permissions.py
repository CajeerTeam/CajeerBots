from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PermissionSet:
    values: frozenset[str]

    @classmethod
    def from_iterable(cls, values: list[str] | tuple[str, ...] | set[str]) -> "PermissionSet":
        return cls(frozenset(str(item) for item in values))

    def allows(self, permission: str) -> bool:
        return "*" in self.values or permission in self.values
