from __future__ import annotations

from ipaddress import ip_address, ip_network
from typing import Mapping


def _trusted(remote_ip: str, cidrs: list[str]) -> bool:
    try:
        address = ip_address(remote_ip)
    except ValueError:
        return False
    for raw in cidrs:
        raw = raw.strip()
        if not raw:
            continue
        try:
            if address in ip_network(raw, strict=False):
                return True
        except ValueError:
            continue
    return False


def _first_forwarded_for(value: str) -> str:
    return value.split(",", 1)[0].strip()


def _forwarded_for(value: str) -> str:
    # RFC 7239: Forwarded: for=203.0.113.43;proto=https
    for part in value.split(";"):
        key, sep, item = part.partition("=")
        if sep and key.strip().lower() == "for":
            return item.strip().strip('"').strip("[]")
    return ""


def client_ip_from_headers(
    *,
    remote_ip: str,
    headers: Mapping[str, str],
    behind_reverse_proxy: bool,
    trusted_proxy_cidrs: list[str],
    real_ip_header: str,
) -> str:
    if not behind_reverse_proxy or not _trusted(remote_ip, trusted_proxy_cidrs):
        return remote_ip
    lower = {str(k).lower(): str(v) for k, v in headers.items()}
    header = real_ip_header.lower()
    if header == "x-forwarded-for":
        return _first_forwarded_for(lower.get("x-forwarded-for", "")) or remote_ip
    if header == "x-real-ip":
        return lower.get("x-real-ip", "").strip() or remote_ip
    if header == "forwarded":
        return _forwarded_for(lower.get("forwarded", "")) or remote_ip
    return remote_ip
