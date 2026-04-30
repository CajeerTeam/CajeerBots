from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable
from urllib.parse import unquote
from uuid import uuid4

from core.api import MAX_BODY_BYTES, ApiServer

ASGIReceive = Callable[[], Awaitable[dict[str, Any]]]
ASGISend = Callable[[dict[str, Any]], Awaitable[None]]


class HeaderMap(dict[str, str]):
    def get(self, key: str, default: str = "") -> str:  # type: ignore[override]
        return super().get(key.lower(), default)


def _headers(scope: dict[str, Any]) -> HeaderMap:
    result: HeaderMap = HeaderMap()
    for key, value in scope.get("headers") or []:
        result[key.decode("latin1").lower()] = value.decode("latin1")
    return result


async def _read_body(receive: ASGIReceive) -> bytes:
    body = bytearray()
    more = True
    while more:
        message = await receive()
        if message.get("type") != "http.request":
            continue
        chunk = message.get("body", b"") or b""
        body.extend(chunk)
        if len(body) > MAX_BODY_BYTES:
            raise ValueError("request body слишком большой")
        more = bool(message.get("more_body"))
    return bytes(body)


def _json_body(raw: bytes) -> dict[str, object]:
    if not raw:
        return {}
    data = json.loads(raw.decode("utf-8") or "{}")
    if not isinstance(data, dict):
        raise ValueError("request body должен быть JSON-объектом")
    return data


async def _send_response(send: ASGISend, status: int, payload: dict[str, object] | str, content_type: str, request_id: str) -> None:
    if isinstance(payload, str):
        body = payload.encode("utf-8")
        response_type = f"{content_type}; charset=utf-8"
    else:
        payload.setdefault("request_id", request_id)
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        response_type = "application/json; charset=utf-8"
    await send({
        "type": "http.response.start",
        "status": int(status),
        "headers": [
            (b"content-type", response_type.encode("latin1")),
            (b"content-length", str(len(body)).encode("ascii")),
            (b"x-request-id", request_id.encode("ascii")),
        ],
    })
    await send({"type": "http.response.body", "body": body})


def create_app(runtime: Any):
    """Вернуть dependency-light ASGI application для production API.

    Основная бизнес-логика маршрутов остаётся в ApiServer. Синхронные route handlers
    выполняются через worker thread, поэтому ASGI loop не блокируется и не конфликтует
    с runtime event loop.
    """

    server = ApiServer(runtime, loop=None)

    async def app(scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        if scope.get("type") != "http":
            await send({"type": "http.response.start", "status": 404, "headers": []})
            await send({"type": "http.response.body", "body": b""})
            return
        headers = _headers(scope)
        request_id = headers.get("x-request-id") or str(uuid4())
        method = str(scope.get("method") or "GET").upper()
        path = unquote(str(scope.get("path") or "/"))
        client = scope.get("client") or ("unknown", 0)
        client_ip = str(client[0]) if client else "unknown"
        scope_value = server._token_scope(headers)

        if method == "GET":
            if not server._can_get(path, scope_value):
                runtime.audit.write(actor_type="api", actor_id=scope_value or "anonymous", action="http.get", resource=path, result="denied", ip=client_ip, user_agent=headers.get("user-agent"))
                await _send_response(send, 401, {"ok": False, "error": {"code": "unauthorized", "message": "требуется токен"}}, "application/json", request_id)
                return
            status, payload, content_type = await asyncio.to_thread(server._payload, path)
            await _send_response(send, status, payload, content_type, request_id)
            return

        if method == "POST":
            if path in {"/webhooks/telegram", "/webhooks/vkontakte"}:
                if server._webhook_rate_limited(f"{path}:{client_ip}"):
                    runtime.audit.write(actor_type="webhook", actor_id=path.rsplit("/", 1)[-1], action="webhook.rate_limited", resource=path, result="denied", ip=client_ip, user_agent=headers.get("user-agent"))
                    await _send_response(send, 429, {"ok": False, "error": {"code": "rate_limited", "message": "webhook rate limit exceeded"}}, "application/json", request_id)
                    return
                try:
                    body = _json_body(await _read_body(receive))
                    if path == "/webhooks/telegram" and not server._telegram_webhook_authorized(headers):
                        server._webhook_rate_limited(f"auth:{path}:{client_ip}", failed_auth=True)
                        runtime.audit.write(actor_type="webhook", actor_id="telegram", action="webhook.telegram.denied", resource=path, result="denied", ip=client_ip, user_agent=headers.get("user-agent"))
                        await _send_response(send, 401, {"ok": False, "error": {"code": "unauthorized", "message": "invalid telegram webhook secret"}}, "application/json", request_id)
                        return
                    status, payload, content_type = await asyncio.to_thread(server._post_payload, path, body, actor=path.rsplit("/", 1)[-1])
                except Exception as exc:  # noqa: BLE001
                    status, payload, content_type = 400, {"ok": False, "error": {"code": "bad_request", "message": str(exc)}}, "application/json"
                await _send_response(send, status, payload, content_type, request_id)
                return

            if not server._scope_allowed(path, "POST", scope_value):
                runtime.audit.write(actor_type="api", actor_id=scope_value or "anonymous", action="http.post", resource=path, result="denied", ip=client_ip, user_agent=headers.get("user-agent"))
                await _send_response(send, 401, {"ok": False, "error": {"code": "unauthorized", "message": "требуется admin-токен"}}, "application/json", request_id)
                return
            try:
                body = _json_body(await _read_body(receive))
                status, payload, content_type = await asyncio.to_thread(server._post_payload, path, body, actor=scope_value or "api")
            except Exception as exc:  # noqa: BLE001
                status, payload, content_type = 400, {"ok": False, "error": {"code": "bad_request", "message": str(exc)}}, "application/json"
            await _send_response(send, status, payload, content_type, request_id)
            return

        await _send_response(send, 405, {"ok": False, "error": {"code": "method_not_allowed", "message": "метод не поддерживается"}}, "application/json", request_id)

    return app
