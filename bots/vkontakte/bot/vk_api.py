from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from typing import Any

import requests

logger = logging.getLogger(__name__)


class VKAPIError(RuntimeError):
    pass


@dataclass(slots=True)
class LongPollServer:
    server: str
    key: str
    ts: str


class VKClient:
    def __init__(self, token: str, api_version: str, timeout: int = 35) -> None:
        self.token = token
        self.api_version = api_version
        self.timeout = timeout
        self.session = requests.Session()

    def call(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(params or {})
        payload['access_token'] = self.token
        payload['v'] = self.api_version
        response = self.session.post(
            f'https://api.vk.com/method/{method}',
            data=payload,
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if 'error' in data:
            raise VKAPIError(f"{method} failed: {data['error']}")
        return data['response']

    def get_group_info(self, group_id: int) -> dict[str, Any]:
        data = self.call('groups.getById', {'group_id': group_id})
        if isinstance(data, list) and data:
            return dict(data[0])
        return dict(data)

    def get_longpoll_server(self, group_id: int) -> LongPollServer:
        data = self.call('groups.getLongPollServer', {'group_id': group_id})
        return LongPollServer(server=data['server'], key=data['key'], ts=str(data['ts']))

    def poll(self, lp: LongPollServer, wait: int) -> dict[str, Any]:
        response = self.session.get(
            lp.server,
            params={
                'act': 'a_check',
                'key': lp.key,
                'ts': lp.ts,
                'wait': wait,
                'mode': 2,
                'version': 3,
            },
            timeout=wait + 10,
        )
        response.raise_for_status()
        return response.json()

    def send_message(self, peer_id: int, text: str, attachment: str = '') -> None:
        self.call(
            'messages.send',
            {
                'peer_id': peer_id,
                'message': text,
                'attachment': attachment,
                'random_id': random.randint(1, 2_147_483_647),
            },
        )

    def wall_post(self, group_id: int, text: str, attachment: str = '') -> None:
        params: dict[str, Any] = {
            'owner_id': -abs(group_id),
            'from_group': 1,
            'message': text,
        }
        if attachment:
            params['attachments'] = attachment
        self.call('wall.post', params)

    def close(self) -> None:
        self.session.close()
