from __future__ import annotations

import json


async def execute_approval_request(application, row: dict, *, actor_user_id: int, actor_name: str = ''):
    db = application.bot_data['db']
    try:
        payload = json.loads(row.get('payload_json') or '{}')
    except Exception:
        payload = {}
    kind = str(row.get('kind') or '')
    if kind == 'maintenance':
        db.set_maintenance_state(active=bool(payload.get('active')), message=str(payload.get('message') or ''), updated_by=str(actor_user_id))
        return True, {'kind': kind, 'active': bool(payload.get('active'))}
    if kind == 'security_session_action':
        status_client = application.bot_data.get('status_client')
        if not status_client:
            return False, {'message': 'status_client_not_available'}
        result = await status_client.act_security_session(telegram_user_id=int(payload.get('telegram_user_id') or 0), action=str(payload.get('action') or ''), session_id=str(payload.get('session_id') or ''), scope=str(payload.get('scope') or ''))
        return bool(result.ok), result.raw or {'message': result.message}
    return False, {'message': f'unsupported approval kind: {kind}'}
