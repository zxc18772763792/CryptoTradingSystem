"""Bounded joined-channel inventory; no message or unrelated-dialog output."""
import asyncio
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts import telegram_desktop_bridge as bridge

STUDY = Path.home() / '.crypto_trading_system/research/telegram_new_kols_20261025'


async def discover():
    from telethon.sessions import StringSession
    from telethon.tl.types import Channel
    selection = json.loads((STUDY / 'selection.json').read_text(encoding='utf-8'))
    known = {row['channel_id']: row for row in selection['channels']}
    since = datetime.fromisoformat('2026-10-25T05:27:00+00:00')
    secret = json.loads(bridge.dpapi(bridge.store_path().read_bytes(), decrypt=True))
    client = bridge.make_client(StringSession(secret['session']), secret['api_id'], secret['api_hash'])
    try:
        await bridge.connect(client)
        identity = await client.get_me()
        if not identity or identity.id != secret['account_id']:
            raise RuntimeError('Account identity mismatch')
        found, n = [], 0
        async for dialog in client.iter_dialogs(limit=500):
            n += 1
            entity = dialog.entity
            if (not isinstance(entity, Channel) or not entity.broadcast or entity.left
                    or not entity.date or entity.date < since):
                continue
            previous = known.get(entity.id)
            found.append({'slug': previous['slug'] if previous else (entity.username or 'new_channel_' + str(len(found))),
                'title': entity.title, 'username': entity.username, 'channel_id': entity.id,
                'joined_at': entity.date.isoformat(), 'previously_researched': bool(previous)})
        result = {'at_utc': datetime.now(timezone.utc).isoformat(), 'since': since.isoformat(),
            'identity_verified': True, 'dialog_cap_reached': n >= 500,
            'channels': sorted(found, key=lambda row: row['joined_at']),
            'unrelated_dialogs_recorded': False, 'read_only': True}
        output = STUDY / ('discovery_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '.json')
        with output.open('x', encoding='utf-8') as stream:
            json.dump(result, stream, ensure_ascii=False, indent=2)
        public = {**result, 'channels': [{k: v for k, v in row.items() if k != 'channel_id'} for row in result['channels']]}
        print(json.dumps(public, ensure_ascii=True, indent=2))
    finally:
        await client.disconnect()


if __name__ == '__main__':
    logging.disable(logging.CRITICAL)
    try:
        with bridge.session_lock():
            asyncio.run(asyncio.wait_for(discover(), timeout=65))
    except Exception as error:
        print(json.dumps({'status': 'failed', 'error_type': type(error).__name__,
                          'cooldown_seconds': getattr(error, 'seconds', None)}))
        raise SystemExit(1)
