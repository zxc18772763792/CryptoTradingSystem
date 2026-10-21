from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from core.research import event_risk_monitor as monitor, event_risk_shadow as risk
from core.research import announcement_short_tracker as announcement
from core.research import listing_short_tracker as listing, unlock_short_tracker as unlock

NOW = datetime(2026, 10, 20, 6, tzinfo=timezone.utc)


def provider(tmp_path, monkeypatch, adapter):
    path = tmp_path / 'tracker.json'
    state = {'updated_at': 'baseline-observation', 'trades': {}}
    path.write_text(json.dumps(state), encoding='utf8')
    saves = []
    def load(path): return json.loads(path.read_text(encoding='utf8'))
    def save(state, path):
        path.write_text(json.dumps(state), encoding='utf8')
        saves.append(json.loads(path.read_text(encoding='utf8')))
    monkeypatch.setattr(monitor, 'providers', lambda: [('test', path, load, save, adapter)])
    monkeypatch.setattr(monitor, '_now', lambda: NOW)
    return path, saves


def test_registration_is_persisted_before_first_observation_and_baseline_time_is_preserved(tmp_path, monkeypatch):
    sources = [{'source_id': 'old', 'source_entry_ms': 1, 'source_status': 'open'},
               {'source_id': 'future', 'source_entry_ms': int(NOW.timestamp()*1000)+60000, 'source_status': 'waiting_entry'}]
    path, saves = provider(tmp_path, monkeypatch, lambda state: sources)
    async def apply(client, state, sources, now, **kwargs):
        persisted = json.loads(path.read_text(encoding='utf8'))
        assert persisted[risk.STATE_KEY]['excluded_source_ids'] == ['old']
        assert state[risk.STATE_KEY]['config']['funding_budget_pct'] == 2
        return {'registered': True}
    monkeypatch.setattr(risk, 'apply_to_tracker', apply)
    result = asyncio.run(monitor.poll(object()))
    assert result['trackers']['test']['registered']
    assert len(saves) == 2 and saves[-1]['updated_at'] == 'baseline-observation'


def test_pending_exit_is_saved_even_if_observation_raises(tmp_path, monkeypatch):
    path, saves = provider(tmp_path, monkeypatch, lambda state: [])
    async def apply(client, state, sources, now, **kwargs):
        state[risk.STATE_KEY]['trades']['x'] = {'status': 'closing_pending_funding'}
        raise ConnectionError('funding not published')
    monkeypatch.setattr(risk, 'apply_to_tracker', apply)
    out = asyncio.run(monitor.poll(object()))
    assert out['trackers']['test']['status'] == 'unavailable'
    assert json.loads(path.read_text(encoding='utf8'))[risk.STATE_KEY]['trades']['x']['status'] == 'closing_pending_funding'


def test_bad_source_adapter_still_runs_frozen_shadow_positions(tmp_path, monkeypatch):
    def broken(state): raise ValueError('one malformed source date')
    path, saves = provider(tmp_path, monkeypatch, broken)
    called=[]
    async def apply(client, state, sources, now, **kwargs):
        called.append(sources)
        return {'open': 1}
    monkeypatch.setattr(risk, 'apply_to_tracker', apply)
    out=asyncio.run(monitor.poll(object()))
    assert called == [[]]
    assert 'source_adapter_error' in out['trackers']['test']


def test_event_source_timestamps_and_frozen_hedge_are_prospective():
    unlock_state={'trades':{'u':{'symbol':'APRUSDT','entry_day':'2026-10-21','exit_day':'2026-10-27',
                               'risk_hedge_symbols':['BTCUSDT'],'risk_hedge_frozen_at':'2026-10-20T00:00:00+00:00',
                               'status':'scheduled','late':False}}}
    u=unlock.risk_shadow_sources(unlock_state)[0]
    assert u['source_entry_ms'] == int(datetime(2026,10,22,tzinfo=timezone.utc).timestamp()*1000)
    assert u['source_exit_due_ms'] == int(datetime(2026,10,28,tzinfo=timezone.utc).timestamp()*1000)
    assert u['hedge_symbols']==['BTCUSDT'] and u['eligible']
    launch=int(datetime(2026,10,21,13,30,tzinfo=timezone.utc).timestamp()*1000)
    l=listing.risk_shadow_sources({'trades':{'p':{'symbol':'ABCUSDT','onboard_ms':launch,'status':'waiting_d2'}}})[0]
    assert l['source_entry_ms']==int(datetime(2026,10,24,tzinfo=timezone.utc).timestamp()*1000)
    assert l['source_exit_due_ms']==int(datetime(2026,11,5,tzinfo=timezone.utc).timestamp()*1000)


def test_notice_feed_outage_cannot_discard_risk_observation(tmp_path, monkeypatch):
    monkeypatch.setattr(announcement, '_now', lambda: NOW)
    async def feed(client): raise ConnectionError('notice feed down')
    async def apply(client, state, sources, now, **kwargs):
        state[risk.STATE_KEY]['trades']['pending']={'status':'closing_pending_funding'}
        return {'closing_pending_funding':1}
    monkeypatch.setattr(announcement, '_poll', feed)
    monkeypatch.setattr(risk, 'apply_to_tracker', apply)
    with pytest.raises(ConnectionError):
        asyncio.run(announcement.tick(object(), state_dir=tmp_path))
    for spec in announcement.KINDS.values():
        state=json.loads((tmp_path/spec['state_path'].name).read_text(encoding='utf8'))
        assert state[risk.STATE_KEY]['trades']['pending']['status']=='closing_pending_funding'


def test_new_notice_uses_clock_after_risk_and_feed_work_not_tick_start(tmp_path, monkeypatch):
    clock=[NOW]
    observed=[]
    monkeypatch.setattr(announcement, '_now', lambda: clock[0])
    async def apply(client, state, sources, now, **kwargs): return {}
    async def feed(client):
        clock[0] += timedelta(minutes=45)
        return [{'code':'new','title':'Binance Will Extend the Monitoring Tag to Include AAA',
                 'releaseDate':int((clock[0]-timedelta(seconds=10)).timestamp()*1000)}]
    async def open_trade(client, kind, token, article, perps, now):
        observed.append(now)
        return {'status':'no_perp'}
    class Response:
        def raise_for_status(self): pass
        def json(self): return {'symbols':[]}
    class Client:
        async def get(self, *args, **kwargs): return Response()
    monkeypatch.setattr(risk, 'apply_to_tracker', apply)
    monkeypatch.setattr(announcement, '_poll', feed)
    monkeypatch.setattr(announcement, '_open_trade', open_trade)
    asyncio.run(announcement.tick(Client(), state_dir=tmp_path))
    assert observed == [NOW+timedelta(minutes=45)]
