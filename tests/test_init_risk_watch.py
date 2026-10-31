from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from core.research.init_risk_watch import HOUR, conditions, features, transition

NOW = 1_800_000_000_000


def inputs():
    # End exactly on an hour; no partially formed candle is usable.
    end = NOW//HOUR*HOUR
    bars = [[end-(25-i)*HOUR, '1', '1', '1', '1', '1', end-(24-i)*HOUR-1, '100'] for i in range(25)]
    bars[-1][4] = '1.12'
    bars[-1][7] = '250'
    return {'klines':bars,
            'oi':[{'timestamp':end-HOUR,'sumOpenInterest':'100'}, {'timestamp':end,'sumOpenInterest':'110.1'}],
            'top_positions':[{'timestamp':end-HOUR,'longAccount':'0.5'}, {'timestamp':end,'longAccount':'0.56'}],
            'mark':{'symbol':'INITUSDT','time':NOW,'lastFundingRate':'-0.0011','nextFundingTime':NOW+HOUR},
            'rpc':{'result':{'node_info':{'network':'interwoven-1'},'sync_info':{
                'latest_block_time':datetime.fromtimestamp((NOW-1000)/1000,timezone.utc).isoformat(),
                'latest_block_height':'123'}}}}


def test_complete_windows_and_thresholds():
    f, issues = features(inputs(), NOW)
    assert issues == []
    assert f['price_1h_pct'] == pytest.approx(12)
    assert f['oi_1h_pct'] == pytest.approx(10.1)
    assert set(conditions(f)) == {'price_volume','oi_expansion','top_position_shift','squeeze_risk'}


def test_open_and_future_candles_cannot_create_alerts():
    d = inputs()
    d['klines'].append([NOW, '1','100','1','100','1',NOW+HOUR-1,'100000'])
    f, _ = features(d, NOW)
    assert f['price'] == 1.12
    d['oi'].append({'timestamp':NOW+HOUR,'sumOpenInterest':'99999'})
    f, _ = features(d, NOW)
    assert f['oi_contract_units'] == 110.1


def test_response_captured_before_hour_close_stays_partial_after_slow_x_poll():
    d = inputs()
    end = NOW//HOUR*HOUR
    # At receipt, the final bar in this fixture still had one minute to go.
    received = end-60_000
    # Supply one more preceding bar so the valid earlier window is complete.
    first = d['klines'][0]
    d['klines'].insert(0,[first[0]-HOUR,'1','1','1','1','1',first[6]-HOUR,'100'])
    f, issues = features(d,end+60_000,{'klines':received})
    assert f['price_window_end_ms']==end-HOUR
    assert f['price']==1
    assert 'price_volume' not in conditions(f)


@pytest.mark.parametrize('kind', ['missing_bar','old_price','missing_oi_hour','wrong_chain'])
def test_unknown_inputs_do_not_become_zero_or_valid_signals(kind):
    d = inputs()
    if kind=='missing_bar':
        d['klines'].pop(10)
    elif kind=='old_price':
        d['klines'] = [[r[0]-5*HOUR,*r[1:6],r[6]-5*HOUR,r[7]] for r in d['klines']]
    elif kind=='missing_oi_hour':
        d['oi'][0]['timestamp'] -= HOUR
    else:
        d['rpc']['result']['node_info']['network'] = 'another-chain'
    f, issues = features(d, NOW)
    assert issues
    if kind in ['missing_bar','old_price']:
        assert 'price_1h_pct' not in f and 'price_volume' not in conditions(f)
    elif kind=='missing_oi_hour':
        assert 'oi_1h_pct' not in f
    else:
        assert 'chain_height' not in f


def test_conflicting_duplicates_are_rejected_and_not_overwritten():
    d = inputs()
    d['oi'].append({**d['oi'][-1], 'sumOpenInterest':'1'})
    f, issues = features(d, NOW)
    assert 'oi_unavailable' in issues and 'oi_contract_units' not in f


def test_notional_inflation_does_not_look_like_new_positions():
    d = inputs()
    d['oi'][1]['sumOpenInterest'] = '100'
    d['oi'][0]['sumOpenInterestValue'] = '100'
    d['oi'][1]['sumOpenInterestValue'] = '200'
    f, _ = features(d, NOW)
    assert f['oi_1h_pct'] == 0
    assert 'oi_expansion' not in conditions(f)


def test_alerts_do_not_repeat_when_unchanged_or_within_cooldown():
    state, first = transition({}, {'x':'message'}, NOW)
    assert len(first)==1
    state, second = transition(state, {'x':'message'}, NOW+HOUR)
    assert second==[]
    state, _ = transition(state, {}, NOW+2*HOUR)
    state, third = transition(state, {'x':'message'}, NOW+3*HOUR)
    assert third==[]
    state, _ = transition(state, {}, NOW+6*HOUR)
    _, fourth = transition(state, {'x':'message'}, NOW+7*HOUR)
    assert len(fourth)==1


def test_cycle_preserves_receipts_and_old_source_posts_are_baseline(tmp_path, monkeypatch):
    import scripts.watch_init_risk as watch
    d = inputs()
    # Deliberately old market data: test degradation, never fabricated freshness.
    async def collect():
        return {k:{'body':v} for k,v in d.items()} | {'upbit_notice':{'body':{'data':{'body':'same notice'}}}}
    xcalls = []
    def x():
        xcalls.append(True)
        return {'received_at_utc':datetime.now(timezone.utc).isoformat(), 'failed_handles':[],
                'posts':[{'handle':'initia','url':'https://x.com/initia/status/1',
                          'published_at':'2026-01-01T00:00:00Z','text':'old restart news'}]}
    monkeypatch.setattr(watch, 'collect', collect)
    monkeypatch.setattr(watch, 'collect_x', x)
    first = watch.run(tmp_path)
    before = Path(first['receipts_path']).read_bytes()
    second = watch.run(tmp_path)
    assert xcalls==[True]
    assert before == Path(first['receipts_path']).read_bytes()
    assert first['receipts_path'] != second['receipts_path']
    assert not any(r['kind']=='source_update' for r in first['alerts'])
    assert second['alerts']==[]
    state = json.loads((tmp_path/'state.json').read_bytes())
    assert state['latest_result_path'].endswith('result.json')
