from datetime import timedelta
import json
import pytest
from core.research import alastar_horizon_tracker as v3
from core.research import alastar_forward_paper as v2
from core.news.alastar_observation import observe_records
from tests.test_alastar_forward_paper import setup as v2_setup, dt, rows, record


@pytest.fixture
def setup(v2_setup,monkeypatch):
    old,label,clock=v2_setup
    monkeypatch.setattr(v3,'now_utc',lambda:clock[0])
    protocol=old.output.parent/'v3.json'
    protocol.write_text(json.dumps({'protocol_id':'alastar_paired_horizons_v3','holding_hours':list(v3.HOURS),'cost_bps':20,'minimum_null_starts':2000}),encoding='utf-8')
    archive=old.output.parent/'v3archive'
    for minute in ('00:55','00:56','00:57','00:58'):
        observe_records([],archive,mode='live_public_preview',observed_at=f'2026-10-21T{minute}:00Z',raw_preview=b'public fixture')
    j=v3.Journal(old.output.parent/'v3',archive,protocol)
    clock[0]=dt('2026-10-21T00:58:30Z')
    j.activate({'ok':True,'audit_sha256':'synthetic','implementation_sha256':j.implementation_sha},{'ok':True})
    for minute in ('01:01','01:02'):
        observe_records([record()],archive,mode='live_public_preview',observed_at=f'2026-10-21T{minute}:00Z',raw_preview=b'public source')
    clock[0]=dt('2026-10-21T01:02:30Z')
    return j,label,clock,old


class Provider:
    def __init__(self): self.calls=0; self.funding_ok=True; self.clock_ok=True
    def clock_check(self): return {'ok':self.clock_ok}
    def klines(self,symbol,start,end):
        self.calls+=1; data=rows(start,int((end-start)/v2.STEP)+1)
        if end-start<=timedelta(hours=168) and len(data)>12: data[12][1:5]=[110.,110.,110.,110.]
        return data,[{'synthetic':True}]
    def funding(self,symbol,start,end):
        if not self.funding_ok: raise RuntimeError('missing funding')
        return [],{'complete_query_coverage':True}


def test_null_profiles_match_frozen_v2_and_use_no_future_data():
    entry=dt('2026-10-21T01:05:00Z'); raw=rows(entry-timedelta(days=30),8641,.00001)
    result=v3.null_profile(raw,entry,1)
    old=v2.pre_entry_null(raw,entry,1)
    assert result['horizons']['1']['n']==old['n']==8629
    assert result['horizons']['1']['mean']==pytest.approx(old['mean_net_price_only'])
    assert result['common_n']==6625
    more=rows(entry+v2.STEP,20,.5)
    assert v3.null_profile(raw+more,entry,1)==result


@pytest.mark.parametrize('side',[1,-1])
def test_null_gaps_and_zero_volume_never_interpolate(side):
    entry=dt('2026-10-21T01:05:00Z'); raw=rows(entry-timedelta(days=30),8641)
    original=v3.null_profile(raw,entry,side)
    missing=v3.null_profile(raw[:4000]+raw[4001:],entry,side)
    assert missing['common_n']<original['common_n']
    assert missing['horizons']['1']['n']==8629-13
    assert missing['horizons']['168']['mean']==pytest.approx(-.002)


def test_liquidation_before_gap_and_endpoint_future_high_handled_like_v2():
    at=dt('2026-10-21T01:05:00Z'); raw=rows(at,49)
    raw[2][2]=200.
    assert v3.price_outcome(v2.bars_by_time(raw[:3]),at,4,-1)['net_price_only']==-1.
    raw=rows(at,13); raw[-1][2]=1e6
    assert v3.price_outcome(v2.bars_by_time(raw),at,1,-1)['net_price_only']==pytest.approx(-.002)


@pytest.mark.parametrize('hours', v3.HOURS)
def test_short_squeeze_then_profitable_exit_stays_total_loss(hours):
    at=dt('2026-10-21T01:05:00Z'); raw=rows(at,hours*12+1)
    raw[2][2]=210.
    raw[-1][1:5]=[70.,70.,70.,70.]
    # A terminal-only short calculation would claim +29.8%; the path lost first.
    outcome=v3.price_outcome(v2.bars_by_time(raw),at,hours,-1)
    assert outcome['net_price_only']==-1.
    assert outcome['liquidated'] is True
    assert outcome['liquidation_bar_start_ms']==raw[2][0]


def test_freeze_all_exits_before_entry_and_preserve_v2(setup):
    j,label,clock,old=setup; before=(old.output/'journal/000000000001.json').read_bytes()
    event=j.freeze(label)['payload']['events'][0]
    assert event['planned_entry_at']=='2026-10-21T01:05:00+00:00'
    assert set(event['planned_exits'])=={'1','4','24','72','168'}
    assert event['planned_exits']['168']=='2026-10-28T01:05:00+00:00'
    with pytest.raises(ValueError,match='already'): j.freeze(label)
    assert (old.output/'journal/000000000001.json').read_bytes()==before
    assert len(old.status()['events'])==0


@pytest.mark.parametrize('change',[{'label_frozen_at_utc':'2020-01-01Z'},{'events':None}])
def test_malformed_or_external_timestamps_rejected(setup,change):
    j,label,_,_=setup; label.update(change)
    with pytest.raises(ValueError): j.freeze(label)


def test_late_label_is_retained_as_skip_not_past_fill(setup):
    j,label,clock,_=setup; clock[0]+=timedelta(minutes=31)
    event=j.freeze(label)['payload']['events'][0]
    assert event['status']=='skipped' and 'entry_too_late' in event['exclusion_reasons']


def test_idle_never_calls_market_and_bad_clock_cannot_fill(setup):
    j,label,clock,_=setup; j.freeze(label); p=Provider(); r=v3.runtime(j.output)
    assert j.tick(lambda:p,r)==[] and p.calls==0
    clock[0]=dt('2026-10-21T01:05:01Z'); p.clock_ok=False
    assert j.tick(lambda:p,r)[0]['kind']=='entry_unavailable' and p.calls==0
    clock[0]+=timedelta(seconds=31)
    assert j.tick(lambda:p,r)[0]['kind']=='missed'


def test_all_horizons_share_one_entry_and_maturity_cannot_favour_short_holds(setup):
    j,label,clock,_=setup; j.freeze(label); p=Provider(); r=v3.runtime(j.output)
    clock[0]=dt('2026-10-21T01:05:01Z'); assert j.tick(lambda:p,r)[0]['kind']=='entry'
    clock[0]=dt('2026-10-21T02:11:00Z'); assert j.tick(lambda:p,r)[0]['kind']=='settlement'
    assert v3.summary(j)['horizon_comparison'][0]['completed_n']==1
    assert all(x['common_n']==0 and x['mean_net_pct'] is None for x in v3.summary(j)['horizon_comparison'])
    assert j.tick(lambda:p,r)[0]['kind']=='null'
    for h in v3.HOURS[1:]:
        clock[0]=dt('2026-10-21T01:05:00Z')+timedelta(hours=h,minutes=6)
        assert j.tick(lambda:p,r)[0]['kind']=='settlement'
    view=v3.summary(j)
    assert view['retirement']['n']==1 and view['retirement']['mean_pct'] is None
    assert all(x['common_n']==1 for x in view['horizon_comparison'])
    assert view['horizon_comparison'][0]['excess_pp']==pytest.approx(10.)
    assert view['horizon_comparison'][-1]['gain_vs_1h_pp']==pytest.approx(-10.)


def test_missing_funding_does_not_turn_into_zero_or_block_price_observation(setup):
    j,label,clock,_=setup; j.freeze(label); p=Provider(); r=v3.runtime(j.output)
    clock[0]=dt('2026-10-21T01:05:01Z'); j.tick(lambda:p,r)
    clock[0]=dt('2026-10-21T02:11:00Z'); p.funding_ok=False
    settled=j.tick(lambda:p,r)[0]
    assert settled['kind']=='settlement' and settled['payload']['net_with_funding'] is None
    path=j.output/'journal'/f"{settled['sequence']:012d}.json"; raw=path.read_bytes()
    j.tick(lambda:p,r); j.tick(lambda:p,r)
    clock[0]+=timedelta(seconds=61); p.funding_ok=True
    assert j.tick(lambda:p,r)[0]['kind']=='funding'
    assert path.read_bytes()==raw


def test_common_cohort_excludes_same_symbol_for_168h(setup):
    j,label,clock,_=setup; j.freeze(label); p=Provider(); r=v3.runtime(j.output)
    new=record(2)
    observe_records([new],j.archive,mode='live_public_preview',observed_at='2026-10-21T01:02:01Z',raw_preview=b'new source')
    label['source_id']=new['source_id']; j.freeze(label)
    clock[0]=dt('2026-10-21T01:05:01Z'); actions=j.tick(lambda:p,r)
    assert [a['kind'] for a in actions]==['entry','missed']
    assert actions[-1]['payload']['reason']=='common_cohort_same_symbol_168h_overlap'


def test_protocol_drift_and_checkpoint_tampering_stop_processing(setup):
    j,_,_,_=setup; j.protocol_path.write_bytes(j.protocol_path.read_bytes()+b' ')
    with pytest.raises(RuntimeError,match='drift'): j._state()


def test_tail_tamper_is_detected(setup):
    j,_,_,_=setup; p=j.output/'journal/000000000001.json'; p.write_bytes(p.read_bytes()+b' ')
    with pytest.raises(RuntimeError,match='checkpoint'): j._state()


def test_multi_horizon_futility_is_independent_of_1h_loss(setup):
    j,_,_,_=setup; records,_=j._state()
    def completed(n,positive72):
        return {str(i):{'event_id':str(i),'status':'completed','entry':{'entry_at':'2026-10-21T01:05:00Z'},
                       'outcomes':{str(h):{'net_price_only':.01 if h==72 and positive72 else -.01} for h in v3.HOURS},
                       'null':{'common_status':'ok','horizons':{str(h):{'common_mean':0.} for h in v3.HOURS}}} for i in range(n)}
    assert j.gate(records,completed(40,True)) is None
    assert j.gate(records,completed(40,False))=='40cohort_all_horizons_futility_stop'
    assert j.gate(records,completed(66,True))=='66cohort_review_due'


def test_clock_response_crossing_entry_deadline_stays_retryable_then_misses(setup):
    j,label,clock,_=setup; j.freeze(label); r=v3.runtime(j.output)
    class Slow(Provider):
        def clock_check(self):
            clock[0]+=timedelta(seconds=31)
            return {'ok':True}
    clock[0]=dt('2026-10-21T01:05:01Z')
    assert j.tick(lambda:Slow(),r)[0]['kind']=='entry_unavailable'
    assert j.tick(lambda:Provider(),r)[0]['kind']=='missed'


def test_tracker_idle_manual_and_stale_status_do_not_claim_managed_running(setup):
    j,_,clock,_=setup
    runner=v3.Tracker(journal=j,provider_factory=lambda:pytest.fail('Idle must not access network'))
    runner.tick(); assert not v3.summary(j)['runner_active']
    runner.managed=True; runner.tick(); assert v3.summary(j)['runner_active']
    clock[0]+=timedelta(seconds=31); assert not v3.summary(j)['runner_active']


@pytest.mark.parametrize('change',[{'confidence':.64},{'is_fresh_entry_view':False},{'reaction_type':'context_opinion'},
                                  {'requires_unavailable_confirmation':True},{'opposed_directions_in_message':True},{'chart_only':True}])
def test_only_unambiguous_fresh_text_can_enter_v3(setup,change):
    j,label,_,_=setup; label['events'][0].update(change)
    assert j.freeze(label)['payload']['events'][0]['status']=='skipped'


def test_exact_original_quote_is_required(setup):
    j,label,_,_=setup; label['events'][0]['original_evidence_quote']='invented'
    with pytest.raises(ValueError,match='Exact'): j.freeze(label)


@pytest.mark.parametrize('side',[1,-1])
def test_all_null_horizons_match_brute_force_with_gaps_and_known_losses(side):
    entry=dt('2026-10-21T01:05:00Z'); start=entry-timedelta(minutes=5*2116)
    raw=rows(start,2117,.00001); raw[2][2]=210.; raw[2][3]=.01
    raw=raw[:700]+raw[701:]
    bars=v2.bars_by_time(raw); result=v3.null_profile(raw,entry,side,minimum=1)
    common=None
    for h in v3.HOURS:
        values={}
        for i in range(2117-h*12):
            at=start+i*v2.STEP; r=v3.price_outcome(bars,at,h,side)
            if r is not None: values[i]=r['net_price_only']
        expected=sum(values.values())/len(values) if values else None
        assert result['horizons'][str(h)]['n']==len(values)
        assert result['horizons'][str(h)]['mean']==pytest.approx(expected) if expected is not None else result['horizons'][str(h)]['mean'] is None
        common=set(values) if common is None else common & set(values)
    assert result['common_n']==len(common)
