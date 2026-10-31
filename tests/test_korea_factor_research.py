from datetime import datetime,timedelta,timezone
import pytest
from core.research.korea_factor_policy import assess
from scripts.research_korea_factor_selection import volume_multiple
from scripts.research_korea_attention_cooling import delayed_short,confirmation
from scripts.research_korea_factor_benchmark import matched_price_reference

DAY=86_400_000
D=datetime(2026,10,30,tzinfo=timezone.utc)
DECISION=(D+timedelta(minutes=5)).isoformat()


def context():
    return {'provenance':'live','venue':'upbit','kind':'caution','asset_mapping_verified':True,
            'published_at_utc':(D-timedelta(hours=12)).isoformat(),
            'received_at_utc':(D-timedelta(hours=11)).isoformat(),
            'classified_at_utc':(D-timedelta(hours=10)).isoformat(),
            'market_received_at_utc':(D+timedelta(minutes=1)).isoformat(),
            'market_window_end_utc':D.isoformat(),'pre_entry_funding_observed':True,
            'pre_entry_funding_received_at_utc':(D+timedelta(minutes=1)).isoformat(),
            'transfer_status_received_at_utc':(D+timedelta(minutes=1)).isoformat(),
            'transfer_status_event_at_utc':(D+timedelta(minutes=1)).isoformat(),
            'fee_inputs_known':True,'reason':'project_or_team_issue','transfer_status':'deposit_only_suspended',
            'relative_notice_day_return':-.02,'latest_funding_rate':-.005}


def test_negative_carry_not_used_as_hard_filter_and_no_execution():
    r=assess(context(),DECISION);assert r['state']=='candidate_for_shadow'
    assert r['creates_order'] is r['creates_paper_trade'] is r['activates_strategy'] is False
    assert r['performance_claim'] is None


@pytest.mark.parametrize('field,value,state',[
    ('provenance','backfill','retrospective_only'),('reason','security_incident','incident_observation'),
    ('transfer_status','both_suspended','defer'),('transfer_status','unknown','defer'),
    ('asset_mapping_verified',False,'defer'),('pre_entry_funding_observed',False,'defer'),
    ('relative_notice_day_return',.01,'observe'),('same_episode_position_open',True,'observe'),
    ('kind','listing_krw','reference_only')])
def test_evidence_gates_and_event_branches(field,value,state):
    c=context();c[field]=value;assert assess(c,DECISION)['state']==state


def test_partial_receipt_and_late_entry_are_ineligible():
    c=context();c['market_received_at_utc']=(D-timedelta(seconds=1)).isoformat()
    assert assess(c,DECISION)['state']=='defer'
    assert assess(context(),(D+timedelta(hours=2)).isoformat())['state']=='missed'


def test_related_vault_reopening_is_not_issuer_remediation():
    c=context();claim={'same_asset':True,'same_incident':True,'actor_role':'related_protocol','stage':'wallet_resumed',
        'received_at_utc':(D-timedelta(hours=2)).isoformat(),'reviewed_at_utc':(D-timedelta(hours=1)).isoformat()}
    c['updates']=[claim];assert assess(c,DECISION)['state']=='candidate_for_shadow'
    claim['actor_role']='issuer';assert assess(c,DECISION)['state']=='risk_review'
    claim['reviewed_at_utc']=(D+timedelta(minutes=6)).isoformat()
    assert assess(c,DECISION)['state']=='defer'


def test_volume_requires_twenty_past_days_and_ignores_future():
    day=100*DAY;data={'A':{str(day-i*DAY):[1,10] for i in range(1,21)}}
    data['A'][str(day+DAY)]=[1,1e10]
    assert volume_multiple(data,'A',day,30)==3
    del data['A'][str(day-7*DAY)];assert volume_multiple(data,'A',day,30) is None


def sample_path():
    return ([[i*DAY,'1','1.05','.8','1','100',(i+1)*DAY-1,'100'] for i in range(8)],
            [{'fundingTime':i*DAY+1,'fundingRate':.001} for i in range(1,8)])


def test_delayed_hold_new_entry_funding_and_stop_window():
    bars,rates=sample_path();bars[1][4]='.9';bars[4][2]='1.3';r=delayed_short(bars,rates,0)
    assert r['entry_ms']==2*DAY and r['exit_ms']==5*DAY and r['stopped']
    assert r['funding_during_hold']==pytest.approx(.003)
    assert r['short_return']==pytest.approx(1-1.4*1.02-.001+.003)
    assert r['stress_return']<r['short_return']


def test_missing_funding_and_misaligned_confirmation_stay_unknown():
    bars,_=sample_path()
    with pytest.raises(ValueError):delayed_short(bars,[],0)
    d=confirmation(bars[1:],{'0':[1,1],str(DAY):[1,1]},0)
    assert d['day1_weak'] is None


def test_same_coin_reference_ends_before_notice_and_ignores_future_prices():
    day=100*DAY;history={str(day-i*DAY):[1,10] for i in range(1,38)}
    history[str(day)]=[1e9,10];history[str(day+7*DAY)]=[1e-9,10]
    r=matched_price_reference(history,day)
    assert r['reference_n']==30 and r['reference_mean']==pytest.approx(-.001)
    assert r['latest_reference_bar_open_ms']<day


def test_missing_same_coin_reference_is_unknown_not_a_smaller_selected_control():
    day=100*DAY;history={str(day-i*DAY):[1,10] for i in range(1,38)}
    del history[str(day-30*DAY)]
    assert matched_price_reference(history,day) is None


def test_today_retrieval_of_an_old_wallet_announcement_is_not_live_status():
    c=context();c['transfer_status_event_at_utc']=(D-timedelta(days=2)).isoformat()
    assert assess(c,DECISION)['state']=='defer'


def test_future_funding_receipt_cannot_pass_a_pre_entry_gate():
    c=context();c['pre_entry_funding_received_at_utc']=(D+timedelta(minutes=6)).isoformat()
    assert assess(c,DECISION)['state']=='defer'
