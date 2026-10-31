import pytest

from scripts.research_korea_composite import DAY, HOUR, decisions, market_features, news_exit, pre_funding, summary

ENTRY = 1_800_000_000_000//DAY*DAY


def test_pre_funding_uses_buffer_and_never_future_settlements():
    rows = [{'fundingTime':ENTRY-h*HOUR,'fundingRate':-.001} for h in [24,16,8]]
    rows += [{'fundingTime':ENTRY,'fundingRate':100}]
    f = pre_funding(rows,ENTRY)
    assert f['funding_pre24h']==pytest.approx(-.003)
    assert f['funding_pre_last_ms']<ENTRY-300000


def test_missing_interior_funding_and_conflicting_duplicate_are_unknown():
    rows = [{'fundingTime':ENTRY-h*HOUR,'fundingRate':.001} for h in [24,16,8]]
    assert pre_funding([rows[0],rows[2]],ENTRY) is None
    assert pre_funding(rows+[dict(rows[0],fundingRate=.002)],ENTRY) is None
    assert pre_funding([],ENTRY) is None


def test_missing_features_do_not_become_negative_or_positive_signals():
    d = decisions({'extension':False})
    assert d['funding_nonnegative'] is None
    assert d['relative_weakness'] is None
    assert d['relative_weakness_and_funding'] is None
    assert decisions({'extension':False,'relative_day0':.1})['relative_weakness_and_funding'] is False


def test_market_feature_uses_exact_previous_days_not_future_or_nearest_fill():
    day = ENTRY-DAY
    d = {'COINUSDT':{str(day):[1,10],str(day-DAY):[2,10],str(day+DAY):[100,10]},
         'BTCUSDT':{str(day):[1,10],str(day-DAY):[1,10],str(day-7*DAY):[2,10]}}
    f = market_features(d,'COINUSDT',day)
    assert f['relative_day0']==-.5 and f['btc_pre7d']==-.5
    del d['COINUSDT'][str(day-DAY)]
    assert 'relative_day0' not in market_features(d,'COINUSDT',day)


def test_opportunity_denominator_and_unknown_bounds_are_explicit():
    rows = [{'date':'2026-01-01','episode':str(i),'short_return':r,
             'decisions':{'a':decision}} for i,(r,decision) in enumerate([(.1,True),(.2,False),(-.3,None)])]
    s = summary(rows,'a')
    assert s['selected']==1 and s['unknown_feature']==1 and s['rejected']==1
    assert s['mean_selected_pct']==10
    assert s['per_opportunity_pct_unknown_no_entry']==pytest.approx(10/3)
    assert s['unknown_selection_bounds_per_opportunity_pct']==pytest.approx([-20/3,10/3])
    assert s['rejected_profitable_n']==1


def bars_and_funding():
    bars = [[ENTRY+i*300000,'1','1.05','.9','.95','100'] for i in range(5)]
    funding = [{'fundingTime':ENTRY+1,'fundingRate':.001}]
    return bars,funding


def test_news_exit_uses_receipt_not_earlier_publication_and_charges_costs():
    bars, funding = bars_and_funding()
    received = ENTRY+7*60000
    r = news_exit(bars,funding,ENTRY,1,received)
    assert r['candidate_exit_ms']==ENTRY+10*60000
    assert r['net_short_leg_pct']==pytest.approx(0)
    stressed = news_exit(bars,funding,ENTRY,1,received,side_stress=.01)
    assert stressed['net_short_leg_pct']<r['net_short_leg_pct']


def test_stop_already_hit_cannot_be_replaced_with_later_news_fill():
    bars, funding = bars_and_funding()
    bars[1][2]='1.41'
    r = news_exit(bars,funding,ENTRY,1,ENTRY+7*60000)
    assert r['eligible_news_exit'] is False
    assert r['first_stop_5m_ms']==ENTRY+300000


@pytest.mark.parametrize('problem',['gap','duplicate_funding','pre_entry_message'])
def test_invalid_news_scenarios_fail_explicitly(problem):
    bars,funding = bars_and_funding()
    received = ENTRY+7*60000
    if problem=='gap':
        bars.pop(1)
    elif problem=='duplicate_funding':
        funding += funding.copy()
    else:
        received = ENTRY-1
    with pytest.raises(ValueError):
        news_exit(bars,funding,ENTRY,1,received)
