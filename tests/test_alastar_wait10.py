import pandas as pd
import pytest

from scripts.research_alastar_wait10 import Bars,cell,freeze_plan


def flat(minutes=5):
    return pd.DataFrame({'open':100.,'high':100.,'low':100.,'close':100.,'volume':1.},
                        index=pd.date_range('2026-01-01',periods=int(193*60/minutes)+1,freq=f'{minutes}min',tz='UTC'))


def test_touch_cannot_fill_before_next_open_or_at_the_touch_price():
    f=flat(); f.iloc[2,f.columns.get_loc('high')]=111.
    f.iloc[3,:4]=[107.,107.,107.,107.]
    result=Bars(f).policy(0,1)
    assert result['entry_at']==str(f.index[3])
    assert result['entry_price']==107.
    assert result['entry_improvement']==pytest.approx(.07)


def test_exact_ten_percent_touch_is_inclusive_and_just_below_is_not():
    f=flat(); f.iloc[2,f.columns.get_loc('high')]=110.
    assert Bars(f).policy(0,1)['status']=='filled'
    f.iloc[2,f.columns.get_loc('high')]=109.999
    assert Bars(f).policy(0,1)['status']=='unfilled'


def test_no_touch_is_zero_but_gap_during_wait_is_unknown():
    f=flat()
    result=Bars(f).policy(0,24)
    assert result['status']=='unfilled' and result['net']==0
    f=f.drop(f.index[100])
    assert Bars(f).policy(0,24)['status']=='missing_wait_path'


def test_touch_bar_squeeze_precedes_next_open_entry_but_counts_for_ideal_limit():
    f=flat(); f.iloc[2,f.columns.get_loc('high')]=230.
    b=Bars(f)
    assert b.policy(0,24)['net']==pytest.approx(-.002)
    assert b.policy(0,24,execution='ideal_limit')['net']==-1


def test_postentry_squeeze_then_recovery_never_restores_profit():
    f=flat(); f.iloc[2,f.columns.get_loc('high')]=111.
    f.iloc[4,f.columns.get_loc('high')]=220.
    assert Bars(f).policy(0,24)['net']==-1.


def test_exit_future_high_is_excluded_and_known_loss_survives_later_gap():
    f=flat(); f.iloc[2,f.columns.get_loc('high')]=111.
    end=3+12
    f.iloc[end,f.columns.get_loc('high')]=1e6
    assert not Bars(f).policy(0,1)['liquidated_1x_proxy']
    f.iloc[4,f.columns.get_loc('high')]=220.
    f=f.drop(f.index[6])
    result=Bars(f).policy(0,1)
    assert result['net']==-1. and result['maximum_adverse_rise'] is None


def test_known_loss_survives_a_missing_tail_instead_of_requiring_exit_price():
    f=flat().iloc[:6].copy(); f.iloc[2,f.columns.get_loc('high')]=220.
    result=Bars(f).outcome(0,12)
    assert result['net']==-1. and result['maximum_adverse_rise'] is None
    assert result['exit_at']=='2026-01-01 01:00:00+00:00'
    f.iloc[2,f.columns.get_loc('high')]=100.
    assert Bars(f).outcome(0,12)['status']=='missing_holding_path'


def test_entry_at_wait_deadline_allowed_but_after_deadline_never_fills():
    f=flat(); f.iloc[11,f.columns.get_loc('high')]=111.
    assert Bars(f).policy(0,1,wait=1)['wait_hours']==1.
    f=flat(); f.iloc[12,f.columns.get_loc('high')]=111.
    assert Bars(f).policy(0,1,wait=1)['status']=='unfilled'


def test_fixed_signal_exit_does_not_trade_on_or_after_exit():
    f=flat(); f.iloc[11,f.columns.get_loc('high')]=111.
    assert Bars(f).policy(0,1,wait=24,clock='signal_deadline')['status']=='unfilled'
    assert Bars(f).policy(0,1,wait=24,clock='after_entry')['status']=='filled'


def test_gap_after_last_eligible_touch_does_not_turn_expiry_into_unknown():
    f=flat().drop(flat().index[11])
    result=Bars(f).policy(0,1,wait=24,clock='signal_deadline')
    assert result['status']=='unfilled'


def test_gap_before_first_touch_cannot_be_skipped_to_a_later_fill():
    f=flat(); f.iloc[2,f.columns.get_loc('high')]=111.
    f=f.drop(f.index[1])
    assert Bars(f).policy(0,24)['status']=='missing_wait_path'


def test_exit_clocks_have_distinct_endpoints():
    f=flat(); f.iloc[2,f.columns.get_loc('high')]=111.
    b=Bars(f)
    assert b.policy(0,24,clock='after_entry')['exit_at']==str(f.index[291])
    assert b.policy(0,24,clock='signal_deadline')['exit_at']==str(f.index[288])


@pytest.mark.parametrize('minutes',[5,60])
def test_same_rules_apply_to_hourly_null_and_five_minute_actual(minutes):
    f=flat(minutes); f.iloc[0,f.columns.get_loc('high')]=120.
    result=Bars(f,minutes).policy(0,24)
    assert result['entry_at']==str(f.index[1])


def test_zero_prints_and_invalid_ohlc_not_counted_as_nontrigger():
    f=flat(); f.iloc[1,f.columns.get_loc('volume')]=0.
    assert Bars(f).policy(0,24)['status']=='missing_wait_path'
    f=flat(); f.iloc[1,f.columns.get_loc('low')]=101.
    assert Bars(f).policy(0,24)['status']=='missing_wait_path'


def test_profitable_filled_subset_can_still_be_worse_after_missed_trades():
    def old(net):
        return {'status':'filled','net':net,'maximum_adverse_rise':.1,'liquidated_1x_proxy':False}
    common={'phase':'later','anchor_at':'2026-08-01T00:00:00Z','immediate_funded':None,'delayed_funded':None,'hourly_control':None}
    rows=[{**common,'immediate':old(-.1),'delayed':{**old(.1),'wait_hours':1.,'entry_improvement':.1}},
          {**common,'immediate':old(.3),'delayed':{'status':'unfilled','net':0.}}]
    result=cell(rows,'text_fresh',24,24,'after_entry','next_open','all')
    assert result['paired_n']==2 and result['filled_n']==1
    assert result['delayed_per_fill_mean']==pytest.approx(.1)
    assert result['delayed_per_signal_mean']==pytest.approx(.05)
    assert result['paired_policy_gain']==pytest.approx(-.05)
    assert result['execution_timing_contribution']==pytest.approx(.1)
    assert result['selection_contribution']==pytest.approx(-.15)


def test_plan_reruns_preserve_bytes_and_reject_real_parameter_changes(tmp_path):
    path=tmp_path/'plan.json'
    freeze_plan(path,{'created_at_utc':'first','horizons':(1,4,24),'threshold':.1})
    frozen=path.read_bytes()
    freeze_plan(path,{'created_at_utc':'later','horizons':(1,4,24),'threshold':.1})
    assert path.read_bytes()==frozen
    with pytest.raises(AssertionError,match='plan changed'):
        freeze_plan(path,{'created_at_utc':'later','horizons':(1,4,24),'threshold':.2})
