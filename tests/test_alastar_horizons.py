import numpy as np
import pandas as pd
import pytest
from scripts.research_alastar_horizons import Panel, net_path, nonoverlap, week_interval, HORIZONS


def frame(n=1000):
    idx=pd.date_range('2026-01-01',periods=n,freq='h',tz='UTC')
    o=100*np.exp(np.arange(n)*.001)
    return pd.DataFrame({'open':o,'high':o*1.001,'low':o*.999,'close':o,'volume':1.},index=idx)


@pytest.mark.parametrize('side',[1,-1])
@pytest.mark.parametrize('hours',HORIZONS)
def test_vector_returns_match_independent_path_at_several_starts(side,hours):
    f=frame(); p=Panel(f); values=p.outcomes(hours,side)
    for k in (0,100,550):
        result=net_path(f,f.index[k],hours,side)
        assert values[k]==pytest.approx(result['net'])


def test_null_never_uses_postentry_prices_and_matches_all_horizons():
    f=frame(1500); p=Panel(f); pos,idx=p.positions(f.index[1000],1)
    assert pos==1000 and idx.min()==280 and idx.max()==832
    assert all(i+max(HORIZONS)<=pos for i in idx)
    altered=f.copy(); altered.loc[f.index[1001]:,'open']*=100
    p2=Panel(altered)
    for h in HORIZONS:
        assert np.array_equal(p.outcomes(h,1)[idx],p2.outcomes(h,1)[idx],equal_nan=True)


def test_known_short_liquidation_survives_later_gap_and_ignores_exit_high():
    f=frame(); f.loc[f.index[2],'high']=250.; f.loc[f.index[3],'open']=np.nan
    assert net_path(f,f.index[0],24,-1)['net']==-1.
    assert Panel(f).outcomes(24,-1)[0]==-1.
    g=frame(); g.loc[g.index[24],'high']=1e6
    assert net_path(g,g.index[0],24,-1)['liquidated'] is False


@pytest.mark.parametrize('hours', HORIZONS)
def test_research_short_squeeze_then_recovery_never_counts_as_profit(hours):
    f=frame(); f.loc[f.index[0],['open','high','low','close']]=100.
    f.loc[f.index[0],'high']=210.
    f.loc[f.index[hours],['open','high','low','close']]=70.
    assert net_path(f,f.index[0],hours,-1)['net']==-1.
    assert Panel(f).outcomes(hours,-1)[0]==-1.


def test_gap_before_liquidation_and_zero_prints_are_missing():
    f=frame(); f.loc[f.index[1],'open']=np.nan; f.loc[f.index[2],'high']=250.
    assert net_path(f,f.index[0],24,-1) is None
    assert np.isnan(Panel(f).outcomes(24,-1)[0])
    f=frame(); f.loc[f.index[2],'volume']=0
    assert net_path(f,f.index[0],24,1) is None


def test_same_cohort_filter_keeps_one_symbol_for_longest_horizon():
    at=pd.Timestamp('2026-01-01',tz='UTC')
    rows=[{'entry_at':at+pd.Timedelta(hours=i),'signal_id':str(i),'symbol':'A'} for i in (0,2,168)]
    assert len(nonoverlap(rows,168))==2
    assert len(nonoverlap(rows,1))==3
    assert week_interval([1.,2.],[at,at])[0] is None
