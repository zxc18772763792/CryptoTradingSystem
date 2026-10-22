import numpy as np
import pandas as pd
import pytest
from core.data.parquet_timezone_repair import apply_plan,rollback_run,RepairConflict,sha256
from scripts.repair_remaining_history_20261022 import build_venue_plan

def candles():
    idx=pd.date_range('2026-01-01',periods=32,freq='h',name='timestamp')
    x=np.arange(len(idx),dtype=float)+100
    return pd.DataFrame(dict(open=x,high=x+1,low=x-1,close=x+.5,volume=x*2),index=idx)

def test_exact_perpetual_witness_replaces_with_spot_at_same_clock(tmp_path):
    spot=candles();perp=spot+3;source=spot.iloc[:12].copy();source.iloc[5]=perp.iloc[5]
    path=tmp_path/'1h.parquet';source.to_parquet(path);before=path.read_bytes()
    plan=build_venue_plan(path,spot,perp,[source.index[5]],{'test':'two official markets'})
    assert plan['evidence']['venue_corrected_rows']==1
    assert plan['evidence']['shifted_rows']==0
    pd.testing.assert_frame_equal(plan['result'],spot.iloc[:12],check_freq=False)
    assert path.read_bytes()==before
    apply_plan(plan,tmp_path/'run')
    fresh=build_venue_plan(path,spot,perp,[source.index[5]],{'test':'two official markets'})
    assert fresh['status']=='already_utc' and not fresh['evidence']['operations']
    rollback_run(tmp_path/'run');assert path.read_bytes()==before

def test_perpetual_local_clock_collides_with_exact_spot_and_explicitly_coalesces(tmp_path):
    spot=candles();perp=spot+3
    source=spot.iloc[:4].copy();wrong=perp.iloc[[2]].copy();wrong.index+=pd.Timedelta(hours=8)
    source=pd.concat([source,wrong]);path=tmp_path/'1h.parquet';source.to_parquet(path)
    plan=build_venue_plan(path,spot,perp,wrong.index,{'test':'official'})
    assert len(plan['coalesced'])==1 and plan['rows_before']-plan['rows_after']==1
    assert plan['evidence']['shifted_rows']==1
    pd.testing.assert_frame_equal(plan['result'],spot.iloc[:4],check_freq=False)

def test_competing_market_clocks_are_not_resolved_by_favored_venue(tmp_path):
    spot=candles();perp=spot+3;perp.iloc[2]=spot.iloc[10]
    source=spot.iloc[[10]].copy();path=tmp_path/'1h.parquet';source.to_parquet(path)
    plan=build_venue_plan(path,spot,perp,source.index,{'test':'official'})
    assert plan['evidence']['unknown_positions']==[0]
    assert not plan['utc_certified']
    pd.testing.assert_frame_equal(plan['result'],source,check_freq=False)

def test_missing_spot_open_key_preserves_exact_perpetual_row(tmp_path):
    spot=candles();perp=spot+3;source=perp.iloc[[5]].copy()
    path=tmp_path/'1h.parquet';source.to_parquet(path)
    plan=build_venue_plan(path,spot.drop(source.index),perp,source.index,{'test':'official'})
    assert plan['evidence']['unknown_positions']==[0] and not plan['evidence']['operations']

def test_unproven_row_blocks_shift_without_losing_either_original(tmp_path):
    spot=candles();perp=spot+3
    bad=spot.iloc[[0]].copy();bad.iloc[0]=[1.,2.,.5,1.5,22.]
    wrong=perp.iloc[[0]].copy();wrong.index+=pd.Timedelta(hours=8)
    source=pd.concat([bad,wrong]);path=tmp_path/'1h.parquet';source.to_parquet(path)
    plan=build_venue_plan(path,spot,perp,source.index,{'test':'official'})
    assert len(plan['evidence']['unknown_positions'])==2
    assert not plan['evidence']['operations'] and plan['rows_after']==2

def test_plan_mutation_cannot_bypass_reference_proof(tmp_path):
    spot=candles();source=(spot+3).iloc[:2];path=tmp_path/'1h.parquet';source.to_parquet(path)
    plan=build_venue_plan(path,spot,spot+3,source.index,{'test':'official'})
    before=sha256(path);plan['result'].iloc[0,0]=0
    with pytest.raises(RepairConflict,match='changed'):
        apply_plan(plan,tmp_path/'run')
    assert sha256(path)==before


def test_unexpected_source_columns_cannot_be_silently_dropped(tmp_path):
    spot=candles();source=spot.iloc[:2].assign(extra_evidence=1)
    path=tmp_path/'1h.parquet';source.to_parquet(path);before=sha256(path)
    with pytest.raises(RepairConflict,match='Unexpected columns'):
        build_venue_plan(path,spot,spot+3,source.index,{'test':'official'})
    assert sha256(path)==before
