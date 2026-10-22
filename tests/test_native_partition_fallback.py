import pandas as pd
import pytest
from tests.test_verified_partition_series import bars,hourly,save,result
from scripts.repair_verified_partition_series import build_series_plans
from core.data.parquet_timezone_repair import RepairConflict


def test_native_candles_prove_partial_hour_without_guessing_missing_bars(tmp_path):
    source=bars('2026-05-01 00:50',hours=2,minutes=5).iloc[:2]
    local=source.copy();local.index+=pd.Timedelta(hours=8)
    directory=tmp_path/'5m_parts';save(directory,'2026-05-01',local)
    empty=source.iloc[:0]
    before=build_series_plans(directory,empty,{},'2026-05-01')
    assert before[0]['evidence']['series_summary']['unknown_rows']==2
    plans=build_series_plans(directory,empty,{},'2026-05-01',native_reference=source,native_provenance={'test':'official'})
    assert plans[0]['evidence']['series_summary']['unknown_rows']==0
    assert plans[0]['evidence']['series_summary']['shifted_rows']==2
    pd.testing.assert_frame_equal(result(plans),source.rename_axis('timestamp'),check_freq=False)
    assert len(plans[0]['evidence']['native_row_witnesses'])==2


def test_native_partial_coverage_cannot_certify_remaining_row(tmp_path):
    source=bars('2026-05-01 00:50',hours=2,minutes=5).iloc[:2]
    local=source.copy();local.index+=pd.Timedelta(hours=8)
    directory=tmp_path/'5m_parts';save(directory,'2026-05-01',local)
    plans=build_series_plans(directory,source.iloc[:0],{},'2026-05-01',
        native_reference=source.iloc[:1],native_provenance={'test':'official'})
    assert plans[0]['evidence']['series_summary']['unknown_rows']==1
    assert not plans[0]['utc_certified']


def test_native_fallback_needs_explicit_provenance(tmp_path):
    source=bars(hours=1,minutes=5)
    directory=tmp_path/'5m_parts';save(directory,'2026-05-01',source)
    with pytest.raises(RepairConflict,match='provenance'):
        build_series_plans(directory,hourly(source),{},native_reference=source)
