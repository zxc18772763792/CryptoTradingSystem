from __future__ import annotations

from pathlib import Path
import pandas as pd
import pytest

from scripts import download_data as gate
from core.data.parquet_lock import parquet_partition_lock, ParquetPartitionLockTimeout


def rows():
    return [[1767225600+i*3600,'10',str(100.5+i),str(101+i),str(99+i),str(100+i)]for i in range(5)]


def test_gate_unix_seconds_are_utc_not_machine_local_wall_clock():
    frame=gate.gate_rows_to_frame(rows())
    assert str(frame.index.tz)=='UTC'
    assert frame.index[0]==pd.Timestamp('2026-01-01T00:00:00Z')
    assert frame.iloc[0]['close']==100.5


def test_gate_download_writes_utc_marker_and_merges_without_discarding_old_rows(tmp_path):
    target=tmp_path/'gate/BTC_USDT/1h.parquet'
    frame=gate.gate_rows_to_frame(rows())
    gate.write_download_frame(target,frame.iloc[:3])
    gate.write_download_frame(target,frame.iloc[3:])
    loaded=pd.read_parquet(target)
    assert len(loaded)==5
    assert loaded.index.tz is None
    assert loaded.index[0]==pd.Timestamp('2026-01-01 00:00')
    assert loaded.attrs['cts_timestamp_timezone']=='UTC'
    assert loaded.attrs['cts_timestamp_schema']==1
    assert loaded.attrs['cts_timestamp_origin']=='writer:gate_download'


def test_gate_download_preserves_unverified_old_source_when_overlap_is_partial(tmp_path):
    target=tmp_path/'1h.parquet'
    frame=gate.gate_rows_to_frame(rows())
    old=frame.copy();old.index=old.index.tz_localize(None)
    old.to_parquet(target)
    before=target.read_bytes()
    with pytest.raises(ValueError,match='timezone audit'):
        gate.write_download_frame(target,frame.tail(1))
    assert target.read_bytes()==before
    assert not list(tmp_path.glob('*.tmp'))


def test_gate_empty_download_leaves_unverified_history_and_metadata_untouched(tmp_path):
    target=tmp_path/'1h.parquet'
    old=gate.gate_rows_to_frame(rows())
    old.index=old.index.tz_localize(None)
    old.to_parquet(target)
    before=target.read_bytes()
    gate.write_download_frame(target,pd.DataFrame())
    assert target.read_bytes()==before
    assert not pd.read_parquet(target).attrs


def test_gate_download_lock_matches_the_common_parquet_lock(tmp_path,monkeypatch):
    target=tmp_path/'1h.parquet'
    monkeypatch.setattr(gate,'parquet_partition_lock',lambda path:parquet_partition_lock(path,timeout_seconds=.05,poll_seconds=.01))
    with parquet_partition_lock(target):
        with pytest.raises(ParquetPartitionLockTimeout):
            gate.write_download_frame(target,gate.gate_rows_to_frame(rows()))
    assert not target.exists()
