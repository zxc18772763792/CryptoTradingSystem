from __future__ import annotations
import io
import zipfile
from datetime import timezone
import pandas as pd
import pytest
from core.data.binance_archive import _parse_zip_payload
from core.data.kline_file_writer import write_utc_kline_file
from core.data.parquet_lock import parquet_partition_lock, ParquetPartitionLockTimeout


def frame():
    return pd.DataFrame({"open": [1., 2., 3.], "high": [2., 3., 4.], "low": [.5, 1., 2.],
        "close": [1.5, 2.5, 3.5], "volume": [10., 20., 30.]},
        index=pd.date_range("2026-01-01", periods=3, freq="s", tz="UTC", name="timestamp"))


def payload(unit="us", duplicate=False, invalid=False):
    base = 1767225600000000 if unit == "us" else 1767225600000
    step = 1000000 if unit == "us" else 1000
    rows = []
    for i in range(3):
        ts = base + i * step
        if duplicate and i == 2:
            ts = base
        if invalid and i == 1:
            ts = "broken"
        rows.append(f"{ts},{i+1},{i+2},0.5,{i+1.5},10,0,0,1,0,0,0")
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        archive.writestr("bars.csv", "\n".join(rows))
    return out.getvalue()


@pytest.mark.parametrize("unit", ["ms", "us"])
def test_official_archive_units_keep_exact_utc_open_times(unit):
    parsed = _parse_zip_payload(payload(unit))
    assert parsed.index[0] == pd.Timestamp("2026-01-01")
    assert parsed.index[1] == pd.Timestamp("2026-01-01 00:00:01")
    assert parsed.attrs["cts_timestamp_timezone"] == "UTC"


@pytest.mark.parametrize("options", [{"duplicate": True}, {"invalid": True}])
def test_archive_does_not_discard_bad_or_duplicate_timestamps(options):
    with pytest.raises(ValueError):
        _parse_zip_payload(payload(**options))


def test_archive_writer_locks_marks_merges_and_preserves_unknown_history(tmp_path, monkeypatch):
    target = tmp_path / "1s_parts/2026-01-01.parquet"
    assert write_utc_kline_file(target, frame().iloc[:2], origin="writer:test") == 2
    assert write_utc_kline_file(target, frame().iloc[2:], origin="writer:test") == 1
    assert len(pd.read_parquet(target)) == 3
    assert pd.read_parquet(target).attrs["cts_timestamp_schema"] == 1
    old = frame().copy(); old.index = old.index.tz_localize(None); old.attrs = {}
    old.to_parquet(target)
    before = target.read_bytes()
    with pytest.raises(ValueError, match="timezone"):
        write_utc_kline_file(target, frame().tail(1), origin="writer:test")
    assert target.read_bytes() == before
    import core.data.kline_file_writer as writer
    monkeypatch.setattr(writer, "parquet_partition_lock", lambda p: parquet_partition_lock(p, timeout_seconds=.05))
    with parquet_partition_lock(target):
        with pytest.raises(ParquetPartitionLockTimeout):
            write_utc_kline_file(target, frame(), origin="writer:test")


def test_wrong_day_is_rejected_before_creation(tmp_path):
    target = tmp_path / "1s_parts/2026-01-02.parquet"
    with pytest.raises(ValueError, match="partition date"):
        write_utc_kline_file(target, frame(), origin="writer:test")
    assert not target.exists()


def test_marked_but_mispartitioned_old_rows_are_not_propagated(tmp_path):
    from core.data.data_storage import prepare_utc_kline_frame
    target = tmp_path / "1s_parts/2026-01-01.parquet"
    target.parent.mkdir()
    old = prepare_utc_kline_frame(frame())
    old.index = old.index - pd.Timedelta(hours=8)
    old.to_parquet(target)
    before = target.read_bytes()
    with pytest.raises(ValueError, match="partition date"):
        write_utc_kline_file(target, frame(), origin="writer:test")
    assert target.read_bytes() == before


def test_second_backfill_preserves_unknown_and_marks_new_parts(tmp_path, monkeypatch):
    from core.data.second_level_backfill import SecondLevelBackfillManager
    manager = object.__new__(SecondLevelBackfillManager)
    monkeypatch.setattr(manager, "_parts_dir", lambda *args: tmp_path / "1s_parts")
    assert manager._save_parts("binance", "BTC/USDT", frame()) == 3
    target = tmp_path / "1s_parts/2026-01-01.parquet"
    assert pd.read_parquet(target).attrs["cts_timestamp_timezone"] == "UTC"
    old = frame(); old.index = old.index.tz_localize(None); old.attrs = {}; old.to_parquet(target)
    before = target.read_bytes()
    with pytest.raises(ValueError, match="timezone"):
        manager._save_parts("binance", "BTC/USDT", frame().tail(1))
    assert target.read_bytes() == before
    assert manager._save_parts("binance", "BTC/USDT", frame()) == 0
    assert pd.read_parquet(target).attrs["cts_timestamp_timezone"] == "UTC"
