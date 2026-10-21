from __future__ import annotations

import importlib.util
import asyncio
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pandas as pd

from core.data.parquet_lock import parquet_partition_lock, ParquetPartitionLockTimeout


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "maintain_all_binance_market_data.py"
SPEC = importlib.util.spec_from_file_location("maintain_all_binance_market_data_test", SCRIPT)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def test_discover_local_series_keeps_market_namespace_and_timeframes(tmp_path: Path) -> None:
    root = tmp_path / "binance" / "BTC_USDT"
    (root / "4h_parts").mkdir(parents=True)
    (root / "1h.parquet").touch()
    (root / "not_a_timeframe_parts").mkdir()

    values = MODULE.discover_local_series(tmp_path, "binance", "spot")

    assert MODULE.SeriesKey("binance", "spot", "BTC/USDT", "4h") in values
    assert MODULE.SeriesKey("binance", "spot", "BTC/USDT", "1h") in values
    assert all(item.namespace == "binance" and item.market_type == "spot" for item in values)


def test_plan_fetch_windows_fills_missing_head_and_stale_tail() -> None:
    bounds = MODULE.LocalBounds(
        first=datetime(2026, 1, 10),
        last=datetime(2026, 1, 20),
        files=10,
    )

    windows = MODULE.plan_fetch_windows(
        bounds,
        datetime(2026, 1, 1),
        datetime(2026, 1, 25),
        "4h",
        overlap_bars=2,
    )

    assert [item.reason for item in windows] == ["missing_head", "stale_tail"]
    assert windows[0].start_ms < windows[0].end_open_ms
    assert windows[1].start_ms < windows[1].end_open_ms


def test_build_catalog_adds_all_active_futures_but_keeps_spot_core(tmp_path: Path) -> None:
    spot = {"BTCUSDT": "TRADING", "MKRUSDT": "BREAK"}
    futures = {"BTCUSDT": "TRADING", "ETHUSDT": "SETTLING", "SOLUSDT": "TRADING"}

    values = MODULE.build_series_catalog(tmp_path, spot, futures)

    assert MODULE.SeriesKey("binance_futures", "futures", "BTC/USDT", "4h") in values
    assert MODULE.SeriesKey("binance_futures", "futures", "SOL/USDT", "4h") in values
    assert MODULE.SeriesKey("binance_futures", "futures", "ETH/USDT", "4h") not in values
    assert MODULE.SeriesKey("binance", "spot", "MKR/USDT", "1d") in values


def test_sqlite_catalog_upsert_is_idempotent(tmp_path: Path) -> None:
    database = tmp_path / "catalog.db"
    row = MODULE.SeriesResult(
        namespace="binance_futures",
        market_type="futures",
        symbol="BTC/USDT",
        timeframe="4h",
        source_status="TRADING",
        target_start_utc="2026-01-01T00:00:00+00:00",
        target_end_utc="2026-01-02T00:00:00+00:00",
        before_first_utc=None,
        before_last_utc=None,
        after_first_utc="2026-01-01T00:00:00+00:00",
        after_last_utc="2026-01-02T00:00:00+00:00",
        file_count=2,
        downloaded_rows=7,
        complete_head=True,
        fresh_tail=True,
        status="complete",
    )

    MODULE.update_sqlite_catalog(database, [row], "2026-01-02T01:00:00+00:00")
    row.downloaded_rows = 0
    MODULE.update_sqlite_catalog(database, [row], "2026-01-02T02:00:00+00:00")

    connection = sqlite3.connect(database)
    try:
        stored = connection.execute(
            "SELECT COUNT(*), downloaded_rows, refresh_status FROM market_data_catalog"
        ).fetchone()
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(market_data_catalog)")
        }
    finally:
        connection.close()
    assert stored == (1, 0, "complete")
    assert {"requested_start_utc", "source_available_from_utc"} <= columns


def test_validate_rows_rejects_inconsistent_ohlc() -> None:
    bad = [[0, "10", "9", "8", "9", "1"]]

    try:
        MODULE.validate_rows(bad, "1h")
    except ValueError as exc:
        assert "inconsistent OHLC" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("invalid bar was accepted")


def test_non_one_second_timeframes_are_derived() -> None:
    assert MODULE.is_derived_seconds_timeframe("10s") is True
    assert MODULE.is_derived_seconds_timeframe("1s") is False
    assert MODULE.is_derived_seconds_timeframe("1m") is False


def test_weekly_alignment_uses_monday_utc() -> None:
    monday_ms = int(datetime(2026, 8, 10, tzinfo=timezone.utc).timestamp() * 1000)
    assert (monday_ms - MODULE.timeframe_alignment_offset_ms("1w")) % MODULE.timeframe_ms("1w") == 0


def test_compact_writer_keeps_seconds_partitioned() -> None:
    assert MODULE.SeriesKey("binance", "spot", "BTC/USDT", "1s").timeframe.endswith("s")
    assert not MODULE.SeriesKey("binance_futures", "futures", "BTC/USDT", "4h").timeframe.endswith("s")


def test_source_available_start_accepts_post_listing_history(monkeypatch: pytest.MonkeyPatch) -> None:
    key = MODULE.SeriesKey("binance", "spot", "NEW/USDT", "4h")
    requested = datetime(2025, 8, 10, 4, 0)
    first = datetime(2025, 10, 1, 8, 0)
    monkeypatch.setattr(
        MODULE,
        "request_json",
        lambda *_args, **_kwargs: [[int(first.replace(tzinfo=timezone.utc).timestamp() * 1000)]],
    )

    effective, available = MODULE.source_available_start(
        key,
        requested,
        MODULE.LocalBounds(first=first, last=datetime(2026, 8, 10, 4, 0), files=1),
        MODULE.RequestRateLimiter(1000),
    )

    assert effective == first
    assert available == first


def test_source_available_start_keeps_requested_when_exchange_history_is_older(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    key = MODULE.SeriesKey("binance_futures", "futures", "OLD/USDT", "4h")
    requested = datetime(2026, 6, 21, 8, 0)
    exchange_first = datetime(2024, 1, 1, 0, 0)
    monkeypatch.setattr(
        MODULE,
        "request_json",
        lambda *_args, **_kwargs: [
            [int(exchange_first.replace(tzinfo=timezone.utc).timestamp() * 1000)]
        ],
    )

    effective, available = MODULE.source_available_start(
        key,
        requested,
        MODULE.LocalBounds(first=datetime(2026, 7, 1, 0, 0), last=None, files=1),
        MODULE.RequestRateLimiter(1000),
    )

    assert effective == requested
    assert available == exchange_first


def _compact_test_bars():
    key=MODULE.SeriesKey('binance_futures','futures','BTC/USDT','1h')
    start=int(datetime(2026,1,1,tzinfo=timezone.utc).timestamp()*1000)
    rows=[[start+i*3600000,str(100+i),str(101+i),str(99+i),str(100.5+i),'10']for i in range(5)]
    return key,MODULE.rows_to_klines(key,rows)


def test_compact_writer_records_utc_metadata_and_keeps_historical_rows(tmp_path,monkeypatch):
    monkeypatch.setattr(MODULE.data_storage,'storage_path',tmp_path)
    key,bars=_compact_test_bars()
    target=Path(asyncio.run(MODULE.save_klines_compact(key,bars[:3])))
    first=pd.read_parquet(target)
    assert first.attrs['cts_timestamp_timezone']=='UTC'
    assert first.attrs['cts_timestamp_schema']==1
    assert first.attrs['cts_timestamp_origin']=='writer:binance_compact'
    asyncio.run(MODULE.save_klines_compact(key,bars[3:]))
    combined=pd.read_parquet(target)
    assert len(combined)==5
    assert combined.index.min()==pd.Timestamp('2026-01-01 00:00')
    assert combined.index.max()==pd.Timestamp('2026-01-01 04:00')


def test_compact_writer_refuses_partial_overlap_with_unmarked_history(tmp_path,monkeypatch):
    monkeypatch.setattr(MODULE.data_storage,'storage_path',tmp_path)
    key,bars=_compact_test_bars()
    target=tmp_path/'binance_futures/BTC_USDT/1h.parquet'
    target.parent.mkdir(parents=True)
    frame=pd.DataFrame([{'open':bar.open,'high':bar.high,'low':bar.low,'close':bar.close,'volume':bar.volume}for bar in bars],
                       index=pd.DatetimeIndex([bar.timestamp.replace(tzinfo=None)for bar in bars],name='timestamp'))
    frame.to_parquet(target)
    before=target.read_bytes()
    with pytest.raises(ValueError,match='timezone audit'):
        asyncio.run(MODULE.save_klines_compact(key,bars[-1:]))
    assert target.read_bytes()==before
    assert not list(target.parent.glob('*.tmp'))


def test_compact_writer_rejects_naive_or_duplicate_incoming_timestamps(tmp_path,monkeypatch):
    monkeypatch.setattr(MODULE.data_storage,'storage_path',tmp_path)
    key,bars=_compact_test_bars()
    with pytest.raises(ValueError,match='duplicate timestamps'):
        asyncio.run(MODULE.save_klines_compact(key,[bars[0],bars[0]]))
    bars[0].timestamp=bars[0].timestamp.replace(tzinfo=None)
    with pytest.raises(ValueError,match='timezone-aware'):
        asyncio.run(MODULE.save_klines_compact(key,[bars[0]]))


def test_compact_writer_uses_the_same_persistent_lock_as_repairs(tmp_path,monkeypatch):
    monkeypatch.setattr(MODULE.data_storage,'storage_path',tmp_path)
    key,bars=_compact_test_bars()
    target=tmp_path/'binance_futures/BTC_USDT/1h.parquet'
    monkeypatch.setattr(MODULE,'parquet_partition_lock',lambda path,**kwargs:parquet_partition_lock(path,timeout_seconds=.05,poll_seconds=.01))
    with parquet_partition_lock(target):
        with pytest.raises(ParquetPartitionLockTimeout):
            asyncio.run(MODULE.save_klines_compact(key,bars))
    assert not target.exists()
