"""Month/year kline part files: layout helpers, writer, reader equivalence with the daily layout."""
import asyncio
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from core.data import kline_partitions as kp
from core.data.data_storage import DataStorage, _normalize_parquet_frame_index, prepare_utc_kline_frame
from core.data.kline_file_writer import write_utc_kline_file
from core.data.path_utils import canonical_symbol_dir
from core.exchanges.base_exchange import Kline

OHLCV = ["open", "high", "low", "close", "volume"]


def _frame(start, periods, freq, base=100.0, name="timestamp"):
    index = pd.date_range(start, periods=periods, freq=freq, name=name)
    values = base + np.arange(periods, dtype=float)
    return pd.DataFrame({"open": values, "high": values + 1, "low": values - 1, "close": values + 0.5,
                         "volume": values * 10}, index=index)


def _marked(frame):
    return prepare_utc_kline_frame(frame, timestamp_timezone="UTC")


def _klines(frame, timeframe):
    return [Kline(exchange="binance", symbol="ETH/USDT", timeframe=timeframe,
                  timestamp=ts.to_pydatetime().replace(tzinfo=timezone.utc), open=row.open, high=row.high,
                  low=row.low, close=row.close, volume=row.volume) for ts, row in frame.iterrows()]


def _storage(tmp_path):
    storage = DataStorage()
    storage.storage_path = tmp_path / "historical"
    return storage


def _parts(storage, timeframe):
    return canonical_symbol_dir(storage.storage_path, "binance", "ETH/USDT") / f"{timeframe}_parts"


def _old_daily_reader(symbol_dir: Path, timeframe: str, start=None, end=None) -> pd.DataFrame:
    """The reader as it was before month/year files (reference for equivalence)."""
    frames = []
    main = symbol_dir / f"{timeframe}.parquet"
    if main.exists():
        frame = pd.read_parquet(main)
        if start is not None or end is not None:
            frame = frame[(frame.index >= (start or frame.index.min())) & (frame.index <= (end or frame.index.max()))]
        if not frame.empty:
            frames.append(_normalize_parquet_frame_index(frame))
    files = sorted((symbol_dir / f"{timeframe}_parts").glob("*.parquet"))
    if start is not None or end is not None:
        lower = (pd.Timestamp(start).date() - timedelta(days=1)) if start else None
        upper = (pd.Timestamp(end).date() + timedelta(days=1)) if end else None
        files = [p for p in files if not ((lower and pd.Timestamp(p.stem).date() < lower)
                                          or (upper and pd.Timestamp(p.stem).date() > upper))]
    for path in files:
        frames.append(_normalize_parquet_frame_index(pd.read_parquet(path)))
    df = pd.concat(frames).sort_index()
    df = df[~df.index.duplicated(keep="last")]
    if start:
        df = df[df.index >= start]
    if end:
        df = df[df.index <= end]
    return df


# ---------------------------------------------------------------------------------------------- helpers


def test_granularity_and_stems():
    assert kp.partition_granularity("1s") == kp.DAY
    assert kp.partition_granularity("10s") == kp.DAY
    assert kp.partition_granularity("1m") == kp.MONTH
    assert kp.partition_granularity("15m") == kp.MONTH
    assert kp.partition_granularity("1h") == kp.YEAR
    assert kp.partition_granularity("1d") == kp.YEAR
    assert kp.partition_granularity("1M") == kp.YEAR
    assert kp.stem_period("2026-02") == (date(2026, 2, 1), date(2026, 2, 28), kp.MONTH)
    assert kp.stem_period("2026") == (date(2026, 1, 1), date(2026, 12, 31), kp.YEAR)
    assert kp.stem_period("2026-02-03") == (date(2026, 2, 3), date(2026, 2, 3), kp.DAY)
    assert kp.stem_period("2026-02.legacy") == (date(2026, 2, 1), date(2026, 2, 28), kp.MONTH)
    assert kp.stem_period("2026-02-03.legacy") is None
    assert kp.stem_period("2026-13") is None
    assert kp.stem_period("2026-01-01.corrupt_20260101_000000") is None
    assert kp.is_legacy_pack(Path("x/2026.legacy.parquet"))
    assert not kp.is_legacy_pack(Path("x/2026.parquet"))


def test_rows_match_partition_per_period():
    index = pd.DatetimeIndex(["2026-02-01 00:00", "2026-02-28 23:00"])
    assert kp.rows_match_partition(index, Path("1h_parts/2026-02.parquet"))
    assert kp.rows_match_partition(index, Path("1h_parts/2026.legacy.parquet"))
    assert not kp.rows_match_partition(index, Path("1h_parts/2026-02-01.parquet"))
    assert not kp.rows_match_partition(index, Path("1h_parts/2026-03.parquet"))
    assert not kp.rows_match_partition(index, Path("1h_parts/p0.parquet"))
    assert kp.rows_match_partition(pd.DatetimeIndex([]), Path("1h_parts/p0.parquet"))


def test_read_precedence_and_edges():
    names = ["2026-03-02.parquet", "2026.parquet", "2026-03.parquet", "2026.legacy.parquet", "2025.parquet",
             "2026-03.legacy.parquet", "zz.parquet"]
    ordered = [p.name for p in sorted((Path(n) for n in names), key=kp.part_sort_key)]
    assert ordered == ["2025.parquet", "2026.legacy.parquet", "2026.parquet", "2026-03.legacy.parquet",
                       "2026-03.parquet", "2026-03-02.parquet", "zz.parquet"]
    first, last = kp.edge_part_files([Path(n) for n in ["2025.legacy.parquet", "2026.parquet", "2026-10-25.parquet"]])
    assert [p.name for p in first] == ["2025.legacy.parquet"]
    assert [p.name for p in last] == ["2026.parquet", "2026-10-25.parquet"]


def test_read_order_rebuilds_daily_concatenation_including_foreign_rows():
    day1 = _frame("2026-05-18", 24, "h")
    day2 = pd.concat([_frame("2026-05-18 16:00", 8, "h", base=500.0), _frame("2026-05-19", 24, "h")])
    day3 = _frame("2026-05-20", 24, "h").set_axis(_frame("2026-05-20", 24, "h").index.as_unit("us"))
    daily = [Path("2026-05-18.parquet"), Path("2026-05-19.parquet"), Path("2026-05-20.parquet")]
    old = pd.concat([day1, day2, day3])
    # Month layout: day1 and day3 packed, day2 (foreign rows) kept daily.
    pack = pd.concat([day1, day3])
    frames, paths = [pack, day2], [Path("2026-05.legacy.parquet"), daily[1]]
    order = kp.read_order(frames, paths)
    rebuilt = pd.concat(frames).iloc[order]
    assert np.array_equal(rebuilt.index.as_unit("ns").asi8, old.index.as_unit("ns").asi8)
    assert np.array_equal(rebuilt.to_numpy(), old.to_numpy())


def test_read_order_keeps_newer_file_on_exact_tie():
    month = _frame("2026-05-01", 48, "h")
    recreated = _frame("2026-05-02", 2, "h", base=900.0)
    frames, paths = [month, recreated], [Path("2026-05.parquet"), Path("2026-05-02.parquet")]
    rebuilt = pd.concat(frames).iloc[kp.read_order(frames, paths)]
    assert not rebuilt.index.has_duplicates
    assert rebuilt.loc["2026-05-02 00:00", "open"] == 900.0
    assert len(rebuilt) == 48


# ----------------------------------------------------------------------------------------------- writer


def test_minute_bars_go_to_month_files_and_hours_to_year_files(tmp_path):
    storage = _storage(tmp_path)
    asyncio.run(storage.save_klines_to_parquet(_klines(_frame("2026-01-31 23:58", 4, "min"), "1m"),
                                               "binance", "ETH/USDT", "1m"))
    asyncio.run(storage.save_klines_to_parquet(_klines(_frame("2025-12-31 22:00", 4, "h"), "1h"),
                                               "binance", "ETH/USDT", "1h"))
    asyncio.run(storage.save_klines_to_parquet(_klines(_frame("2026-01-01 23:59:58", 4, "s"), "1s"),
                                               "binance", "ETH/USDT", "1s"))
    assert sorted(p.name for p in _parts(storage, "1m").glob("*.parquet")) == ["2026-01.parquet", "2026-02.parquet"]
    assert sorted(p.name for p in _parts(storage, "1h").glob("*.parquet")) == ["2025.parquet", "2026.parquet"]
    assert sorted(p.name for p in _parts(storage, "1s").glob("*.parquet")) == ["2026-01-01.parquet", "2026-01-02.parquet"]
    assert (_parts(storage, "1m") / "_series.lock").exists()
    assert not list(_parts(storage, "1m").glob("*.parquet.lock"))
    loaded = asyncio.run(storage.load_klines_from_parquet("binance", "ETH/USDT", "1m"))
    assert len(loaded) == 4 and loaded.attrs["cts_timestamp_status"] == "verified_utc"
    latest = asyncio.run(storage.get_latest_kline_timestamp("binance", "ETH/USDT", "1h"))
    assert latest == datetime(2026, 1, 1, 1, 0)


def test_writer_absorbs_verified_daily_file_it_touches(tmp_path):
    storage = _storage(tmp_path)
    parts = _parts(storage, "1h")
    parts.mkdir(parents=True)
    stored = _marked(_frame("2026-03-05", 24, "h"))
    stored.to_parquet(parts / "2026-03-05.parquet")
    (parts / "2026-03-05.parquet.lock").write_bytes(b"\0")
    untouched = _marked(_frame("2026-03-06", 24, "h", base=300.0))
    untouched.to_parquet(parts / "2026-03-06.parquet")
    incoming = _frame("2026-03-05 22:00", 2, "h", base=700.0)
    asyncio.run(storage.save_klines_to_parquet(_klines(incoming, "1h"), "binance", "ETH/USDT", "1h"))
    assert not (parts / "2026-03-05.parquet").exists()
    assert (parts / "2026-03-06.parquet").exists()
    year = pd.read_parquet(parts / "2026.parquet")
    assert len(year) == 24
    assert year.loc["2026-03-05 00:00", "open"] == 100.0
    assert year.loc["2026-03-05 22:00", "open"] == 700.0
    assert year.attrs["cts_timestamp_timezone"] == "UTC"
    if __import__("os").name == "nt":
        assert not (parts / "2026-03-05.parquet.lock").exists()


def test_writer_keeps_unverified_daily_file_behind_the_guard(tmp_path):
    storage = _storage(tmp_path)
    parts = _parts(storage, "1h")
    parts.mkdir(parents=True)
    legacy = _frame("2026-03-05", 24, "h")
    legacy.to_parquet(parts / "2026-03-05.parquet")
    before = (parts / "2026-03-05.parquet").read_bytes()
    with pytest.raises(ValueError, match="Unverified parquet timezone"):
        asyncio.run(storage.save_klines_to_parquet(_klines(legacy.iloc[-2:], "1h"), "binance", "ETH/USDT", "1h"))
    assert (parts / "2026-03-05.parquet").read_bytes() == before
    assert not (parts / "2026.parquet").exists()
    # Full, matching coverage certifies the day where it is, as before.
    asyncio.run(storage.save_klines_to_parquet(_klines(legacy, "1h"), "binance", "ETH/USDT", "1h"))
    assert pd.read_parquet(parts / "2026-03-05.parquet").attrs["cts_timestamp_timezone"] == "UTC"


def test_writer_guards_each_packed_day_and_moves_it_once_verified(tmp_path):
    storage = _storage(tmp_path)
    parts = _parts(storage, "1h")
    parts.mkdir(parents=True)
    day_a, day_b = _frame("2026-03-05", 24, "h"), _frame("2026-03-06", 24, "h", base=300.0)
    pack = pd.concat([day_a, day_b])
    pack.to_parquet(parts / "2026.legacy.parquet")
    before = (parts / "2026.legacy.parquet").read_bytes()
    with pytest.raises(ValueError, match="Unverified parquet timezone"):
        asyncio.run(storage.save_klines_to_parquet(_klines(day_a.iloc[-3:], "1h"), "binance", "ETH/USDT", "1h"))
    assert (parts / "2026.legacy.parquet").read_bytes() == before
    assert not (parts / "2026.parquet").exists()
    asyncio.run(storage.save_klines_to_parquet(_klines(day_a, "1h"), "binance", "ETH/USDT", "1h"))
    remaining = pd.read_parquet(parts / "2026.legacy.parquet")
    pd.testing.assert_frame_equal(remaining, day_b, check_freq=False)
    assert not remaining.attrs
    year = pd.read_parquet(parts / "2026.parquet")
    pd.testing.assert_frame_equal(year[OHLCV], day_a, check_freq=False)
    loaded = asyncio.run(storage.load_klines_from_parquet("binance", "ETH/USDT", "1h"))
    assert len(loaded) == 48 and loaded.attrs["cts_timestamp_status"] == "unverified_naive"
    asyncio.run(storage.save_klines_to_parquet(_klines(day_b, "1h"), "binance", "ETH/USDT", "1h"))
    assert not (parts / "2026.legacy.parquet").exists()
    loaded = asyncio.run(storage.load_klines_from_parquet("binance", "ETH/USDT", "1h"))
    assert len(loaded) == 48 and loaded.attrs["cts_timestamp_status"] == "verified_utc"


def test_file_writer_checks_month_files_and_takes_series_lock(tmp_path):
    target = tmp_path / "1m_parts" / "2026-01.parquet"
    write_utc_kline_file(target, _frame("2026-01-31 23:58", 2, "min"), origin="test", timestamp_timezone="UTC")
    assert len(pd.read_parquet(target)) == 2
    assert (tmp_path / "1m_parts" / "_series.lock").exists()
    with pytest.raises(ValueError, match="partition date"):
        write_utc_kline_file(target, _frame("2026-02-01", 2, "min"), origin="test", timestamp_timezone="UTC")


# ----------------------------------------------------------------------------------------------- reader


def _build_daily_layout(symbol_dir: Path) -> None:
    """Main file plus daily parts: marked and unmarked days, a us index, a day holding a neighbour's rows,
    and timestamps where main and parts disagree."""
    parts = symbol_dir / "1h_parts"
    parts.mkdir(parents=True)
    main = _frame("2026-02-27", 24 * 6, "h", base=1000.0)
    main.to_parquet(symbol_dir / "1h.parquet")
    for n, day in enumerate(pd.date_range("2026-02-27", "2026-03-06", freq="D")):
        frame = _frame(day, 24, "h", base=100.0 + n)
        if n % 2:
            frame = _marked(frame)
        if n == 3:
            frame = frame.set_axis(frame.index.as_unit("us"))
        if n == 5:  # holds 8h of the previous day with other values
            frame = pd.concat([_frame(day - pd.Timedelta(hours=8), 8, "h", base=5000.0), frame])
        frame.to_parquet(parts / f"{day.date()}.parquet")


def _consolidated_copy(src: Path, dst: Path) -> None:
    import shutil
    from core.data.kline_part_consolidation import colliding_daily_files, consolidate_period, daily_files_by_period
    shutil.copytree(src, dst)
    parts = dst / "1h_parts"
    groups = daily_files_by_period(parts, "1h", date(2026, 10, 1))
    collisions = colliding_daily_files([p for g in groups.values() for p in g])
    for stem, paths in groups.items():
        record = consolidate_period(parts, stem, paths, apply=True, legacy_before=date(2026, 10, 1),
                                    collisions=collisions)
        assert record["status"] == "consolidated", record


@pytest.mark.parametrize("window", [(None, None), ("2026-03-01 05:00", "2026-03-02 03:00"),
                                    ("2026-03-03 10:00", None), (None, "2026-02-28 12:00")])
def test_reader_on_consolidated_layout_matches_old_daily_reader(tmp_path, window):
    daily_root = tmp_path / "daily" / "binance" / "ETH_USDT"
    _build_daily_layout(daily_root)
    packed_root = tmp_path / "packed" / "binance" / "ETH_USDT"
    _consolidated_copy(daily_root, packed_root)
    names = sorted(p.name for p in (packed_root / "1h_parts").glob("*.parquet"))
    assert names == ["2026-03-03.parquet", "2026-03-04.parquet", "2026.legacy.parquet", "2026.parquet"]
    start, end = (pd.Timestamp(w).to_pydatetime() if w else None for w in window)
    expected = _old_daily_reader(daily_root, "1h", start, end)
    storage = DataStorage()
    for root in (tmp_path / "daily", tmp_path / "packed"):
        storage.storage_path = root
        loaded = asyncio.run(storage.load_klines_from_parquet("binance", "ETH/USDT", "1h", start, end))
        pd.testing.assert_frame_equal(loaded, expected, check_exact=True, check_freq=False)
        assert loaded.attrs["cts_timestamp_status"] == "unverified_naive"


def test_latest_timestamp_sees_daily_file_after_month_file(tmp_path):
    storage = _storage(tmp_path)
    parts = _parts(storage, "1m")
    parts.mkdir(parents=True)
    _marked(_frame("2026-10-01", 60, "min")).to_parquet(parts / "2026-10.parquet")
    _marked(_frame("2026-10-26 08:00", 5, "min")).to_parquet(parts / "2026-10-26.parquet")
    _marked(_frame("2026-09-30 23:00", 5, "min")).to_parquet(parts / "2026-09.parquet")
    latest = asyncio.run(storage.get_latest_kline_timestamp("binance", "ETH/USDT", "1m"))
    assert latest == datetime(2026, 10, 26, 8, 4)


# ----------------------------------------------------------------------------------- other part readers


def _load_script(name):
    import importlib.util
    import sys
    path = Path(__file__).resolve().parents[1] / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"{name}_kline_partitions_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_maintainer_bounds_find_a_recreated_day_after_the_year_file(tmp_path):
    maint = _load_script("maintain_all_binance_market_data")
    parts = tmp_path / "binance" / "ETH_USDT" / "1h_parts"
    parts.mkdir(parents=True)
    _frame("2025-03-01", 48, "h").to_parquet(parts / "2025.legacy.parquet")
    _marked(_frame("2026-01-01", 24 * 30, "h")).to_parquet(parts / "2026.parquet")
    _marked(_frame("2026-10-26", 3, "h")).to_parquet(parts / "2026-10-26.parquet")
    bounds = maint.local_bounds(tmp_path, maint.SeriesKey("binance", "spot", "ETH/USDT", "1h"))
    assert bounds.first == datetime(2025, 3, 1)
    assert bounds.last == datetime(2026, 10, 26, 2)
    assert bounds.files == 3


def test_certify_partition_targets_consider_day_files_only(tmp_path):
    cert = _load_script("certify_binance_store_from_archive")
    parts = tmp_path / "binance" / "ETH_USDT" / "1m_parts"
    parts.mkdir(parents=True)
    for name in ("2026-10.parquet", "2026-09.legacy.parquet", "2026-10-24.parquet", "2026-10-25.parquet"):
        (parts / name).write_bytes(b"")
    found = cert.partition_targets(tmp_path, datetime(2026, 10, 26), "2026-10-10", None)
    assert sorted(part.name for _, _, part in found) == ["2026-10-24.parquet", "2026-10-25.parquet"]


def test_trade_attribution_reads_the_requested_days_from_month_files(tmp_path):
    from scripts import agent_trade_attribution as attr
    parts = tmp_path / "ETH_USDT" / "5m_parts"
    parts.mkdir(parents=True)
    _frame("2026-09-01", 288 * 30, "5min").to_parquet(parts / "2026-09.legacy.parquet")
    _frame("2026-10-02", 288, "5min", base=900.0).to_parquet(parts / "2026-10-02.parquet")
    attr._BARS.pop("ETH/USDT", None)
    bars = attr.load_bars("ETH/USDT", "2026-09-29", "2026-10-02", tmp_path)
    attr._BARS.pop("ETH/USDT", None)
    days = sorted(set(bars.index.strftime("%Y-%m-%d")))
    assert days == ["2026-09-29", "2026-09-30", "2026-10-02"]
    assert len(bars) == 288 * 3


def test_web_latest_partition_end_uses_day_names_and_month_file_contents(tmp_path, monkeypatch):
    import web.api.data as data_api
    monkeypatch.setattr(data_api.settings, "DATA_STORAGE_PATH", tmp_path)
    parts = tmp_path / "binance" / "ETH_USDT" / "1h_parts"
    parts.mkdir(parents=True)
    _marked(_frame("2026-10-20", 24, "h")).to_parquet(parts / "2026-10-20.parquet")
    assert data_api._latest_partition_end_time("binance", "ETH/USDT", "1h") == datetime(2026, 10, 21)
    _marked(_frame("2026-01-01", 24 * 297 + 13, "h")).to_parquet(parts / "2026.parquet")  # last bar 10-25 12:00
    assert data_api._latest_partition_end_time("binance", "ETH/USDT", "1h") == datetime(2026, 10, 26)
