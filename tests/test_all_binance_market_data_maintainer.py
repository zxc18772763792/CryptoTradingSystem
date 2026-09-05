from __future__ import annotations

import importlib.util
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


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
