"""Parquet kline timezone integrity.

Guards the UTC-normalization invariants relied on by the strategy runtime
and backtest: parquet indexes must end up tz-naive UTC, and a legacy
local-stamped (UTC+8) partition must be healed against authoritative
incoming UTC bars without ever corrupting a genuinely-UTC partition.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from core.data.data_storage import (
    _heal_local_existing_against_utc,
    _normalize_parquet_frame_index,
)


def _ohlcv(index: pd.DatetimeIndex) -> pd.DataFrame:
    n = len(index)
    base = np.arange(n, dtype=float) + 100.0
    return pd.DataFrame(
        {
            "open": base,
            "high": base + 1.0,
            "low": base - 1.0,
            "close": base + 0.5,
            "volume": base * 10.0,
        },
        index=index,
    )


def test_normalize_tz_aware_utc_to_naive():
    idx = pd.date_range("2026-05-01 00:00", periods=10, freq="1h", tz="UTC")
    out = _normalize_parquet_frame_index(_ohlcv(idx))
    assert out.index.tz is None
    assert out.index[0] == pd.Timestamp("2026-05-01 00:00")


def test_normalize_future_local_index_shifted_back():
    # tz-naive index whose newest bar is "local now" (~real UTC + 8h), as
    # produced by the legacy maintain script.
    now = pd.Timestamp(datetime.now(timezone.utc)).tz_localize(None)
    idx = pd.date_range(end=now + pd.Timedelta(hours=8), periods=12, freq="1h")
    out = _normalize_parquet_frame_index(_ohlcv(idx))
    assert out.index.tz is None
    assert out.index.max() <= now + pd.Timedelta(minutes=2)
    # idempotent: a second pass must not shift again.
    again = _normalize_parquet_frame_index(out)
    assert again.index.max() == out.index.max()


def test_heal_local_existing_against_utc_shifts_when_ohlc_anchored():
    utc_idx = pd.date_range("2026-05-10 00:00", periods=24, freq="1h")
    incoming = _ohlcv(utc_idx)  # authoritative UTC
    existing = incoming.copy()
    existing.index = existing.index + pd.Timedelta(hours=8)  # legacy local label

    healed = _heal_local_existing_against_utc(existing, incoming)
    assert healed.index.min() == utc_idx.min()
    assert healed.index.max() == utc_idx.max()
    pd.testing.assert_frame_equal(
        healed.sort_index(), incoming.sort_index(), check_freq=False
    )


def test_heal_leaves_genuine_utc_partition_untouched():
    utc_idx = pd.date_range("2026-05-10 00:00", periods=24, freq="1h")
    incoming = _ohlcv(utc_idx)
    existing = incoming.copy()  # already UTC, matches incoming as-is

    healed = _heal_local_existing_against_utc(existing, incoming)
    pd.testing.assert_frame_equal(healed, existing)


def test_heal_no_overlap_is_noop():
    incoming = _ohlcv(pd.date_range("2026-05-10 00:00", periods=10, freq="1h"))
    existing = _ohlcv(pd.date_range("2026-01-01 00:00", periods=10, freq="1h"))
    healed = _heal_local_existing_against_utc(existing, incoming)
    pd.testing.assert_frame_equal(healed, existing)
