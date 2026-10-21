"""Parquet kline timezone integrity.

Reads never guess timezone shifts. Future writers require explicit UTC
provenance and reject unverified legacy merges rather than silently migrating.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from core.data.data_storage import (
    _heal_local_existing_against_utc,
    _normalize_parquet_frame_index,
    _parquet_max_timestamp,
    prepare_utc_kline_frame,
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


def test_normalize_future_naive_index_does_not_guess_a_timezone(monkeypatch):
    from config.settings import settings
    monkeypatch.setattr(settings, "PARQUET_TZ_STRICT", False)
    now = pd.Timestamp(datetime.now(timezone.utc)).tz_localize(None)
    idx = pd.date_range(end=now + pd.Timedelta(hours=8), periods=12, freq="1h")
    out = _normalize_parquet_frame_index(_ohlcv(idx))
    assert out.index.tz is None
    pd.testing.assert_index_equal(out.index, idx)
    assert out.attrs["cts_timestamp_status"] == "unverified_naive"
    again = _normalize_parquet_frame_index(out)
    assert again.index.max() == out.index.max()


def test_partial_shifted_anchors_require_explicit_audit_instead_of_auto_healing():
    utc_idx = pd.date_range("2026-05-10 00:00", periods=24, freq="1h")
    incoming = prepare_utc_kline_frame(_ohlcv(utc_idx), timestamp_timezone="UTC")
    existing = incoming.copy()
    existing.index = existing.index + pd.Timedelta(hours=8)  # legacy local label

    existing.attrs = {}  # unmarked legacy file
    before = existing.copy()
    with pytest.raises(ValueError, match="explicit timezone audit"):
        _heal_local_existing_against_utc(existing, incoming)
    pd.testing.assert_frame_equal(existing, before)


def test_heal_leaves_genuine_utc_partition_untouched():
    utc_idx = pd.date_range("2026-05-10 00:00", periods=24, freq="1h")
    incoming = prepare_utc_kline_frame(_ohlcv(utc_idx), timestamp_timezone="UTC")
    existing = incoming.copy()  # already UTC, matches incoming as-is

    healed = _heal_local_existing_against_utc(existing, incoming)
    pd.testing.assert_frame_equal(healed, existing)


def test_unknown_no_overlap_cannot_be_certified_by_appending_utc_data():
    incoming = prepare_utc_kline_frame(_ohlcv(pd.date_range("2026-05-10 00:00", periods=10, freq="1h")), timestamp_timezone="UTC")
    existing = _ohlcv(pd.date_range("2026-01-01 00:00", periods=10, freq="1h"))
    with pytest.raises(ValueError, match="Unverified parquet timezone"):
        _heal_local_existing_against_utc(existing, incoming)


def test_explicit_utc_marker_never_triggers_a_second_shift(monkeypatch):
    from config.settings import settings
    monkeypatch.setattr(settings, "PARQUET_TZ_STRICT", False)
    now = pd.Timestamp(datetime.now(timezone.utc)).tz_localize(None)
    frame = _ohlcv(pd.date_range(now+pd.Timedelta(hours=8), periods=3, freq="1h"))
    frame.attrs = {"cts_timestamp_timezone":"UTC", "cts_timestamp_schema":1, "cts_timestamp_origin":"verified_timezone_repair"}
    result = _normalize_parquet_frame_index(frame)
    pd.testing.assert_index_equal(result.index, frame.index)
    assert result.attrs["cts_timestamp_origin"] == "verified_timezone_repair"
    incoming = prepare_utc_kline_frame(_ohlcv(pd.date_range("2026-05-01",periods=3,freq="1h")),timestamp_timezone="UTC")
    checked = _heal_local_existing_against_utc(result,incoming)
    pd.testing.assert_index_equal(checked.index,frame.index)


def test_latest_timestamp_metadata_does_not_guess_minus_eight_hours(tmp_path):
    now = pd.Timestamp(datetime.now(timezone.utc)).tz_localize(None)
    frame = _ohlcv(pd.date_range(now+pd.Timedelta(hours=8),periods=3,freq="1h"))
    frame.attrs = {"cts_timestamp_timezone":"UTC", "cts_timestamp_schema":1, "cts_timestamp_origin":"verified_timezone_repair"}
    path = tmp_path/"1h.parquet"
    frame.to_parquet(path)
    assert _parquet_max_timestamp(path) == frame.index.max().to_pydatetime()


def test_old_unmarked_naive_history_stays_readable_and_unverified():
    frame = _ohlcv(pd.date_range("2020-01-01 08:00",periods=3,freq="1h"))
    result = _normalize_parquet_frame_index(frame)
    pd.testing.assert_index_equal(result.index,frame.index)
    assert result.attrs["cts_timestamp_status"] == "unverified_naive"
    assert "cts_timestamp_timezone" not in result.attrs


def test_aware_local_writer_converts_to_utc_and_persists_metadata(tmp_path):
    local = _ohlcv(pd.date_range("2026-01-01 08:00",periods=3,freq="1h",tz="Asia/Shanghai"))
    result = prepare_utc_kline_frame(local)
    assert result.index[0] == pd.Timestamp("2026-01-01 00:00")
    assert result.index.tz is None
    path=tmp_path/"1h.parquet"
    result.to_parquet(path)
    loaded=pd.read_parquet(path)
    assert loaded.attrs["cts_timestamp_timezone"] == "UTC"
    assert loaded.attrs["cts_timestamp_schema"] == 1
    assert loaded.attrs["cts_timestamp_origin"] == "writer"


def test_naive_writer_requires_an_explicit_utc_declaration():
    frame = _ohlcv(pd.date_range("2026-01-01",periods=3,freq="1h"))
    with pytest.raises(ValueError,match="timezone-aware"):
        prepare_utc_kline_frame(frame)
    result = prepare_utc_kline_frame(frame,timestamp_timezone="UTC")
    assert result.attrs["cts_timestamp_timezone"] == "UTC"


def test_empty_writer_frame_has_a_datetime_index_and_utc_metadata():
    frame = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    frame.index.name = "timestamp"
    result = prepare_utc_kline_frame(frame)
    assert result.empty
    assert isinstance(result.index, pd.DatetimeIndex)
    assert result.index.tz is None
    assert result.index.name == "timestamp"
    assert result.attrs["cts_timestamp_timezone"] == "UTC"


def test_empty_incoming_cannot_certify_unmarked_history():
    existing = _ohlcv(pd.date_range("2026-01-01", periods=5, freq="1h"))
    incoming = prepare_utc_kline_frame(existing.iloc[:0])
    with pytest.raises(ValueError, match="empty incoming bars cannot certify"):
        _heal_local_existing_against_utc(existing, incoming)


@pytest.mark.parametrize("invalid",["nat","duplicates","future"])
def test_writer_rejects_invalid_or_ambiguous_indices_without_dropping_rows(invalid):
    index = pd.date_range("2026-01-01",periods=3,freq="1h",tz="UTC")
    frame = _ohlcv(index)
    if invalid == "nat": frame.index = pd.DatetimeIndex([index[0],pd.NaT,index[2]])
    elif invalid == "duplicates": frame.index = pd.DatetimeIndex([index[0],index[0],index[2]])
    else: frame.index = pd.date_range(datetime.now(timezone.utc)+timedelta(days=2),periods=3,freq="1h")
    with pytest.raises(ValueError): prepare_utc_kline_frame(frame)


def test_full_nonflat_authoritative_overlap_can_certify_unmarked_utc_without_shifting():
    existing = _ohlcv(pd.date_range("2026-01-01",periods=5,freq="1h"))
    incoming = prepare_utc_kline_frame(existing,timestamp_timezone="UTC")
    result = _heal_local_existing_against_utc(existing,incoming)
    pd.testing.assert_index_equal(result.index,existing.index)
    assert result.attrs["cts_timestamp_timezone"] == "UTC"
    assert result.attrs["cts_timestamp_origin"] == "verified_complete_utc_overlap"


@pytest.mark.parametrize("offset", [-8, 8])
def test_full_overlap_rejects_a_complete_eight_hour_periodic_competing_match(offset):
    index = pd.date_range("2026-01-01", periods=48, freq="1h")
    source = _ohlcv(index)
    source.loc[:, :] = np.tile(source.iloc[:8].to_numpy(), (6, 1))
    incoming = prepare_utc_kline_frame(source, timestamp_timezone="UTC")
    existing = source.iloc[8:32].copy()
    existing.attrs = {}
    # Cover just one shifted candidate, to exercise both directions separately.
    start = min(existing.index.min(), existing.index.min() + pd.Timedelta(hours=offset))
    end = max(existing.index.max(), existing.index.max() + pd.Timedelta(hours=offset))
    incoming = incoming.loc[start:end]
    before = existing.copy()
    with pytest.raises(ValueError, match="matches are ambiguous"):
        _heal_local_existing_against_utc(existing, incoming)
    pd.testing.assert_frame_equal(existing, before)


@pytest.mark.parametrize("kind",["partial","flat","mixed","duplicate","nat","volume_conflict"])
def test_legacy_merge_evidence_must_cover_the_whole_valid_nonflat_file(kind):
    existing = _ohlcv(pd.date_range("2026-01-01",periods=24,freq="1h"))
    incoming = prepare_utc_kline_frame(existing,timestamp_timezone="UTC")
    if kind == "partial": incoming = incoming.tail(3)
    elif kind == "flat":
        existing.loc[:,:]=100.
        incoming = prepare_utc_kline_frame(existing,timestamp_timezone="UTC")
    elif kind == "mixed":
        existing = pd.concat([existing,existing.iloc[:6].set_axis(existing.index[:6]+pd.Timedelta(hours=8))])
    elif kind == "duplicate": existing = pd.concat([existing,existing.iloc[[0]]])
    elif kind == "volume_conflict": existing.loc[existing.index[0],"volume"] += 1
    else: existing.index = pd.DatetimeIndex([pd.NaT]+list(existing.index[1:]))
    with pytest.raises(ValueError): _heal_local_existing_against_utc(existing,incoming)
