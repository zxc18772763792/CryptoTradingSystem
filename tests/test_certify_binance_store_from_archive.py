import io
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import certify_binance_store_from_archive as cert  # noqa: E402
from core.data.data_storage import _has_utc_timestamp_metadata  # noqa: E402

COLS = ["open", "high", "low", "close", "volume"]


def _zip(csv: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("x.csv", csv)
    return buffer.getvalue()


def _bars(start="2026-01-01", periods=40):
    index = pd.date_range(start, periods=periods, freq="h", name="timestamp")
    base = 100 + np.cumsum(np.sin(np.arange(periods)) + 0.3)
    frame = pd.DataFrame({"open": base, "high": base + 2, "low": base - 2, "close": base + 0.5, "volume": 10.0 + np.arange(periods)},
                         index=index)
    return frame


def test_parse_archive_csv_handles_header_and_microseconds():
    header = "open_time,open,high,low,close,volume,close_time,q,n,tb,tq,i\n"
    millis = int(pd.Timestamp("2026-03-01").value // 1_000_000)
    row = f"{millis},1.0,2.0,0.5,1.5,10.0,{millis + 3599999},0,0,0,0,0\n"
    frame = cert.parse_archive_csv(_zip(header + row))
    assert list(frame.index) == [pd.Timestamp("2026-03-01")] and frame.iloc[0].tolist() == [1.0, 2.0, 0.5, 1.5, 10.0]
    micro = f"{millis * 1000},1.0,2.0,0.5,1.5,10.0,0,0,0,0,0,0\n"
    assert list(cert.parse_archive_csv(_zip(micro)).index) == [pd.Timestamp("2026-03-01")]


def _run(monkeypatch, tmp_path, stored, official, live=None, dry_run=False):
    path = tmp_path / "binance_futures" / "ABC_USDT" / "1h.parquet"
    path.parent.mkdir(parents=True)
    stored.to_parquet(path)
    before = path.read_bytes()
    maint = SimpleNamespace(SeriesKey=lambda *args: args)
    monkeypatch.setattr(cert, "official_frame", lambda *args: (official.copy(), {"dropped_invalid": 0, "missing_files": 0, "missing_sample": []}))
    monkeypatch.setattr(cert, "confirm_with_api", lambda maint_, key, stamps, limiter: {stamp: live[stamp] for stamp in stamps if live and stamp in live})
    result = cert.certify_series(maint, "binance_futures", "futures", "ABC/USDT", "ABCUSDT", "1h",
                                 datetime(2026, 2, 1), tmp_path, None, dry_run)
    return result, path, before


def test_matching_unmarked_history_is_certified_and_extended(monkeypatch, tmp_path):
    official = _bars(periods=40)
    stored = official.iloc[:30]
    result, path, _ = _run(monkeypatch, tmp_path, stored, official)
    assert result["status"] == "certified" and result["added"] == 10
    written = pd.read_parquet(path)
    assert _has_utc_timestamp_metadata(written) and len(written) == 40
    assert (written.loc[stored.index, COLS].to_numpy() == stored[COLS].to_numpy()).all()


def test_revised_bar_needs_the_live_api_to_agree(monkeypatch, tmp_path):
    official = _bars(periods=40)
    stored = official.iloc[:30].copy()
    stamp = stored.index[7]
    stored.loc[stamp, "volume"] += 5.0  # Binance later restated the volume
    result, path, before = _run(monkeypatch, tmp_path, stored, official, live={})
    assert result["status"] == "refused" and result["reason"] == "live_api_disagrees_with_stored_rows"
    assert path.read_bytes() == before  # a refused file is untouched

    live = {stamp: [float(x) for x in stored.loc[stamp]]}
    result, path, _ = _run(monkeypatch, tmp_path / "second", stored, official, live=live)
    assert result["status"] == "certified" and result["confirmed_by_live_api"] == 1
    assert pd.read_parquet(path).loc[stamp, "volume"] == stored.loc[stamp, "volume"]  # the stored value is kept


def test_dry_run_and_outage_fillers(monkeypatch, tmp_path):
    official = _bars(periods=40)
    official.loc[official.index[35], ["open", "high", "low", "close", "volume"]] = [50.0, 50.0, 50.0, 50.0, 0.0]
    stored = official.iloc[:30]
    result, path, before = _run(monkeypatch, tmp_path, stored, official, dry_run=True)
    assert result["status"] == "would_certify" and path.read_bytes() == before
    result, path, _ = _run(monkeypatch, tmp_path / "w", stored, official)
    assert result["status"] == "certified"
    assert official.index[35] not in pd.read_parquet(path).index  # a flat zero-volume filler is never added


def test_verified_and_partially_repaired_files_are_skipped(monkeypatch, tmp_path):
    official = _bars()
    stored = official.iloc[:30].copy()
    stored.attrs.update({"cts_timestamp_timezone": "UTC", "cts_timestamp_schema": 1, "cts_timestamp_status": "verified_utc"})
    result, _, _ = _run(monkeypatch, tmp_path, stored, official)
    assert result["status"] == "already_verified"
    partial = official.iloc[:30].copy()
    partial.attrs.update({"cts_timestamp_status": "unverified_segments_remain"})
    result, _, _ = _run(monkeypatch, tmp_path / "p", partial, official)
    assert result["status"] == "skipped_partially_repaired"


def test_forming_last_candle_is_completed_but_an_earlier_mismatch_is_not(monkeypatch, tmp_path):
    official = _bars(periods=40)
    stored = official.iloc[:30].copy()
    last = stored.index[-1]
    stored.loc[last, ["high", "low", "close", "volume"]] = [stored.loc[last, "open"] + 0.1, stored.loc[last, "open"] - 0.1,
                                                            stored.loc[last, "open"], 1.0]  # still forming when written
    result, path, _ = _run(monkeypatch, tmp_path, stored, official, live={})
    assert result["status"] == "certified" and result["completed_forming_bar"] == str(last)
    assert (pd.read_parquet(path).loc[last, COLS].to_numpy() == official.loc[last, COLS].to_numpy()).all()

    earlier = official.iloc[:30].copy()
    earlier.loc[earlier.index[10], "volume"] += 3.0
    result, _, _ = _run(monkeypatch, tmp_path / "e", earlier, official, live={})
    assert result["status"] == "refused" and result["reason"] == "live_api_disagrees_with_stored_rows"
