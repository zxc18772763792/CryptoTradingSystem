"""Read-only scan for perp bars in the spot kline store (scripts/scan_kline_venue_pollution.py)."""
from __future__ import annotations

import importlib.util
import io
import zipfile
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "scan_kline_venue_pollution.py"
TODAY = datetime(2026, 10, 27, 12, 0)
COMPACT = {"cts_timestamp_timezone": "UTC", "cts_timestamp_schema": 1,
           "cts_timestamp_origin": "writer:binance_compact", "cts_timestamp_status": "verified_utc"}


@pytest.fixture(scope="module")
def scan():
    spec = importlib.util.spec_from_file_location("scan_kline_venue_pollution_under_test", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _bars(start: str, periods: int, freq: str, shift: float) -> pd.DataFrame:
    """Bars whose prices are unique per minute of time, so 15m and 1h bars cut from them agree at hour edges."""
    index = pd.date_range(start, periods=periods, freq=freq, name="timestamp")
    minutes = (index - pd.Timestamp("2026-01-01")).total_seconds().to_numpy() / 60.0
    step = pd.Timedelta(freq).total_seconds() / 60.0
    opens = 100.0 + minutes / 64.0 + shift
    closes = 100.0 + (minutes + step) / 64.0 + shift  # the next bar's open
    return pd.DataFrame({"open": opens, "high": np.maximum(opens, closes) + 1.0,
                         "low": np.minimum(opens, closes) - 1.0, "close": closes,
                         "volume": np.full(len(index), 5.0)}, index=index)


def _zip(frame: pd.DataFrame) -> bytes:
    ms = (frame.index - pd.Timestamp("1970-01-01")).total_seconds().astype("int64") * 1000
    lines = [f"{t},{o},{h},{l},{c},{v},0,0,0,0,0,0" for t, o, h, l, c, v in
             zip(ms, frame.open, frame.high, frame.low, frame.close, frame.volume)]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("bars.csv", "\n".join(lines))
    return buffer.getvalue()


def _write(path: Path, frame: pd.DataFrame, attrs: dict | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = frame.copy()
    frame.attrs = dict(attrs or {})
    frame.to_parquet(path)


SPOT, PERP = 0.0, 0.03125


@pytest.fixture()
def store(tmp_path, scan, monkeypatch):
    """XYZ: spot archive for September only; perp history for August and September.

    15m main file (unmarked): Sep 10 holds perp bars, Sep 11 spot bars, Aug 20 perp bars (no spot that month),
    Sep 15 perp bars 8 hours late. 5m main file: only those late perp bars.
    1h main file carries the spot collector's marker and is skipped. Parts are clean spot bars.
    """
    root = tmp_path / "historical"
    monkeypatch.setattr(scan, "ROOT", root)
    spot_1h = _bars("2026-09-01", 30 * 24, "1h", SPOT)
    perp_1h = _bars("2026-08-01", 61 * 24, "1h", PERP)
    _write(root / "binance_futures" / "XYZ_USDT" / "1h.parquet", perp_1h)
    shifted = _bars("2026-09-14 16:00", 96, "15min", PERP)  # perp bars stored on the local clock (UTC+8)
    shifted.index = shifted.index + pd.Timedelta(hours=8)
    main = pd.concat([_bars("2026-08-20", 96, "15min", PERP), _bars("2026-09-10", 96, "15min", PERP),
                      _bars("2026-09-11", 96, "15min", SPOT), shifted])
    _write(root / "binance" / "XYZ_USDT" / "15m.parquet", main)
    late_5m = _bars("2026-09-14 16:00", 288, "5min", PERP)  # a file whose only finding is the 8-hour offset
    late_5m.index = late_5m.index + pd.Timedelta(hours=8)
    _write(root / "binance" / "XYZ_USDT" / "5m.parquet", late_5m)
    _write(root / "binance" / "XYZ_USDT" / "1h.parquet", perp_1h.loc["2026-09"], COMPACT)
    _write(root / "binance" / "XYZ_USDT" / "15m_parts" / "2026-09.parquet", _bars("2026-09-12", 96, "15min", SPOT))
    requests = []

    def fetch_zip(path):
        requests.append(path)
        if path.startswith("spot/monthly/klines/XYZUSDT/1h/XYZUSDT-1h-2026-09"):
            return _zip(spot_1h)
        if path.startswith("spot/daily/klines/XYZUSDT/15m/"):
            day = path[-14:-4]
            return _zip(_bars(day, 96, "15min", SPOT)) if day.startswith("2026-09") else None
        if path.startswith("futures/um/daily/klines/XYZUSDT/15m/"):
            return _zip(_bars(path[-14:-4], 96, "15min", PERP))
        return None

    monkeypatch.setattr(scan.ARCHIVE, "fetch_zip", fetch_zip)
    return root, requests


def test_main_files_are_scanned_and_spot_proven_ones_skipped(scan, store, tmp_path):
    root, requests = store
    records, summaries = scan.scan_symbol("XYZ_USDT", ["5m", "15m", "1h"], tmp_path / "cache", TODAY)
    late_5m = [r for r in records if r["series"] == "XYZ_USDT/5m"]
    assert [(r["day"], r["perp"]) for r in late_5m] == [("2026-09-15", 0)] and late_5m[0]["perp_8h"] > 0
    assert late_5m[0]["perp_first"].startswith("2026-09-15")
    flagged = {(r["source"], r["day"]): r for r in records if r["series"] == "XYZ_USDT/15m"}
    assert set(flagged) == {("main", "2026-08-20"), ("main", "2026-09-10"), ("main", "2026-09-15")}
    assert flagged[("main", "2026-09-15")]["perp"] == 0 and flagged[("main", "2026-09-15")]["perp_8h"] > 0
    assert flagged[("main", "2026-09-10")]["perp_8h"] == 0
    assert flagged[("main", "2026-09-10")]["perp"] > 0 and flagged[("main", "2026-09-10")]["perp_only"] == 0
    assert flagged[("main", "2026-08-20")]["perp_only"] > 0 and flagged[("main", "2026-08-20")]["checked"] == 0
    skipped = [s for s in summaries if s.get("skipped")]
    assert skipped == [{"series": "XYZ_USDT/1h", "source": "main",
                        "skipped": "spot provenance marker (writer:binance_compact)"}]
    parts = next(s for s in summaries if s["source"] == "parts")
    assert parts["perp"] == 0 and parts["spot"] > 0
    # Sep 10 is flagged although the local spot 1h file holds the perp bars: the reference is the archive.
    # August (no spot zip) is remembered as missing.
    assert any(r.endswith("XYZUSDT-1h-2026-08.zip") for r in requests)
    requests.clear()
    scan.scan_symbol("XYZ_USDT", ["15m"], tmp_path / "cache", TODAY)
    assert not any("/1h/" in r for r in requests)  # both months come from the cache now


def test_chunked_main_file_reads_give_the_same_days(scan, store, tmp_path, monkeypatch):
    whole, _ = scan.scan_symbol("XYZ_USDT", ["15m"], tmp_path / "cache", TODAY, ("main",))
    monkeypatch.setattr(scan, "CHUNK_ROWS", 50)  # days split across chunks
    chunked, _ = scan.scan_symbol("XYZ_USDT", ["15m"], tmp_path / "cache", TODAY, ("main",))
    assert chunked == whole


def test_confirm_reads_the_flagged_day_of_a_main_file(scan, store, tmp_path):
    records, _ = scan.scan_symbol("XYZ_USDT", ["15m"], tmp_path / "cache", TODAY, ("main",))
    confirmed = {r["day"]: scan.confirm_day(r) for r in records}
    september = confirmed["2026-09-10"]
    assert september["rows_in_day"] == 96 and september["rows_perp_not_spot"] == 96
    late = confirmed["2026-09-15"]
    assert late["rows_perp_not_spot"] == 0 and late["rows_perp_8h"] == 96
    august = confirmed["2026-08-20"]
    assert august["rows_equal_spot_archive"] is None and august["rows_perp_no_spot_archive"] == 96
    assert "rows_perp_not_spot" not in august


def test_reading_one_day_of_a_file_with_utc_aware_stamps(scan, tmp_path):
    frame = _bars("2026-09-10", 192, "15min", SPOT)
    frame.index = frame.index.tz_localize("UTC")
    path = tmp_path / "aware.parquet"
    frame.to_parquet(path)
    day = scan._read(path, day=pd.Timestamp("2026-09-11"))
    assert len(day) == 96 and day.index.tz is None and day.index.min() == pd.Timestamp("2026-09-11")
