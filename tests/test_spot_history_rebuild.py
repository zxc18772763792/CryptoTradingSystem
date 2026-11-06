from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from core.data import spot_history_rebuild as rebuild
from core.data.parquet_timezone_repair import apply_plan, RepairConflict, StalePlan, sha256


def frame():
    return pd.DataFrame({"open": [10., 11.], "high": [12., 13.], "low": [9., 10.],
                         "close": [11., 12.], "volume": [4., 5.]},
                        index=pd.date_range("2026-01-01", periods=2, freq="h", name="timestamp"))


def test_archive_sha_and_exact_filename_are_required():
    payload = b"official archive"
    checksum = hashlib.sha256(payload).hexdigest() + "  BTCUSDT.zip"
    assert rebuild.checked_sha(payload, checksum, "BTCUSDT.zip") == hashlib.sha256(payload).hexdigest()
    with pytest.raises(rebuild.SourceError, match="SHA-256"):
        rebuild.checked_sha(payload + b"tampered", checksum, "BTCUSDT.zip")
    with pytest.raises(rebuild.SourceError, match="filename"):
        rebuild.checked_sha(payload, checksum, "ETHUSDT.zip")


def test_ms_and_us_archives_have_identical_utc_index():
    stamp = int(pd.Timestamp("2026-01-01").value // 1_000_000)
    ms = rebuild.parse_rows([[stamp, 10, 12, 9, 11, 4]], "1h")
    us = rebuild.parse_rows([[stamp * 1000, 10, 12, 9, 11, 4]], "1h")
    pd.testing.assert_frame_equal(ms, us)
    with pytest.raises(rebuild.SourceError, match="Invalid official OHLCV"):
        rebuild.parse_rows([[stamp, 10, 9, 8, 11, 4]], "1h")
    with pytest.raises(rebuild.SourceError, match="grid"):
        rebuild.parse_rows([[stamp + 1, 10, 12, 9, 11, 4]], "1h")


def test_http_refusal_is_one_attempt_and_stops_batch(tmp_path, monkeypatch):
    source = rebuild.OfficialSpotSource(tmp_path)
    calls = []
    def get(*args, **kwargs):
        calls.append(args)
        return SimpleNamespace(status_code=403)
    monkeypatch.setattr(source, "_session", lambda public_api: SimpleNamespace(get=get))
    with pytest.raises(rebuild.SourceRefused):
        source._get(rebuild.API_URL, public_api=True)
    with pytest.raises(rebuild.SourceRefused):
        source._get(rebuild.API_URL, public_api=True)
    assert len(calls) == 1


def test_reconstruction_replaces_corrupt_values_and_parts_but_preserves_originals(tmp_path):
    directory = tmp_path / "binance" / "ABC_USDT"
    parts = directory / "1h_parts"
    parts.mkdir(parents=True)
    main, part = directory / "1h.parquet", parts / "2026.legacy.parquet"
    old = frame() * 100
    old.index = old.index + pd.Timedelta(hours=8)
    old.to_parquet(main)
    old.to_parquet(part)
    original_hash = sha256(main)
    files = rebuild.series_files(tmp_path, "ABC/USDT", "1h")
    plans = rebuild.reconstruction_plans(files, frame(), "1h", {"source_records": [{"sha256": "checked"}]})
    transaction = apply_plan(plans, tmp_path / "transaction")
    assert transaction["status"] == "complete"
    for path in (main, part):
        actual = pd.read_parquet(path)
        pd.testing.assert_frame_equal(actual, plans[files.index(path)]["result"], check_freq=False)
        assert actual.attrs["cts_timestamp_timezone"] == "UTC"
    assert sha256(Path(transaction["files"][0]["backup"])) == original_hash
    assert pd.read_parquet(transaction["files"][0]["backup"]).equals(old)


def test_stale_reconstruction_cannot_overwrite_a_new_writer(tmp_path):
    path = tmp_path / "1h.parquet"
    frame().to_parquet(path)
    plan = rebuild.reconstruction_plans([path], frame(), "1h", {"source_records": []})
    newer = frame() * 2
    newer.to_parquet(path)
    with pytest.raises(StalePlan):
        apply_plan(plan, tmp_path / "transaction")
    assert pd.read_parquet(path).equals(newer)


def test_newer_verified_rows_and_extra_columns_are_not_discarded(tmp_path):
    path = tmp_path / "1h.parquet"
    newer = frame()
    newer.index += pd.Timedelta(hours=2)
    newer.attrs["cts_timestamp_timezone"] = "UTC"
    newer.to_parquet(path)
    with pytest.raises(RepairConflict, match="Newer verified"):
        rebuild.reconstruction_plans([path], frame(), "1h", {})
    newer["custom"] = 3
    newer.to_parquet(path)
    with pytest.raises(RepairConflict, match="Unexpected stored columns"):
        rebuild.reconstruction_plans([path], frame(), "1h", {})


def test_closed_only_rebuild_archives_open_candle_but_rejects_later_writer(tmp_path):
    path = tmp_path / "1h.parquet"
    original = pd.concat([frame(), frame().iloc[[0]].set_axis(pd.DatetimeIndex(["2026-01-01 02:00"], name="timestamp"))])
    original.attrs["cts_timestamp_timezone"] = "UTC"
    original.to_parquet(path)
    proof = {"closed_only": True, "snapshot_current_open_utc": "2026-01-01T02:00:00"}
    plans = rebuild.reconstruction_plans([path], frame(), "1h", proof)
    assert len(plans[0]["result"]) == 2
    assert len(pd.read_parquet(path)) == 3  # planning preserves the original
    original.index += pd.Timedelta(hours=1)
    original.to_parquet(path)
    with pytest.raises(RepairConflict, match="Newer verified"):
        rebuild.reconstruction_plans([path], frame(), "1h", proof)
    # Even when the last row is forming, an intervening closed row missing
    # from the downloaded reference may not be silently removed.
    proof["snapshot_current_open_utc"] = "2026-01-01T03:00:00"
    with pytest.raises(RepairConflict, match="Newer verified"):
        rebuild.reconstruction_plans([path], frame(), "1h", proof)
