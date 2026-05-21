"""Test that /data/klines returns explicit UTC ISO timestamps.

Bug history: parquet indexes are tz-naive UTC. Previously the endpoint
serialized them with ``idx.isoformat()`` which yields a naive string like
``"2026-05-21T05:00:00"``. JavaScript ``new Date(...)`` parses that as
**local time**, visually shifting every candlestick by the user's TZ
offset (e.g. UTC+8 users saw bars 8 hours off). Fix appends an explicit
``Z`` so both sides agree on UTC.

These tests guard the serializer (``_to_utc_iso``) and the three series
endpoints that previously had the bug, without spinning up the full app.
"""
from __future__ import annotations

import importlib.util
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_MODULE_PATH = REPO_ROOT / "web/api/data.py"


@pytest.fixture(scope="module")
def data_module():
    """Load web/api/data.py without importing the whole web.api package
    (which would pull every router and might be unstable in the test env).

    We only need ``_to_utc_iso`` so an isolated module load is sufficient.
    """
    if "web_api_data_kline_tz" in sys.modules:
        return sys.modules["web_api_data_kline_tz"]
    # Try the normal import path first — if it succeeds, prefer it.
    try:
        from web.api import data as _real
        return _real
    except Exception:
        pass
    spec = importlib.util.spec_from_file_location("web_api_data_kline_tz", DATA_MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["web_api_data_kline_tz"] = mod
    spec.loader.exec_module(mod)
    return mod


ISO_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


class TestToUtcIso:
    def test_naive_timestamp_gets_z_suffix(self, data_module):
        # Parquet rows look like this — tz-naive UTC moment.
        ts = pd.Timestamp("2026-05-21 05:00:00")
        out = data_module._to_utc_iso(ts)
        assert out == "2026-05-21T05:00:00Z"
        assert ISO_UTC_RE.match(out), f"output must be unambiguous UTC ISO, got {out!r}"

    def test_tz_aware_utc_kept_as_z(self, data_module):
        ts = pd.Timestamp("2026-05-21 05:00:00", tz="UTC")
        assert data_module._to_utc_iso(ts) == "2026-05-21T05:00:00Z"

    def test_tz_aware_non_utc_converted_to_utc(self, data_module):
        # 13:00 Asia/Shanghai (UTC+8) = 05:00 UTC.
        ts = pd.Timestamp("2026-05-21 13:00:00", tz="Asia/Shanghai")
        assert data_module._to_utc_iso(ts) == "2026-05-21T05:00:00Z"

    def test_python_datetime_naive(self, data_module):
        dt = datetime(2026, 5, 21, 5, 0, 0)
        assert data_module._to_utc_iso(dt) == "2026-05-21T05:00:00Z"

    def test_python_datetime_utc(self, data_module):
        dt = datetime(2026, 5, 21, 5, 0, 0, tzinfo=timezone.utc)
        assert data_module._to_utc_iso(dt) == "2026-05-21T05:00:00Z"

    def test_none_returns_empty_string(self, data_module):
        assert data_module._to_utc_iso(None) == ""

    def test_nat_returns_empty_string(self, data_module):
        assert data_module._to_utc_iso(pd.NaT) == ""

    def test_unparseable_string_returns_text_or_empty(self, data_module):
        # pd.Timestamp can parse some strings; the helper should not raise on garbage.
        out = data_module._to_utc_iso("not a date")
        # Either '' or the raw text passes the no-throw contract; both are safe.
        assert isinstance(out, str)

    def test_iso_string_with_z_roundtrips(self, data_module):
        ts = data_module._to_utc_iso("2026-05-21T05:00:00Z")
        assert ts == "2026-05-21T05:00:00Z"

    def test_microseconds_preserved(self, data_module):
        ts = pd.Timestamp("2026-05-21 05:00:00.123456")
        out = data_module._to_utc_iso(ts)
        assert out.endswith("Z")
        # Either with or without microseconds is acceptable; main contract is "ends in Z".
        assert ISO_UTC_RE.match(out)


class TestSharedHelperParity:
    """The same ``_to_utc_iso`` semantics are duplicated in trading.py and
    backtest.py (to avoid cross-module imports). These tests ensure the three
    implementations stay in lockstep — divergence would silently re-introduce
    the timezone bug in one path but not the other."""

    @pytest.fixture(scope="class")
    def trading_helper(self):
        try:
            from web.api.trading import _to_utc_iso
            return _to_utc_iso
        except Exception as exc:
            pytest.skip(f"web.api.trading not importable in test env: {exc}")

    @pytest.fixture(scope="class")
    def backtest_helper(self):
        try:
            from web.api.backtest import _to_utc_iso
            return _to_utc_iso
        except Exception as exc:
            pytest.skip(f"web.api.backtest not importable in test env: {exc}")

    @pytest.mark.parametrize("input_value,expected", [
        (pd.Timestamp("2026-05-21 05:00:00"), "2026-05-21T05:00:00Z"),
        (pd.Timestamp("2026-05-21 05:00:00", tz="UTC"), "2026-05-21T05:00:00Z"),
        (pd.Timestamp("2026-05-21 13:00:00", tz="Asia/Shanghai"), "2026-05-21T05:00:00Z"),
        (datetime(2026, 5, 21, 5, 0, 0), "2026-05-21T05:00:00Z"),
        (datetime(2026, 5, 21, 5, 0, 0, tzinfo=timezone.utc), "2026-05-21T05:00:00Z"),
        (None, ""),
        (pd.NaT, ""),
    ])
    def test_three_helpers_agree(self, data_module, trading_helper, backtest_helper, input_value, expected):
        assert data_module._to_utc_iso(input_value) == expected
        assert trading_helper(input_value) == expected
        assert backtest_helper(input_value) == expected


class TestJavaScriptCompatibility:
    """The whole point of this fix: JS ``new Date(...)`` must parse the output
    as the SAME moment in time, regardless of the JS runtime's local TZ."""

    def test_output_is_unambiguous_for_javascript(self, data_module):
        # Spec: ISO with 'Z' is always parsed as UTC by JS Date.
        ts = pd.Timestamp("2026-05-21 05:00:00")
        out = data_module._to_utc_iso(ts)
        # The string must end with 'Z' OR include explicit ±HH:MM offset.
        assert out.endswith("Z") or re.search(r"[+-]\d{2}:?\d{2}$", out)

    def test_naive_iso_would_be_ambiguous(self, data_module):
        """Sanity: confirm the OLD format (no Z) would actually be ambiguous —
        this documents *why* we fixed it."""
        ts = pd.Timestamp("2026-05-21 05:00:00")
        old_format = ts.isoformat()  # naive ISO; this is what the bug returned
        assert not old_format.endswith("Z")
        assert old_format == "2026-05-21T05:00:00"
        # And the new format is unambiguous:
        new_format = data_module._to_utc_iso(ts)
        assert new_format == "2026-05-21T05:00:00Z"
        assert new_format != old_format  # the fix changes the wire format


class TestKlineFallbackWriters:
    def test_public_trade_resample_uses_utc_timestamps(self, data_module):
        trades = [
            {"timestamp": 1779343200000, "price": 100.0, "amount": 1.0},
            {"timestamp": 1779343260000, "price": 102.0, "amount": 2.0},
        ]

        out = data_module._trades_to_ohlcv(trades, "1m")

        assert not out.empty
        assert str(out.index.tz) == "UTC"
        assert out.index[0].isoformat() == "2026-05-21T06:00:00+00:00"
