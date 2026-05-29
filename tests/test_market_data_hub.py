from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from core.marketdata.hub import MarketDataHub, normalize_market_symbol


def test_normalize_market_symbol_handles_ccxt_perp_and_binance_compact_symbols():
    assert normalize_market_symbol("BTC/USDT:USDT") == "BTC/USDT"
    assert normalize_market_symbol("btcusdt") == "BTC/USDT"
    assert normalize_market_symbol("ETH/USDT") == "ETH/USDT"


def test_hub_records_ws_tick_with_metadata_and_status_snapshot():
    hub = MarketDataHub(symbol_max_age_sec=10, exchange_max_age_sec=15)

    tick = hub.upsert_ws_tick(
        "Binance",
        "BTC/USDT:USDT",
        {
            "last": "68000.5",
            "bid": "68000.0",
            "ask": "68001.0",
            "timestamp": "2026-05-29T08:00:00+00:00",
        },
    )

    assert tick is not None
    assert tick.exchange == "binance"
    assert tick.symbol == "BTC/USDT"
    assert tick.source == "ws"

    current = hub.get_tick("binance", "BTCUSDT")
    assert current is not None
    assert current["tick"]["last"] == 68000.5
    assert current["meta"]["source"] == "ws"
    assert current["meta"]["is_stale"] is False

    snapshot = hub.snapshot(include_symbols=True)
    assert snapshot["hub_healthy"] is True
    assert snapshot["healthy_exchanges"] == ["binance"]
    assert snapshot["symbol_count"] == 1
    assert snapshot["ws_tick_count"] == 1
    assert snapshot["symbols"]["binance"]["BTC/USDT"]["quality"] == "healthy"


def test_hub_counts_rest_fallback_and_preserves_source_metadata():
    hub = MarketDataHub()

    tick = hub.upsert_rest_tick(
        "gate",
        "ETH/USDT",
        {"last": 3800, "bid": 3799, "ask": 3801, "timestamp": 1_700_000_000_000},
        reason="ws_unhealthy",
    )

    assert tick is not None
    assert tick.source == "rest_fallback"
    snapshot = hub.snapshot()
    assert snapshot["rest_fallback_count"] == 1
    assert snapshot["fallback_reasons"] == {"ws_unhealthy": 1}
    assert snapshot["ws_tick_count"] == 0


def test_hub_rejects_invalid_payloads_without_overwriting_good_tick():
    hub = MarketDataHub()

    good = hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 100.0, "bid": 99.0, "ask": 101.0})
    assert good is not None

    assert hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 0.0}) is None
    assert hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 100.0, "bid": 102.0, "ask": 101.0}) is None

    current = hub.get_tick("binance", "BTC/USDT")
    assert current is not None
    assert current["tick"]["last"] == 100.0
    assert hub.snapshot()["invalid_payload_count"] == 2


def test_hub_marks_stale_ticks_by_symbol_age():
    hub = MarketDataHub(symbol_max_age_sec=5)
    tick = hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 100.0})
    assert tick is not None

    key = ("binance", "BTC/USDT", "ticker")
    hub._ticks[key] = replace(  # type: ignore[attr-defined]
        tick,
        timestamp_received=datetime.now(timezone.utc) - timedelta(seconds=30),
    )

    current = hub.get_tick("binance", "BTC/USDT")
    assert current is not None
    assert current["meta"]["is_stale"] is True
    assert current["meta"]["fallback_required"] is True
    assert hub.snapshot()["stale_symbol_count"] == 1
    assert hub.snapshot()["ws_stale_symbol_count"] == 1


def test_hub_rejects_exchange_timestamp_regression():
    hub = MarketDataHub()

    first = hub.upsert_ws_tick(
        "binance",
        "BTC/USDT",
        {"last": 100.0, "timestamp": "2026-05-29T08:00:00+00:00"},
    )
    second = hub.upsert_ws_tick(
        "binance",
        "BTC/USDT",
        {"last": 90.0, "timestamp": "2026-05-29T07:59:59+00:00"},
    )

    assert first is not None
    assert second is first
    current = hub.get_tick("binance", "BTC/USDT")
    assert current is not None
    assert current["tick"]["last"] == 100.0
    snapshot = hub.snapshot()
    assert snapshot["timestamp_regression_count"] == 1
    assert snapshot["last_error"] == "timestamp_regression:binance:BTC/USDT"


def test_shadow_compare_accepts_ws_rest_diff_inside_threshold():
    hub = MarketDataHub(max_price_diff_bps=20, shadow_compare_enabled=True)

    hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 100.1})
    assert hub.snapshot()["shadow_compare_count"] == 0

    hub.upsert_rest_tick(
        "binance",
        "BTC/USDT",
        {"last": 100.0},
        source="rest_snapshot",
    )

    snapshot = hub.snapshot()
    compare = snapshot["shadow_last_compare"]
    assert snapshot["shadow_compare_count"] == 1
    assert snapshot["shadow_compare_violation_count"] == 0
    assert snapshot["shadow_missing_ws_count"] == 0
    assert snapshot["shadow_max_abs_diff_bps"] == pytest.approx(10.0)
    assert compare["accepted"] is True
    assert compare["diff_bps"] == pytest.approx(10.0)
    assert compare["ws_last"] == 100.1
    assert compare["rest_last"] == 100.0
    assert compare["trigger_source"] == "rest_snapshot"


def test_shadow_compare_counts_price_diff_violation():
    hub = MarketDataHub(max_price_diff_bps=5, shadow_compare_enabled=True)

    hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 101.0})
    hub.upsert_rest_tick(
        "binance",
        "BTC/USDT",
        {"last": 100.0},
        source="rest_snapshot",
    )

    snapshot = hub.snapshot()
    assert snapshot["shadow_compare_count"] == 1
    assert snapshot["shadow_compare_violation_count"] == 1
    assert snapshot["shadow_last_compare"]["accepted"] is False
    assert snapshot["shadow_last_compare"]["abs_diff_bps"] == pytest.approx(100.0)


def test_shadow_compare_skips_stale_ws_input_on_rest_trigger():
    hub = MarketDataHub(
        max_price_diff_bps=5,
        shadow_compare_enabled=True,
        shadow_compare_max_age_sec=1,
    )

    ws_tick = hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 101.0})
    assert ws_tick is not None
    stale_ws = replace(
        ws_tick,
        timestamp_received=datetime.now(timezone.utc) - timedelta(seconds=5),
    )
    hub._ticks[("binance", "BTC/USDT", "ticker")] = stale_ws  # type: ignore[attr-defined]
    hub._source_ticks[("ws", "binance", "BTC/USDT", "ticker")] = stale_ws  # type: ignore[attr-defined]

    hub.upsert_rest_tick(
        "binance",
        "BTC/USDT",
        {"last": 100.0},
        source="rest_snapshot",
    )

    snapshot = hub.snapshot()
    assert snapshot["shadow_compare_count"] == 0
    assert snapshot["shadow_compare_violation_count"] == 0
    assert snapshot["shadow_compare_stale_skip_count"] == 1
    assert snapshot["shadow_max_abs_diff_bps"] == 0.0
    assert snapshot["shadow_last_compare"]["skipped"] is True
    assert snapshot["shadow_last_compare"]["skip_reason"] == "stale_compare_input"
    assert snapshot["shadow_last_compare"]["trigger_source"] == "rest_snapshot"
    assert snapshot["shadow_last_compare"]["ws_age_ms"] >= 4000


def test_shadow_compare_does_not_reuse_stale_rest_baseline_on_ws_push():
    hub = MarketDataHub(
        max_price_diff_bps=5,
        shadow_compare_enabled=True,
        shadow_compare_max_age_sec=1,
    )

    rest = hub.upsert_rest_tick(
        "binance",
        "BTC/USDT",
        {"last": 100.0},
        source="rest_snapshot",
    )
    assert rest is not None
    stale_rest = replace(
        rest,
        timestamp_received=datetime.now(timezone.utc) - timedelta(seconds=5),
    )
    hub._ticks[("binance", "BTC/USDT", "ticker")] = stale_rest  # type: ignore[attr-defined]
    hub._source_ticks[("rest_snapshot", "binance", "BTC/USDT", "ticker")] = stale_rest  # type: ignore[attr-defined]

    hub.upsert_ws_tick("binance", "BTC/USDT", {"last": 101.0})

    snapshot = hub.snapshot()
    assert snapshot["shadow_compare_count"] == 0
    assert snapshot["shadow_compare_stale_skip_count"] == 0
    assert snapshot["shadow_last_compare"] is None


def test_shadow_compare_counts_missing_ws_only_when_enabled():
    disabled = MarketDataHub(shadow_compare_enabled=False)
    disabled.upsert_rest_tick("binance", "BTC/USDT", {"last": 100.0}, source="rest_snapshot")
    assert disabled.snapshot()["shadow_missing_ws_count"] == 0

    enabled = MarketDataHub(shadow_compare_enabled=True)
    enabled.upsert_rest_tick("binance", "BTC/USDT", {"last": 100.0}, source="rest_snapshot")
    snapshot = enabled.snapshot()
    assert snapshot["shadow_missing_ws_count"] == 1
    assert snapshot["shadow_compare_count"] == 0
    assert snapshot["shadow_last_compare"] is None
