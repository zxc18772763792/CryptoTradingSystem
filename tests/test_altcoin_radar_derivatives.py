from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from core.data.coinglass_altcoin import build_derivatives_snapshot_from_market_snapshot
from core.research.altcoin_radar import build_altcoin_rows


def _market_frame(now: datetime) -> pd.DataFrame:
    index = pd.date_range(end=now, periods=48, freq="4H", tz="UTC")
    base = pd.Series(range(len(index)), index=index, dtype=float)
    return pd.DataFrame(
        {
            "open": 100 + base * 0.8,
            "high": 101 + base * 0.8,
            "low": 99 + base * 0.8,
            "close": 100 + base,
            "volume": 1_000_000 + base * 10_000,
        },
        index=index,
    )


def test_build_altcoin_rows_includes_derivatives_context_and_metrics():
    now = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
    rows = build_altcoin_rows(
        market_frames={"AAA/USDT": _market_frame(now)},
        timeframe="4h",
        micro_snapshots={
            "AAA/USDT": {
                "timestamp": "2026-04-18T11:55:00+00:00",
                "orderbook": {"spread_bps": 6.0},
                "aggressor_flow": {"imbalance": 0.11},
                "funding_rate": {"funding_rate": 0.0004},
                "spot_futures_basis": {"basis_pct": 0.0018},
                "payload": {"large_order_count": 2},
            }
        },
        community_snapshots={
            "AAA/USDT": {
                "timestamp": "2026-04-18T11:54:00+00:00",
                "flow_proxy": {"imbalance": 0.15, "buy_ratio": 0.61},
                "announcements": [{"title": "demo"}],
                "payload": {"announcement_count": 1},
            }
        },
        whale_snapshots={
            "AAA/USDT": {
                "timestamp": "2026-04-18T11:53:00+00:00",
                "count": 3,
                "transactions": [{"btc": 15.0}],
                "payload": {},
            }
        },
        derivatives_snapshots={
            "AAA/USDT": {
                "timestamp": "2026-04-18T11:58:00+00:00",
                "source_name": "coinglass_cache",
                "capture_status": "ok",
                "source_error": None,
                "oi_change_1h": 0.08,
                "funding_rate": 0.0003,
                "basis_pct": 0.0015,
                "long_short_ratio": 1.07,
                "taker_buy_sell_imbalance": 0.19,
                "crowding_score": 0.44,
                "squeeze_score": 0.63,
                "distribution_score": 0.22,
                "orderbook_imbalance_score": 0.17,
                "depth_thinness_score": 0.18,
                "payload": {},
            }
        },
        now=now,
    )

    assert len(rows) == 1
    row = rows[0]

    assert row["freshness"]["derivatives_label"] == "fresh"
    assert row["freshness"]["derivatives_age_sec"] == 120.0
    assert row["data_quality"]["derivatives_data_freshness"] >= 0.7
    assert row["derivatives_context"]["available"] is True
    assert row["derivatives_context"]["freshness_label"] == "fresh"
    assert row["derivatives_context"]["source_name"] == "coinglass_cache"
    assert row["metrics"]["basis_pct"] == 0.0015
    assert row["metrics"]["long_short_ratio"] == 1.07
    assert row["metrics"]["crowding_score"] == 0.44
    assert row["metrics"]["depth_thinness_score"] == 0.18


def test_build_altcoin_rows_surfaces_derivatives_labels_and_plan_tags():
    now = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
    rows = build_altcoin_rows(
        market_frames={"AAA/USDT": _market_frame(now)},
        timeframe="4h",
        derivatives_snapshots={
            "AAA/USDT": {
                "timestamp": "2026-04-18T11:58:00+00:00",
                "source_name": "coinglass_cache",
                "capture_status": "ok",
                "source_error": None,
                "oi_change_1h": 8.2,
                "funding_rate": 0.0014,
                "basis_pct": 0.012,
                "long_short_ratio": 1.15,
                "taker_buy_sell_imbalance": 0.21,
                "crowding_score": 0.78,
                "squeeze_score": 0.74,
                "distribution_score": 0.24,
                "orderbook_imbalance_score": 0.28,
                "depth_thinness_score": 0.18,
                "payload": {
                    "history_ready": True,
                    "history_exchange": "Binance",
                    "history_interval": "h1",
                    "funding_mean": 0.0008,
                    "funding_zscore": 1.9,
                    "funding_reversion_speed": 0.22,
                    "long_short_ratio_change_24h": 0.18,
                    "liquidation_burst_score": 0.66,
                    "liquidation_map_pressure_score": 0.58,
                    "liquidation_map_total_usd": 320_000_000.0,
                    "liquidation_map_above_usd": 210_000_000.0,
                    "liquidation_map_below_usd": 110_000_000.0,
                    "liquidation_map_largest_cluster_price": 105.0,
                    "liquidation_map_largest_cluster_usd": 80_000_000.0,
                    "orderbook_agg_bid_usd": 180_000_000.0,
                    "orderbook_agg_ask_usd": 120_000_000.0,
                    "orderbook_agg_imbalance": 0.2,
                    "orderbook_wall_above_usd": 45_000_000.0,
                    "orderbook_wall_below_usd": 55_000_000.0,
                    "liquidity_heatmap_total_usd": 420_000_000.0,
                    "liquidity_heatmap_above_usd": 270_000_000.0,
                    "liquidity_heatmap_below_usd": 150_000_000.0,
                    "liquidity_void_score": 0.25,
                    "heatmap_pressure_score": 0.48,
                    "derivatives_heat_score": 0.84,
                    "crowded_long": True,
                    "squeeze_building": True,
                    "order_flow_confirmed": True,
                    "derivatives_labels": ["crowded_long", "squeeze_building", "order_flow_confirmed"],
                },
            }
        },
        now=now,
    )

    assert len(rows) == 1
    row = rows[0]

    assert row["derivatives_context"]["history_ready"] is True
    assert row["derivatives_context"]["history_exchange"] == "Binance"
    assert row["derivatives_context"]["history_interval"] == "h1"
    assert row["derivatives_context"]["crowded_long"] is True
    assert row["derivatives_context"]["squeeze_building"] is True
    assert row["derivatives_context"]["order_flow_confirmed"] is True
    assert "crowded_long" in row["derivatives_context"]["derivatives_labels"]
    assert row["metrics"]["funding_zscore"] == 1.9
    assert row["metrics"]["funding_reversion_speed"] == 0.22
    assert row["metrics"]["long_short_ratio_change_24h"] == 0.18
    assert row["metrics"]["liquidation_map_pressure_score"] == 0.58
    assert row["metrics"]["orderbook_agg_imbalance"] == 0.2
    assert row["metrics"]["liquidity_heatmap_total_usd"] == 420_000_000.0
    assert row["metrics"]["liquidity_void_score"] == 0.25
    assert row["metrics"]["heatmap_pressure_score"] == 0.48
    assert row["derivatives_context"]["liquidation_map_total_usd"] == 320_000_000.0
    assert row["derivatives_context"]["liquidation_map_largest_cluster_price"] == 105.0
    assert row["derivatives_context"]["orderbook_agg_bid_usd"] == 180_000_000.0
    assert row["derivatives_context"]["liquidity_heatmap_above_usd"] == 270_000_000.0
    assert row["metrics"]["history_ready"] == 1.0
    assert "Crowded Long" in row["tags"]
    assert "Short Squeeze Risk" in row["tags"]
    assert "Order Flow Confirmed" in row["tags"]


def test_build_altcoin_rows_marks_missing_derivatives_in_tags():
    now = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
    rows = build_altcoin_rows(
        market_frames={"AAA/USDT": _market_frame(now)},
        timeframe="4h",
        now=now,
    )

    assert len(rows) == 1
    row = rows[0]

    assert row["freshness"]["derivatives_label"] == "missing"
    assert "derivatives_missing" in row["data_quality"]["degraded_reason"]
    assert "Derivatives Missing" in row["tags"]


def test_build_altcoin_rows_uses_derivatives_snapshot_for_funding_basis_percentile():
    now = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
    rows = build_altcoin_rows(
        market_frames={"AAA/USDT": _market_frame(now)},
        timeframe="4h",
        derivatives_snapshots={
            "AAA/USDT": {
                "timestamp": "2026-04-18T11:58:00+00:00",
                "source_name": "coinglass_cache",
                "capture_status": "ok",
                "source_error": None,
                "funding_rate": 0.0009,
                "basis_pct": 0.0062,
                "oi_change_1h": 0.08,
                "taker_buy_sell_imbalance": 0.19,
                "crowding_score": 0.44,
                "squeeze_score": 0.63,
                "distribution_score": 0.22,
                "orderbook_imbalance_score": 0.17,
                "depth_thinness_score": 0.18,
                "payload": {},
            }
        },
        now=now,
    )

    assert len(rows) == 1
    row = rows[0]

    assert row["metrics"]["funding_rate"] == 0.0009
    assert row["metrics"]["basis_pct"] == 0.0062
    assert row["metrics"]["percentiles"]["funding_basis"] == 0.5


def test_build_altcoin_rows_prefers_coinglass_market_snapshot_when_local_frame_is_stale():
    now = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
    stale_now = now - timedelta(days=30)
    market_snapshot = {
        "symbol": "AAA/USDT",
        "base_symbol": "AAA",
        "timestamp": now.isoformat(),
        "source_name": "coinglass_coins_markets",
        "current_price": 12.5,
        "market_cap_usd": 2_500_000_000,
        "price_change_percent_4h": 6.2,
        "price_change_percent_12h": 11.8,
        "price_change_percent_24h": 19.4,
        "volume_change_percent_4h": 44.0,
        "open_interest_change_percent_1h": 3.4,
        "open_interest_change_percent_4h": 8.8,
        "open_interest_change_percent_24h": 18.2,
        "avg_funding_rate_by_oi": 0.0006,
        "oi_vol_ratio_change_percent_4h": 2.4,
        "long_short_ratio_4h": 1.12,
        "long_volume_usd_4h": 24_000_000,
        "short_volume_usd_4h": 17_000_000,
        "long_liquidation_usd_4h": 180_000,
        "short_liquidation_usd_4h": 620_000,
        "long_liquidation_usd_24h": 950_000,
        "short_liquidation_usd_24h": 2_250_000,
        "latency_ms": 120,
    }
    derivatives_snapshot = build_derivatives_snapshot_from_market_snapshot(market_snapshot)

    rows = build_altcoin_rows(
        market_frames={"AAA/USDT": _market_frame(stale_now)},
        timeframe="4h",
        market_snapshots={"AAA/USDT": market_snapshot},
        derivatives_snapshots={"AAA/USDT": derivatives_snapshot},
        now=now,
    )

    assert len(rows) == 1
    row = rows[0]

    assert row["freshness"]["market_label"] == "fresh"
    assert row["freshness"]["derivatives_label"] == "fresh"
    assert row["derivatives_context"]["source_name"] == "coinglass_coins_markets"
    assert row["data_quality"]["market_data_freshness"] >= 0.9
    assert "market_data_stale" not in row["data_quality"]["degraded_reason"]
    assert row["metrics"]["market_cap_usd"] == 2500000000.0


def test_build_altcoin_rows_does_not_hard_degrade_fresh_public_market_snapshot_without_derivatives():
    now = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
    stale_now = now - timedelta(days=30)
    market_snapshot = {
        "symbol": "PEPE/USDT",
        "base_symbol": "PEPE",
        "timestamp": now.isoformat(),
        "source_name": "binance_futures_ticker_24h",
        "current_price": 0.00000417,
        "quote_volume_24h": 18_000_000.0,
        "price_change_percent_24h": 12.0,
        "spread_bps": 4.0,
    }

    rows = build_altcoin_rows(
        market_frames={"PEPE/USDT": _market_frame(stale_now)},
        timeframe="4h",
        market_snapshots={"PEPE/USDT": market_snapshot},
        derivatives_snapshots={},
        now=now,
    )

    assert len(rows) == 1
    row = rows[0]

    assert row["freshness"]["market_label"] == "fresh"
    assert row["freshness"]["derivatives_label"] == "missing"
    assert "market_data_stale" not in row["data_quality"]["degraded_reason"]
    assert "snapshot_missing" not in row["data_quality"]["degraded_reason"]
    assert "derivatives_missing" not in row["data_quality"]["degraded_reason"]
    assert row["metrics"]["last_price"] == 0.00000417
    assert row["metrics"]["avg_dollar_volume"] == 3000000.0


def test_build_altcoin_rows_suppresses_major_benchmark_symbols():
    now = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc)
    market_snapshot = {
        "symbol": "BTC/USDT",
        "base_symbol": "BTC",
        "timestamp": now.isoformat(),
        "source_name": "coinglass_coins_markets",
        "current_price": 76162.5,
        "market_cap_usd": 1_525_000_000_000,
        "price_change_percent_4h": 1.6,
        "price_change_percent_12h": 2.8,
        "price_change_percent_24h": 4.1,
        "volume_change_percent_4h": 31.0,
        "open_interest_change_percent_1h": 2.9,
        "open_interest_change_percent_4h": 7.5,
        "open_interest_change_percent_24h": 14.2,
        "avg_funding_rate_by_oi": 0.0011,
        "oi_vol_ratio_change_percent_4h": 3.1,
        "long_short_ratio_4h": 1.18,
        "long_volume_usd_4h": 2_150_000_000,
        "short_volume_usd_4h": 1_420_000_000,
        "long_liquidation_usd_4h": 1_650_000,
        "short_liquidation_usd_4h": 5_250_000,
        "long_liquidation_usd_24h": 8_750_000,
        "short_liquidation_usd_24h": 21_250_000,
        "latency_ms": 100,
    }
    derivatives_snapshot = build_derivatives_snapshot_from_market_snapshot(market_snapshot)

    rows = build_altcoin_rows(
        market_frames={},
        timeframe="4h",
        market_snapshots={"BTC/USDT": market_snapshot},
        derivatives_snapshots={"BTC/USDT": derivatives_snapshot},
        now=now,
    )

    assert len(rows) == 1
    row = rows[0]

    assert row["alt_eligible"] is False
    assert row["signal_state"] == ""
    assert "Benchmark Excluded" in row["tags"]
