from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

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
