import asyncio
from datetime import datetime, timezone

from core.ai import signal_engine


def test_signal_engine_pm_overlay(monkeypatch):
    async def fake_recent_events(symbol, since_minutes):
        return [{
            "event_id": "e1",
            "ts": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "event_type": "macro",
            "sentiment": 1,
            "impact_score": 0.5,
            "half_life_min": 120,
            "evidence": {"source": "test"},
        }]

    async def fake_pm_features(symbol, ts, timeframe="1m"):
        return {"pm_price_signal": 0.6, "pm_global_risk": 0.1, "pm_macro_shock_sev": 0.2}

    monkeypatch.setattr(signal_engine.news_db, "get_recent_events", fake_recent_events)
    monkeypatch.setattr(signal_engine.pm_db, "get_features_asof", fake_pm_features)
    monkeypatch.setenv("PM_FEATURES_ENABLE", "true")

    result = asyncio.run(
        signal_engine.generate_signal(
            symbol="BTCUSDT",
            market_features={"spread": 0.001, "vol_1h": 0.02},
            since_minutes=60,
            cfg={"thresholds": {"alpha_threshold": 0.05}, "symbols": {}},
        )
    )
    assert result["signal"] in {"LONG", "FLAT"}
    assert any("pm boost" in msg for msg in result["explain"])
