from datetime import timezone

import pandas as pd

from core.strategies.strategy_base import SignalType
from strategies.event_driven.supply_event_strategy import SupplyEventStrategy


def _frame(ts: str, **cols):
    index = pd.DatetimeIndex([pd.Timestamp(ts).tz_convert(timezone.utc)])
    row = {
        "close": 1.0,
        "symbol": "ARB/USDT",
        "supply_pressure_score": 0.78,
        "priced_in_score": 0.35,
        "liquidity_score": 0.8,
        "borrow_or_perp_available": True,
        "market_regime": "neutral",
        "short_crowding_score": 0.2,
    }
    row.update(cols)
    return pd.DataFrame([row], index=index)


def _strategy(**params):
    base = {
        "events": [
            {
                "event_id": "ARB_unlock",
                "symbol": "ARB/USDT",
                "base_asset": "ARB",
                "event_type": "token_unlock",
                "event_time": "2026-06-16T00:00:00Z",
                "first_seen_at": "2026-05-20T00:00:00Z",
                "source": "manual",
                "unlock_pct_float": 0.15,
                "recipient_type": "investor",
                "confidence": 0.9,
            }
        ]
    }
    base.update(params)
    return SupplyEventStrategy(params=base)


def test_supply_event_pre_event_window_blocks_longs_and_allows_small_short():
    strategy = _strategy()
    signals = strategy.generate_signals(_frame("2026-06-10T00:00:00Z"))

    assert [s.signal_type for s in signals] == [SignalType.HOLD, SignalType.SELL]
    assert "supply_event_pre_event_long_gate" in signals[0].metadata["reason_codes"]
    assert signals[1].metadata["reason_codes"] == ["supply_event_pre_event_small_short"]


def test_supply_event_post_event_absorption_emits_long():
    strategy = _strategy(emit_gate_hold_signal=False)
    signals = strategy.generate_signals(
        _frame(
            "2026-06-19T00:00:00Z",
            supply_pressure_score=0.2,
            priced_in_score=0.8,
            absorption_score=0.76,
            price_recovers_event_vwap=True,
            funding_reset=True,
            event_low=0.8,
        )
    )

    assert [s.signal_type for s in signals] == [SignalType.BUY]
    assert signals[0].stop_loss == 0.8
    assert signals[0].metadata["reason_codes"] == ["supply_event_post_event_absorption_long"]
