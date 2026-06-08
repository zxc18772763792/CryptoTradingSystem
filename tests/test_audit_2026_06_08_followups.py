"""Follow-up tests for the 2026-06-08 audit fixes (CODE_AUDIT_2026-06-08.md).

Locks in three signal-completeness changes that previously had no coverage:
  1. SupplyEventStrategy.check_exit — closes a position once the event window expires.
  2. HurstExponentStrategy trending-mode SELL — symmetric to the trending BUY.
  3. MaxDrawdownStrategy SELL — reversal off a strong run-up (mirror of the BUY).
"""
from __future__ import annotations

from datetime import timezone
from types import SimpleNamespace

import numpy as np
import pandas as pd

from core.strategies.strategy_base import SignalType
from strategies.event_driven.supply_event_strategy import SupplyEventStrategy
from strategies.factor_based.factor_strategies import (
    HurstExponentStrategy,
    MaxDrawdownStrategy,
)


def _ohlcv(close, *, symbol: str = "BTC/USDT") -> pd.DataFrame:
    arr = np.asarray(close, dtype=float)
    return pd.DataFrame(
        {
            "open": arr,
            "high": arr * 1.002,
            "low": arr * 0.998,
            "close": arr,
            "volume": np.full(len(arr), 1000.0),
            "symbol": [symbol] * len(arr),
        },
        index=pd.date_range("2026-01-01", periods=len(arr), freq="1h"),
    )


def _pos(side: str) -> SimpleNamespace:
    return SimpleNamespace(side=side, symbol="ARB/USDT", entry_price=1.0, metadata={})


# ───────────────────────────── SupplyEventStrategy.check_exit ─────────────────────────────


def _supply_frame(ts: str) -> pd.DataFrame:
    index = pd.DatetimeIndex([pd.Timestamp(ts).tz_convert(timezone.utc)])
    return pd.DataFrame(
        [{
            "close": 1.0,
            "symbol": "ARB/USDT",
            "supply_pressure_score": 0.5,
            "priced_in_score": 0.35,
            "liquidity_score": 0.8,
            "borrow_or_perp_available": True,
            "market_regime": "neutral",
            "short_crowding_score": 0.2,
        }],
        index=index,
    )


def _supply_strategy() -> SupplyEventStrategy:
    return SupplyEventStrategy(params={
        "events": [{
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
        }]
    })


def test_supply_event_check_exit_closes_long_after_window_expires():
    strategy = _supply_strategy()
    # 2026-07-15 is well past the post-event window (event 06-16 ± 14d) → no active events.
    sig = strategy.check_exit(_supply_frame("2026-07-15T00:00:00Z"), _pos("long"))
    assert sig is not None
    assert sig.signal_type == SignalType.CLOSE_LONG
    assert sig.metadata["exit_reason"] == "event_window_expired"


def test_supply_event_check_exit_closes_short_after_window_expires():
    strategy = _supply_strategy()
    sig = strategy.check_exit(_supply_frame("2026-07-15T00:00:00Z"), _pos("short"))
    assert sig is not None
    assert sig.signal_type == SignalType.CLOSE_SHORT
    assert sig.metadata["exit_reason"] == "event_window_expired"


def test_supply_event_check_exit_holds_while_event_active():
    strategy = _supply_strategy()
    # 2026-06-10 is inside the pre-event window → event still active → no exit.
    assert strategy.check_exit(_supply_frame("2026-06-10T00:00:00Z"), _pos("long")) is None


def test_supply_event_check_exit_returns_none_on_empty_data():
    assert _supply_strategy().check_exit(pd.DataFrame(), _pos("long")) is None


# ───────────────────────────── HurstExponentStrategy trending SELL ─────────────────────────────

# Deterministic series with variance-ratio ≈ 1.87 (trending regime, VR > 1.20) where the
# 20→ z-score crosses DOWN through -1.5 on the final bar (prev_z ≈ -1.455, cur_z ≈ -1.683).
_HURST_TREND_SELL_CLOSE = [
    100.012, 99.9785, 100.054, 100.0423, 99.9277, 99.9407, 100.0884, 100.1757, 100.0083,
    99.7502, 99.5818, 99.5061, 99.0713, 98.9438, 98.6575, 98.4411, 98.2462, 98.0785,
    98.0113, 98.0301, 97.8702, 97.9232, 97.6713, 97.5622, 97.5271, 97.3671, 97.0787,
    96.7586, 96.5001, 96.3336, 95.9836, 95.7432, 95.504, 95.3589, 95.1609, 94.9769,
    94.6434, 94.3788, 94.2375, 94.1901, 93.7484, 93.6913, 93.6043, 93.4317, 93.1808,
    92.8436,
]
_HURST_PARAMS = {"hurst_period": 40, "zscore_period": 10, "zscore_threshold": 1.5}


def test_hurst_trending_mode_emits_symmetric_sell():
    strategy = HurstExponentStrategy(params=dict(_HURST_PARAMS))
    df = _ohlcv(_HURST_TREND_SELL_CLOSE)
    signals = strategy.generate_signals(df)

    sells = [s for s in signals if s.signal_type == SignalType.SELL]
    assert len(sells) == 1, f"expected one trending SELL, got {[s.signal_type for s in signals]}"
    sell = sells[0]
    assert sell.metadata["regime"] == "trending"
    assert sell.metadata["variance_ratio"] > 1.20
    price = df["close"].iloc[-1]
    # Short bias: protective stop sits above price, target below it.
    assert sell.stop_loss > price
    assert sell.take_profit < price


def test_hurst_no_signal_when_insufficient_history():
    strategy = HurstExponentStrategy(params=dict(_HURST_PARAMS))
    assert strategy.generate_signals(_ohlcv([100.0] * 10)) == []


# ───────────────────────────── MaxDrawdownStrategy SELL ─────────────────────────────

_MD_PARAMS = {"lookback": 10, "dd_threshold": -0.10, "recovery_threshold": 0.30}


def test_maxdrawdown_emits_sell_on_runup_reversal():
    strategy = MaxDrawdownStrategy(params=dict(_MD_PARAMS))
    # Run up 100 → 130 off the low, then reverse down on the last bars.
    close = [100, 100, 100, 105, 110, 115, 120, 125, 130, 128, 124, 119]
    signals = strategy.generate_signals(_ohlcv(close))

    assert len(signals) == 1
    sell = signals[0]
    assert sell.signal_type == SignalType.SELL
    assert sell.metadata["runup"] >= abs(_MD_PARAMS["dd_threshold"])
    assert sell.metadata["retracement"] > _MD_PARAMS["recovery_threshold"]
    price = close[-1]
    assert sell.stop_loss > price   # short stop above
    assert sell.take_profit < price  # short target below


def test_maxdrawdown_still_emits_buy_on_drawdown_recovery():
    """Regression: the new SELL branch must not suppress the original BUY."""
    strategy = MaxDrawdownStrategy(params=dict(_MD_PARAMS))
    # Drop 100 → 80 (20% drawdown, prev bar still ≤ -10%), recover with last bar up.
    close = [100, 100, 100, 95, 90, 85, 80, 82, 84, 86, 84, 88]
    signals = strategy.generate_signals(_ohlcv(close))

    assert len(signals) == 1
    assert signals[0].signal_type == SignalType.BUY
    assert signals[0].metadata["recovery"] > _MD_PARAMS["recovery_threshold"]


def test_maxdrawdown_silent_on_flat_market():
    strategy = MaxDrawdownStrategy(params=dict(_MD_PARAMS))
    assert strategy.generate_signals(_ohlcv([100.0] * 20)) == []
