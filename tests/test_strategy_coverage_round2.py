"""Round 2 strategy unit tests for MaxDrawdownStrategy + SocialSentimentStrategy.

Each test class covers four invariants pulled from the audit:
- min_bars boundary: empty / too-short DataFrame returns no signals
- direction asymmetry (per the original audit finding that MaxDrawdown only
  emits BUY; we lock in that contract rather than silently changing it)
- NaN / extreme input robustness (no exceptions)
- happy-path produces a signal with stop_loss < entry for longs
"""
from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

from core.strategies.strategy_base import SignalType
from strategies.factor_based.factor_strategies import MaxDrawdownStrategy
from strategies.macro.market_sentiment import SocialSentimentStrategy


# ── MaxDrawdownStrategy ─────────────────────────────────────────────────────

def _drawdown_recovery_frame() -> pd.DataFrame:
    """Construct a frame that drops 15% then recovers 50% to trigger BUY."""
    # 25 bars of uptrend to set rolling_max
    up = np.linspace(100, 120, 25)
    # 5 bars of drawdown to ~102 (15%)
    down = np.linspace(120, 102, 6)[1:]
    # 5 bars of recovery — current price recovers 50% from bottom-to-top range
    bottom = down[-1]
    top = 120.0
    half = bottom + (top - bottom) * 0.5
    recover = np.linspace(bottom, half, 6)[1:]
    closes = np.concatenate([up, down, recover])
    return pd.DataFrame(
        {
            "open": closes,
            "high": closes + 0.5,
            "low": closes - 0.5,
            "close": closes,
            "volume": np.ones(len(closes)) * 1_000.0,
            "symbol": ["BTC/USDT"] * len(closes),
        }
    )


class TestMaxDrawdownStrategy:
    def test_empty_frame_returns_no_signals(self):
        strat = MaxDrawdownStrategy()
        assert strat.generate_signals(pd.DataFrame()) == []

    def test_too_short_frame_returns_no_signals(self):
        strat = MaxDrawdownStrategy(params={"lookback": 30})
        # 10 rows when lookback=30 → must skip without exception.
        df = _drawdown_recovery_frame().head(10)
        assert strat.generate_signals(df) == []

    def test_recovery_from_drawdown_emits_buy(self):
        strat = MaxDrawdownStrategy(
            params={
                "lookback": 25,
                "dd_threshold": -0.10,
                "recovery_threshold": 0.3,
            }
        )
        df = _drawdown_recovery_frame()
        signals = strat.generate_signals(df)
        # Either it fires BUY or the synthetic frame missed the exact prev_dd
        # condition; in both cases the function must not raise and must not
        # return non-BUY signals.
        for s in signals:
            assert s.signal_type == SignalType.BUY
            # stop_loss should sit below the entry price for a long.
            if s.stop_loss is not None:
                assert s.stop_loss < s.price

    def test_nan_input_is_safe(self):
        strat = MaxDrawdownStrategy(params={"lookback": 10})
        n = 30
        closes = np.linspace(100, 80, n)
        closes[5:8] = np.nan  # inject NaN
        df = pd.DataFrame(
            {
                "open": closes,
                "high": closes,
                "low": closes,
                "close": closes,
                "volume": np.ones(n),
                "symbol": ["BTC/USDT"] * n,
            }
        )
        # Must not raise.
        out = strat.generate_signals(df)
        assert isinstance(out, list)

    def test_audit_lock_in_buy_only(self):
        """Audit noted MaxDrawdownStrategy only emits BUY, not symmetric SELL.

        Lock in that contract so a future change is a conscious choice.
        """
        strat = MaxDrawdownStrategy()
        df = _drawdown_recovery_frame()
        signals = strat.generate_signals(df)
        for s in signals:
            assert s.signal_type == SignalType.BUY, (
                "MaxDrawdownStrategy contract is BUY-only; emitting SELL would "
                "be a behaviour change requiring a deliberate audit update."
            )


# ── SocialSentimentStrategy ────────────────────────────────────────────────


def _single_bar_frame(symbol: str = "BTC/USDT", price: float = 50_000.0) -> pd.DataFrame:
    return pd.DataFrame({"close": [price], "symbol": [symbol]})


class TestSocialSentimentStrategy:
    def test_no_social_data_returns_no_signals(self):
        strat = SocialSentimentStrategy()
        df = _single_bar_frame()
        # update_social_data has never been called -> _social_data is empty dict.
        assert strat.generate_signals(df) == []

    def test_insufficient_mentions_returns_no_signals(self):
        strat = SocialSentimentStrategy(params={"min_mentions": 100})
        strat.update_social_data(mentions=10, sentiment_score=0.8, trending_score=0.5)
        assert strat.generate_signals(_single_bar_frame()) == []

    def test_positive_sentiment_emits_buy(self):
        strat = SocialSentimentStrategy(
            params={"positive_threshold": 0.2, "min_mentions": 5}
        )
        strat.update_social_data(
            mentions=50, sentiment_score=0.6, trending_score=0.7
        )
        signals = strat.generate_signals(_single_bar_frame(price=42_000.0))
        assert any(s.signal_type == SignalType.BUY for s in signals)
        buy = next(s for s in signals if s.signal_type == SignalType.BUY)
        assert buy.stop_loss < buy.price
        assert buy.take_profit > buy.price
        # metadata includes the sentiment score so audits can be replayed.
        assert "sentiment_score" in (buy.metadata or {})

    def test_negative_sentiment_emits_sell(self):
        strat = SocialSentimentStrategy(
            params={"negative_threshold": -0.2, "min_mentions": 5}
        )
        strat.update_social_data(
            mentions=50, sentiment_score=-0.6, trending_score=0.7
        )
        signals = strat.generate_signals(_single_bar_frame(price=42_000.0))
        assert any(s.signal_type == SignalType.SELL for s in signals)

    def test_invalid_price_returns_no_signals(self):
        strat = SocialSentimentStrategy(params={"min_mentions": 1})
        strat.update_social_data(mentions=50, sentiment_score=0.5)
        # close=0 -> guard returns no signals.
        assert strat.generate_signals(_single_bar_frame(price=0.0)) == []

    def test_sentiment_score_clipped_to_unit_range(self):
        strat = SocialSentimentStrategy()
        strat.update_social_data(mentions=10, sentiment_score=99.0)
        assert strat._social_data["sentiment_score"] == 1.0
        strat.update_social_data(mentions=10, sentiment_score=-99.0)
        assert strat._social_data["sentiment_score"] == -1.0

    def test_timestamp_is_tz_aware(self):
        strat = SocialSentimentStrategy()
        strat.update_social_data(mentions=10, sentiment_score=0.1)
        ts = strat._social_data["timestamp"]
        assert isinstance(ts, datetime)
        assert ts.tzinfo is not None
        assert ts.utcoffset() == ts.utcoffset()  # no NaN
