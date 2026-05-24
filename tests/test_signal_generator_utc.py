from __future__ import annotations

from datetime import datetime, timedelta, timezone

from core.strategies.signal_generator import SignalCombiner, SignalFilter
from core.strategies.strategy_base import Signal, SignalType


def _signal(ts: datetime, strength: float = 0.8) -> Signal:
    return Signal(
        symbol="BTC/USDT",
        signal_type=SignalType.BUY,
        price=100.0,
        timestamp=ts,
        strategy_name="unit_test",
        strength=strength,
    )


def test_signal_filter_stores_utc_aware_timestamps_for_naive_input() -> None:
    signal_filter = SignalFilter(min_strength=0.1, cooldown_minutes=0)
    # Use a fixed naive datetime to exercise the tz-normalization path.
    naive_ts = datetime(2026, 1, 1, 12, 0, 0) - timedelta(minutes=10)

    assert signal_filter.filter(_signal(naive_ts))

    stored = signal_filter._recent_signals["BTC/USDT_buy"][0]
    assert stored.tzinfo is not None
    assert stored.utcoffset() == timedelta(0)


def test_signal_combiner_emits_utc_aware_timestamp() -> None:
    combined = SignalCombiner().combine(
        [
            _signal(datetime(2026, 1, 1, tzinfo=timezone.utc), strength=0.8),
            Signal(
                symbol="BTC/USDT",
                signal_type=SignalType.SELL,
                price=99.0,
                timestamp=datetime(2026, 1, 1, 1, tzinfo=timezone.utc),
                strategy_name="unit_test",
                strength=0.2,
            ),
        ]
    )

    assert combined is not None
    assert combined.timestamp.tzinfo is not None
    assert combined.timestamp.utcoffset() == timedelta(0)
