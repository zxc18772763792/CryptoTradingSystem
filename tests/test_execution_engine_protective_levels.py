from datetime import datetime, timezone
import sys
from types import ModuleType, SimpleNamespace

import pytest

from config.settings import settings

if "pandas" not in sys.modules:
    pd_stub = ModuleType("pandas")

    class _DummyDataFrame:
        def __init__(self, *args, **kwargs):
            self.empty = True

        def copy(self):
            return self

    pd_stub.DataFrame = _DummyDataFrame
    pd_stub.Timedelta = lambda *args, **kwargs: 0
    pd_stub.to_datetime = lambda value, *args, **kwargs: value
    pd_stub.concat = lambda *args, **kwargs: _DummyDataFrame()
    sys.modules["pandas"] = pd_stub

from core.trading.execution_engine import ExecutionEngine
from core.trading.order_manager import OrderSide


def _make_signal(
    *,
    price: float = 100.0,
    stop_loss=None,
    take_profit=None,
    metadata=None,
) -> SimpleNamespace:
    return SimpleNamespace(
        symbol="BTC/USDT",
        price=price,
        timestamp=datetime.now(timezone.utc),
        strategy_name="unit_test_strategy",
        stop_loss=stop_loss,
        take_profit=take_profit,
        metadata=dict(metadata or {}),
    )


def test_auto_inject_buy_levels_from_policy_pct():
    engine = ExecutionEngine()
    signal = _make_signal(price=100.0)

    stop_loss, take_profit = engine._ensure_signal_protection_levels(
        signal=signal,
        side=OrderSide.BUY,
        entry_price=100.0,
        trade_policy={"stop_loss_pct": 0.03, "take_profit_pct": 0.06},
    )

    assert stop_loss == pytest.approx(97.0)
    assert take_profit == pytest.approx(106.0)


def test_auto_inject_sell_levels_from_policy_pct():
    engine = ExecutionEngine()
    signal = _make_signal(price=200.0)

    stop_loss, take_profit = engine._ensure_signal_protection_levels(
        signal=signal,
        side=OrderSide.SELL,
        entry_price=200.0,
        trade_policy={"stop_loss_pct": 0.025, "take_profit_pct": 0.05},
    )

    assert stop_loss == pytest.approx(205.0)
    assert take_profit == pytest.approx(190.0)


def test_preserve_existing_valid_protection_levels():
    engine = ExecutionEngine()
    signal = _make_signal(
        price=100.0,
        stop_loss=95.0,
        take_profit=112.0,
    )

    stop_loss, take_profit = engine._ensure_signal_protection_levels(
        signal=signal,
        side=OrderSide.BUY,
        entry_price=100.0,
        trade_policy={"stop_loss_pct": 0.03, "take_profit_pct": 0.06},
    )

    assert stop_loss == pytest.approx(95.0)
    assert take_profit == pytest.approx(112.0)


def test_metadata_pct_overrides_policy_pct():
    engine = ExecutionEngine()
    signal = _make_signal(
        price=100.0,
        metadata={"stop_loss_pct": 0.01, "take_profit_pct": 0.02},
    )

    stop_loss, take_profit = engine._ensure_signal_protection_levels(
        signal=signal,
        side=OrderSide.BUY,
        entry_price=100.0,
        trade_policy={"stop_loss_pct": 0.03, "take_profit_pct": 0.06},
    )

    assert stop_loss == pytest.approx(99.0)
    assert take_profit == pytest.approx(102.0)


def test_fallback_to_global_default_pct(monkeypatch):
    monkeypatch.setattr(settings, "STRATEGY_DEFAULT_STOP_LOSS_PCT", 0.04, raising=False)
    monkeypatch.setattr(settings, "STRATEGY_DEFAULT_TAKE_PROFIT_PCT", 0.08, raising=False)
    engine = ExecutionEngine()
    signal = _make_signal(price=100.0)

    stop_loss, take_profit = engine._ensure_signal_protection_levels(
        signal=signal,
        side=OrderSide.BUY,
        entry_price=100.0,
        trade_policy={},
    )

    assert stop_loss == pytest.approx(96.0)
    assert take_profit == pytest.approx(108.0)
