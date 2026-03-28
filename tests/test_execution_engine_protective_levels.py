from datetime import datetime, timezone
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from config.settings import settings

try:
    import pandas  # noqa: F401
except ImportError:
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
from core.trading.position_manager import PositionSide, position_manager


@pytest.fixture(autouse=True)
def _clear_positions():
    position_manager.clear_all()
    yield
    position_manager.clear_all()


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


def test_execution_engine_profit_protect_raises_stop_loss_for_long():
    engine = ExecutionEngine()
    position_manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=2.0,
        strategy="AI_AutonomousAgent",
        account_id="main",
        metadata={
            "source": "ai_autonomous_agent",
            "profit_protect_enabled": True,
            "profit_protect_trigger_pct": 0.0035,
            "profit_protect_lock_pct": 0.0012,
        },
    )
    engine._resolve_price = AsyncMock(return_value=100.7)

    import asyncio

    asyncio.run(engine._check_protective_orders())

    position = position_manager.get_position("binance", "BTC/USDT", account_id="main")
    assert position is not None
    assert position.stop_loss == pytest.approx(100.12)


def test_execution_engine_partial_take_profit_reduces_position_and_arms_trailing():
    engine = ExecutionEngine()
    position_manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=2.0,
        strategy="AI_AutonomousAgent",
        account_id="main",
        metadata={
            "source": "ai_autonomous_agent",
            "profit_protect_enabled": True,
            "profit_protect_trigger_pct": 0.0035,
            "profit_protect_lock_pct": 0.0012,
            "partial_take_profit_enabled": True,
            "partial_take_profit_trigger_pct": 0.006,
            "partial_take_profit_fraction": 0.5,
            "post_partial_trailing_stop_pct": 0.0025,
        },
    )
    engine._resolve_price = AsyncMock(return_value=101.0)

    async def _fake_execute_manual_order_single(**kwargs):
        position_manager.close_position(
            exchange=kwargs["exchange"],
            symbol=kwargs["symbol"],
            close_price=kwargs["price"],
            quantity=kwargs["amount"],
            account_id=kwargs["account_id"],
        )
        return {"order_id": "partial-1", "filled": kwargs["amount"], "price": kwargs["price"]}

    engine._execute_manual_order_single = _fake_execute_manual_order_single

    import asyncio

    asyncio.run(engine._check_protective_orders())

    position = position_manager.get_position("binance", "BTC/USDT", account_id="main")
    assert position is not None
    assert position.quantity == pytest.approx(1.0)
    assert position.metadata["partial_take_profit_done"] is True
    assert position.trailing_stop_pct == pytest.approx(0.0025)
    assert position.trailing_stop_price is not None
    assert position.take_profit is None
    assert position.stop_loss == pytest.approx(100.12)


def test_execution_engine_partial_take_profit_skips_when_below_min_notional():
    engine = ExecutionEngine()
    position_manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=1.2,
        strategy="AI_AutonomousAgent",
        account_id="main",
        metadata={
            "source": "ai_autonomous_agent",
            "profit_protect_enabled": True,
            "profit_protect_trigger_pct": 0.0035,
            "profit_protect_lock_pct": 0.0012,
            "partial_take_profit_enabled": True,
            "partial_take_profit_trigger_pct": 0.006,
            "partial_take_profit_fraction": 0.5,
            "post_partial_trailing_stop_pct": 0.0025,
        },
    )
    engine._resolve_price = AsyncMock(return_value=101.0)
    execute_mock = AsyncMock(return_value={"order_id": "should-not-run"})
    engine._execute_manual_order_single = execute_mock

    import asyncio

    asyncio.run(engine._check_protective_orders())

    position = position_manager.get_position("binance", "BTC/USDT", account_id="main")
    assert position is not None
    assert position.quantity == pytest.approx(1.2)
    assert position.stop_loss == pytest.approx(100.12)
    assert position.metadata["partial_take_profit_skip_reason"] == "partial_notional_below_min"
    assert execute_mock.await_count == 0
