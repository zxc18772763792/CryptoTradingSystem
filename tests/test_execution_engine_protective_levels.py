import asyncio
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import importlib
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

from core.backtest.exit_engine import resolve_exit_engine_config
from core.trading.execution_engine import ExecutionEngine
from core.trading.order_manager import OrderSide
from core.trading.position_manager import PositionSide, position_manager


@pytest.fixture(autouse=True)
def _clear_positions():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    position_manager.clear_all()
    yield
    position_manager.clear_all()
    loop.close()
    asyncio.set_event_loop(None)


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


def test_background_tick_checks_default_scope_only_when_no_cross_mode_work(monkeypatch):
    engine = ExecutionEngine()
    engine.set_paper_trading(True, sync_runtime_state=False)
    engine._bg_check_interval_seconds = 0
    events = []

    async def fake_reconcile():
        events.append("live:reconcile")

    async def fake_conditional():
        events.append(f"{engine._current_trading_mode()}:conditional")

    async def fake_protective():
        events.append(f"{engine._current_trading_mode()}:protective")

    monkeypatch.setattr(engine, "_reconcile_local_positions_with_exchange", fake_reconcile)
    monkeypatch.setattr(engine, "_check_conditional_orders", fake_conditional)
    monkeypatch.setattr(engine, "_check_protective_orders", fake_protective)
    monkeypatch.setattr(position_manager, "get_all_positions", lambda scope=None: [])

    asyncio.run(engine._background_tick())

    assert events == [
        "paper:conditional",
        "paper:protective",
    ]


def test_background_tick_checks_live_scope_when_live_work_exists(monkeypatch):
    engine = ExecutionEngine()
    engine.set_paper_trading(True, sync_runtime_state=False)
    engine._bg_check_interval_seconds = 0
    engine._conditional_orders["live_cond"] = SimpleNamespace(
        account_id="acct_live",
        trading_mode="live",
    )
    events = []

    async def fake_reconcile():
        events.append("live:reconcile")

    async def fake_conditional():
        events.append(f"{engine._current_trading_mode()}:conditional")

    async def fake_protective():
        events.append(f"{engine._current_trading_mode()}:protective")

    def fake_resolve_mode(account_id, *, metadata=None, fallback=None):
        return "live" if account_id == "acct_live" else (fallback or "paper")

    monkeypatch.setattr(engine, "_resolve_account_trading_mode", fake_resolve_mode)
    monkeypatch.setattr(engine, "_reconcile_local_positions_with_exchange", fake_reconcile)
    monkeypatch.setattr(engine, "_check_conditional_orders", fake_conditional)
    monkeypatch.setattr(engine, "_check_protective_orders", fake_protective)
    monkeypatch.setattr(position_manager, "get_all_positions", lambda scope=None: [])

    asyncio.run(engine._background_tick())

    assert events == [
        "paper:conditional",
        "paper:protective",
        "live:reconcile",
        "live:conditional",
        "live:protective",
    ]


def test_conditional_order_stays_paper_and_freezes_after_account_switches_live(monkeypatch):
    engine = ExecutionEngine()
    engine.set_paper_trading(True, sync_runtime_state=False)
    account_mode = {"main": "paper"}
    current_price = {"value": 100.0}
    notifications = AsyncMock(return_value=None)

    monkeypatch.setattr(
        engine,
        "_resolve_account_trading_mode",
        lambda account_id, *, metadata=None, fallback=None: account_mode.get(account_id, fallback or "paper"),
    )
    monkeypatch.setattr(
        engine,
        "_resolve_price",
        AsyncMock(side_effect=lambda exchange, symbol, fallback=None: current_price["value"]),
    )
    monkeypatch.setattr(engine, "_notify_callbacks", notifications)

    queued = asyncio.run(
        engine.execute_manual_order(
            exchange="binance",
            symbol="BTC/USDT",
            side="buy",
            order_type="market",
            amount=1.0,
            trigger_price=101.0,
            order_mode="conditional",
            account_id="main",
        )
    )

    assert queued is not None
    assert queued["trading_mode"] == "paper"
    conditional = engine._conditional_orders[queued["conditional_id"]]
    assert conditional.trading_mode == "paper"
    with pytest.raises(FrozenInstanceError):
        conditional.trading_mode = "live"

    execute_mock = AsyncMock(return_value={"order_id": "must-not-submit"})
    monkeypatch.setattr(engine, "_execute_manual_order_single", execute_mock)
    account_mode["main"] = "live"
    current_price["value"] = 102.0

    asyncio.run(engine._check_conditional_orders())
    asyncio.run(engine._check_conditional_orders())

    execute_mock.assert_not_awaited()
    listed = engine.list_conditional_orders()
    assert listed == [
        {
            **conditional.to_dict(),
            "current_account_mode": "live",
            "status": "frozen",
            "freeze_reason": "account_trading_mode_changed",
        }
    ]
    frozen_events = [
        call
        for call in notifications.await_args_list
        if call.args and call.args[0] == "conditional_frozen"
    ]
    assert len(frozen_events) == 1
    assert frozen_events[0].args[1]["trading_mode"] == "paper"
    assert frozen_events[0].args[1]["current_account_mode"] == "live"


def test_conditional_background_routing_and_paper_clear_use_stored_mode(monkeypatch):
    engine = ExecutionEngine()
    engine.set_paper_trading(False, sync_runtime_state=False)
    engine._conditional_orders["legacy"] = SimpleNamespace(account_id="main")
    engine._conditional_orders["live"] = SimpleNamespace(account_id="main", trading_mode="live")

    monkeypatch.setattr(
        engine,
        "_resolve_account_trading_mode",
        lambda account_id, *, metadata=None, fallback=None: "live",
    )
    monkeypatch.setattr(position_manager, "get_all_positions", lambda scope=None: [])

    # A legacy object has no proof of live authorization, so it fails safe to
    # paper.  Background routing must use the stored/defaulted order mode, not
    # the account's current live mode.
    assert engine._has_background_work_for_mode("paper", fallback="live") is True
    assert engine._has_background_work_for_mode("live", fallback="live") is True

    cleared = engine.clear_paper_runtime()

    assert cleared["conditional_orders_cleared"] == 1
    assert set(engine._conditional_orders) == {"live"}


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


def test_rebuilds_take_profit_invalidated_by_fill_price():
    engine = ExecutionEngine()
    signal = _make_signal(
        price=100.0,
        stop_loss=98.0,
        take_profit=101.0,
        metadata={"take_profit_pct": 0.06},
    )

    stop_loss, take_profit = engine._ensure_signal_protection_levels(
        signal=signal,
        side=OrderSide.BUY,
        entry_price=102.0,
        trade_policy={"stop_loss_pct": 0.02, "take_profit_pct": 0.04},
    )

    assert stop_loss == pytest.approx(98.0)
    assert take_profit == pytest.approx(108.12)


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


def test_exit_template_disables_default_protection_injection():
    engine = ExecutionEngine()
    signal = _make_signal(price=100.0)

    stop_loss, take_profit = engine._ensure_signal_protection_levels(
        signal=signal,
        side=OrderSide.BUY,
        entry_price=100.0,
        trade_policy={"disable_default_protection_injection": True},
    )

    assert stop_loss is None
    assert take_profit is None


def test_exit_template_runtime_overrides_attach_atr_partial_and_time_stop(monkeypatch):
    engine = ExecutionEngine()
    signal = _make_signal(price=100.0, metadata={"timeframe": "1h"})
    monkeypatch.setattr(engine, "_estimate_exit_template_atr_pct", AsyncMock(return_value=0.01))

    import asyncio

    asyncio.run(
        engine._apply_exit_template_runtime_overrides(
            signal=signal,
            trade_policy={
                "exit_template": "PartialPlusATR",
                "timeframe": "1h",
                "exit_template_config": resolve_exit_engine_config(template_name="PartialPlusATR").to_dict(),
            },
            exchange="binance",
            entry_price=100.0,
        )
    )

    metadata = signal.metadata
    assert metadata["exit_template"] == "PartialPlusATR"
    assert metadata["stop_loss_pct"] == pytest.approx(0.02)
    assert metadata["trailing_stop_pct"] == pytest.approx(0.025)
    assert metadata["profit_protect_lock_pct"] == pytest.approx(0.001)
    assert metadata["partial_take_profit_enabled"] is True
    assert metadata["partial_take_profit_trigger_pct"] == pytest.approx(0.03)
    assert metadata["partial_take_profit_fraction"] == pytest.approx(0.5)
    assert metadata["post_partial_trailing_activation_pct"] == pytest.approx(0.04)
    assert metadata["time_stop_enabled"] is True
    assert metadata["time_stop_minutes"] == 20 * 60
    assert metadata["outage_protection_enabled"] is False
    assert metadata["atr_pct"] == pytest.approx(0.01)
    assert metadata["profit_management_atr_pct"] == pytest.approx(0.01)


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


def test_strategy_positions_receive_default_profit_management_and_time_stop():
    engine = ExecutionEngine()
    position = SimpleNamespace(
        strategy="unit_test_strategy",
        metadata={
            "source": "strategy",
            "timeframe": "5m",
            "profit_management_atr_pct": 0.01,
        },
    )

    metadata = engine._effective_profit_management_metadata(position)

    assert metadata["tier2_managed"] is True
    assert metadata["profit_protect_enabled"] is True
    assert metadata["profit_protect_trigger_pct"] == pytest.approx(0.01)
    assert metadata["profit_protect_lock_pct"] == pytest.approx(0.001)
    assert metadata["partial_take_profit_enabled"] is True
    assert metadata["partial_take_profit_trigger_pct"] == pytest.approx(0.015)
    assert metadata["post_partial_trailing_stop_pct"] == pytest.approx(0.012)
    assert metadata["post_partial_trailing_activation_pct"] == pytest.approx(0.02)
    assert metadata["time_stop_enabled"] is True
    assert metadata["max_bars_in_trade"] == 20
    assert metadata["time_stop_minutes"] == 100
    assert metadata["time_stop_deadline_at"]


def test_manual_positions_do_not_receive_strategy_profit_management_defaults():
    engine = ExecutionEngine()
    position = SimpleNamespace(strategy="manual", metadata={"source": "manual"})

    metadata = engine._effective_profit_management_metadata(position)

    assert "profit_protect_enabled" not in metadata
    assert "time_stop_enabled" not in metadata


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


def test_execution_engine_post_partial_trailing_waits_for_activation_pct():
    engine = ExecutionEngine()
    position_manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=2.0,
        strategy="unit_test_strategy",
        account_id="main",
        metadata={
            "source": "strategy",
            "partial_take_profit_enabled": True,
            "partial_take_profit_trigger_pct": 0.015,
            "partial_take_profit_fraction": 0.5,
            "post_partial_trailing_stop_pct": 0.012,
            "post_partial_trailing_activation_pct": 0.02,
        },
    )

    async def _fake_execute_manual_order_single(**kwargs):
        position_manager.close_position(
            exchange=kwargs["exchange"],
            symbol=kwargs["symbol"],
            close_price=kwargs["price"],
            quantity=kwargs["amount"],
            account_id=kwargs["account_id"],
        )
        return {"order_id": "partial-wait-1", "filled": kwargs["amount"], "price": kwargs["price"]}

    engine._execute_manual_order_single = _fake_execute_manual_order_single

    import asyncio

    engine._resolve_price = AsyncMock(return_value=101.6)
    asyncio.run(engine._check_protective_orders())

    position = position_manager.get_position("binance", "BTC/USDT", account_id="main")
    assert position is not None
    assert position.quantity == pytest.approx(1.0)
    assert position.metadata["partial_take_profit_done"] is True
    assert position.trailing_stop_pct is None

    engine._resolve_price = AsyncMock(return_value=102.1)
    asyncio.run(engine._check_protective_orders())

    position = position_manager.get_position("binance", "BTC/USDT", account_id="main")
    assert position is not None
    assert position.trailing_stop_pct == pytest.approx(0.012)
    assert position.trailing_stop_price is not None


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


def test_execution_engine_time_stop_closes_template_managed_position():
    engine = ExecutionEngine()
    position_manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=1.0,
        strategy="unit_test_strategy",
        account_id="main",
        metadata={
            "source": "strategy",
            "time_stop_enabled": True,
            "time_stop_deadline_at": (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
        },
    )
    engine._resolve_price = AsyncMock(return_value=100.5)
    engine._execute_protective_close = AsyncMock(return_value={"reason": "time_stop"})

    import asyncio

    asyncio.run(engine._check_protective_orders())

    assert engine._execute_protective_close.await_count == 1
    assert engine._execute_protective_close.await_args.args[4] == "time_stop"


def test_protective_orders_use_position_runtime_mode_before_account_mode(monkeypatch):
    execution_engine_module = importlib.import_module("core.trading.execution_engine")
    engine = ExecutionEngine()
    engine.set_paper_trading(False, sync_runtime_state=False)
    position_manager.open_position(
        exchange="binance",
        symbol="NEAR/USDT",
        side=PositionSide.SHORT,
        entry_price=2.017,
        quantity=80.0,
        strategy="bt_ma_near_15m_032320_409",
        account_id="strategy_bt_ma_near_15m_032320_409",
        stop_loss=2.044428571428571,
        take_profit=1.9621428571428572,
        metadata={"source": "strategy", "runtime_mode": "live"},
    )

    monkeypatch.setattr(
        execution_engine_module.account_manager,
        "get_account",
        lambda account_id: {"account_id": account_id} if account_id == "strategy_bt_ma_near_15m_032320_409" else None,
    )
    monkeypatch.setattr(
        execution_engine_module.account_manager,
        "get_account_mode",
        lambda account_id, default="paper": "paper",
    )
    engine._resolve_price = AsyncMock(return_value=1.90001845)
    engine._execute_protective_close = AsyncMock(return_value={"reason": "take_profit"})

    import asyncio

    asyncio.run(engine._check_protective_orders())

    assert engine._execute_protective_close.await_count == 1
    assert engine._execute_protective_close.await_args.args[2] == "strategy_bt_ma_near_15m_032320_409"
    assert engine._execute_protective_close.await_args.args[4] == "take_profit"


def test_execution_engine_partial_take_profit_preserves_take_profit_when_requested():
    engine = ExecutionEngine()
    position_manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=2.0,
        strategy="unit_test_strategy",
        account_id="main",
        take_profit=104.0,
        metadata={
            "source": "strategy",
            "partial_take_profit_enabled": True,
            "partial_take_profit_trigger_pct": 0.006,
            "partial_take_profit_fraction": 0.5,
            "post_partial_trailing_stop_pct": 0.0025,
            "preserve_take_profit_after_partial": True,
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
        return {"order_id": "partial-preserve-1", "filled": kwargs["amount"], "price": kwargs["price"]}

    engine._execute_manual_order_single = _fake_execute_manual_order_single

    import asyncio

    asyncio.run(engine._check_protective_orders())

    position = position_manager.get_position("binance", "BTC/USDT", account_id="main")
    assert position is not None
    assert position.quantity == pytest.approx(1.0)
    assert position.metadata["partial_take_profit_done"] is True
    assert position.take_profit == pytest.approx(104.0)
    assert position.trailing_stop_pct == pytest.approx(0.0025)
