from __future__ import annotations

import asyncio
import importlib
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.strategies import Signal, SignalType
from core.trading.execution_engine import ExecutionEngine
from core.trading.position_manager import PositionSide


execution_engine_module = importlib.import_module("core.trading.execution_engine")


def _make_signal(strategy: str = "demo_strategy") -> Signal:
    return Signal(
        symbol="BTC/USDT",
        signal_type=SignalType.BUY,
        price=100.0,
        timestamp=datetime.now(timezone.utc),
        strategy_name=strategy,
        strength=0.8,
        metadata={"account_id": "main", "exchange": "binance"},
    )


def test_record_live_strategy_trade_persists_journal_and_counts(tmp_path: Path, monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = False
    engine._live_review_root = tmp_path
    engine._live_trade_journal_path = tmp_path / "strategy_trade_journal.jsonl"
    engine._live_trade_counts_path = tmp_path / "strategy_trade_counts.json"
    engine._live_strategy_trade_counts = {}
    monkeypatch.setattr(execution_engine_module.audit_logger, "log", AsyncMock(return_value=None))

    signal = _make_signal(strategy="alpha")
    asyncio.run(
        engine._record_live_strategy_trade(
            signal=signal,
            exchange="binance",
            account_id="main",
            side="buy",
            quantity=0.2,
            fill_price=100.0,
            order_id="order-1",
            order_status="filled",
            pnl=1.2,
            fee_usd=0.05,
            slippage_cost_usd=0.01,
            action="open_or_add",
        )
    )
    asyncio.run(
        engine._record_live_strategy_trade(
            signal=signal,
            exchange="binance",
            account_id="main",
            side="buy",
            quantity=0.1,
            fill_price=101.0,
            order_id="order-2",
            order_status="filled",
            pnl=0.5,
            fee_usd=0.03,
            slippage_cost_usd=0.01,
            action="open_or_add",
        )
    )

    assert engine._live_trade_journal_path.exists()
    lines = [json.loads(x) for x in engine._live_trade_journal_path.read_text(encoding="utf-8").splitlines() if x.strip()]
    assert len(lines) == 2
    assert lines[-1]["strategy"] == "alpha"
    assert lines[-1]["strategy_trade_count"] == 2
    assert lines[-1]["signal"]["strategy_name"] == "alpha"

    summary = engine.get_live_trade_review(limit=10, strategy="alpha", hours=24 * 30)
    assert summary["count"] == 2
    assert summary["strategy_trade_counts"]["alpha"] == 2
    assert summary["summary"]["trade_count"] == 2
    assert summary["summary"]["entry_count"] == 2
    assert summary["summary"]["close_count"] == 0
    assert summary["summary"]["gross_pnl_usd"] == 1.78
    assert summary["summary"]["fee_usd"] == 0.08
    assert summary["summary"]["slippage_cost_usd"] == 0.02
    assert summary["summary"]["cost_usd"] == 0.1
    assert summary["summary"]["net_pnl_usd"] == 1.68
    assert summary["summary"]["dominant_symbol"] == "BTC/USDT"


def test_record_live_strategy_trade_skips_when_paper_mode(tmp_path: Path, monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = True
    engine._live_review_root = tmp_path
    engine._live_trade_journal_path = tmp_path / "strategy_trade_journal.jsonl"
    engine._live_trade_counts_path = tmp_path / "strategy_trade_counts.json"
    engine._live_strategy_trade_counts = {}
    monkeypatch.setattr(execution_engine_module.audit_logger, "log", AsyncMock(return_value=None))

    signal = _make_signal(strategy="beta")
    asyncio.run(
        engine._record_live_strategy_trade(
            signal=signal,
            exchange="binance",
            account_id="main",
            side="buy",
            quantity=0.2,
            fill_price=100.0,
            order_id="order-paper",
            order_status="filled",
            pnl=0.0,
            fee_usd=0.0,
            slippage_cost_usd=0.0,
            action="open_or_add",
        )
    )

    assert not engine._live_trade_journal_path.exists()
    assert engine.get_live_trade_review(limit=20)["count"] == 0


class _FakeStrategyPositionManager:
    def __init__(self, positions):
        self.positions = dict(positions)
        self.close_calls = []

    def get_position(self, exchange, symbol, account_id=None, strategy=None):
        if strategy is None:
            matches = [
                pos
                for (pos_exchange, pos_symbol, pos_account_id, _), pos in self.positions.items()
                if pos_exchange == exchange and pos_symbol == symbol and pos_account_id == account_id
            ]
            if len(matches) > 1:
                return matches[0]
            return matches[0] if matches else None
        return self.positions.get((exchange, symbol, account_id, strategy))

    def close_position(self, exchange, symbol, close_price, quantity=None, account_id=None, strategy=None):
        if strategy is None:
            raise AssertionError("close_position must receive a strategy when multiple strategy positions exist")
        key = (exchange, symbol, account_id, strategy)
        position = self.positions.pop(key, None)
        if position is None:
            return None
        self.close_calls.append(
            {
                "exchange": exchange,
                "symbol": symbol,
                "close_price": close_price,
                "quantity": quantity,
                "account_id": account_id,
                "strategy": strategy,
            }
        )
        position.realized_pnl = float(getattr(position, "realized_pnl", 0.0) or 0.0)
        return position

    def open_position(self, *args, **kwargs):
        return None

    def update_position_price(self, *args, **kwargs):
        return None


@pytest.mark.parametrize(
    ("signal_side", "position_side", "signal_type"),
    [
        ("sell", PositionSide.LONG, SignalType.CLOSE_LONG),
        ("buy", PositionSide.SHORT, SignalType.CLOSE_SHORT),
    ],
)
def test_execute_manual_order_single_closes_only_matching_strategy_position(
    signal_side: str,
    position_side: PositionSide,
    signal_type: SignalType,
    monkeypatch,
):
    engine = ExecutionEngine()
    engine._paper_trading = True

    target_strategy = "alpha"
    other_strategy = "beta"
    target_position = SimpleNamespace(
        exchange="binance",
        symbol="BTC/USDT",
        side=position_side,
        quantity=1.0,
        leverage=1.0,
        strategy=target_strategy,
        account_id="main",
        realized_pnl=0.0,
        current_price=100.0,
        entry_price=100.0,
        metadata={"source": "strategy"},
    )
    other_position = SimpleNamespace(
        exchange="binance",
        symbol="BTC/USDT",
        side=position_side,
        quantity=2.0,
        leverage=1.0,
        strategy=other_strategy,
        account_id="main",
        realized_pnl=0.0,
        current_price=100.0,
        entry_price=100.0,
        metadata={"source": "strategy"},
    )
    fake_position_manager = _FakeStrategyPositionManager(
        {
            ("binance", "BTC/USDT", "main", target_strategy): target_position,
            ("binance", "BTC/USDT", "main", other_strategy): other_position,
        }
    )

    monkeypatch.setattr(execution_engine_module, "position_manager", fake_position_manager)
    monkeypatch.setattr(
        execution_engine_module.order_manager,
        "create_order",
        AsyncMock(
            return_value=SimpleNamespace(
                id="order-1",
                price=100.0,
                fee=0.0,
                status=SimpleNamespace(value="filled"),
                amount=1.0,
                filled=1.0,
            )
        ),
    )
    monkeypatch.setattr(execution_engine_module.order_manager, "get_last_error", lambda: "")
    monkeypatch.setattr(execution_engine_module.order_manager, "get_order_metadata", lambda order_id: {})
    monkeypatch.setattr(execution_engine_module.risk_manager, "pre_trade_check", lambda **kwargs: True)
    monkeypatch.setattr(execution_engine_module.risk_manager, "record_trade", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        execution_engine_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(return_value=SimpleNamespace(allowed=True, trace_id="trace-1")),
    )
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(100.0, 100.0)))
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(engine, "_consume_paper_order_cost", lambda order_id: {"fee_usd": 0.0, "slippage_cost_usd": 0.0})
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))

    signal = Signal(
        symbol="BTC/USDT",
        signal_type=signal_type,
        price=100.0,
        timestamp=datetime.now(timezone.utc),
        strategy_name=target_strategy,
        strength=0.8,
        quantity=1.0,
        metadata={"account_id": "main", "exchange": "binance"},
    )

    result = asyncio.run(
        engine._execute_manual_order_single(
            exchange="binance",
            symbol="BTC/USDT",
            side=signal_side,
            order_type="market",
            amount=1.0,
            price=100.0,
            leverage=1.0,
            stop_loss=None,
            take_profit=None,
            trailing_stop_pct=None,
            trailing_stop_distance=None,
            trigger_price=None,
            order_mode="normal",
            iceberg_parts=1,
            algo_slices=1,
            algo_interval_sec=0,
            account_id="main",
            reduce_only=False,
            strategy=target_strategy,
            params={},
        )
    )

    assert result is not None
    assert result["side"] == signal_side
    assert fake_position_manager.close_calls == [
        {
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "close_price": 100.0,
            "quantity": 1.0,
            "account_id": "main",
            "strategy": target_strategy,
        }
    ]
    assert ("binance", "BTC/USDT", "main", other_strategy) in fake_position_manager.positions


def test_execute_manual_order_single_sequential_strategy_reversals_remain_isolated(monkeypatch):
    engine = ExecutionEngine()
    engine._paper_trading = True

    alpha_strategy = "alpha"
    beta_strategy = "beta"
    alpha_short = SimpleNamespace(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.SHORT,
        quantity=1.0,
        leverage=1.0,
        strategy=alpha_strategy,
        account_id="main",
        realized_pnl=0.0,
        current_price=100.0,
        entry_price=100.0,
        metadata={"source": "strategy"},
    )
    beta_long = SimpleNamespace(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        quantity=1.5,
        leverage=1.0,
        strategy=beta_strategy,
        account_id="main",
        realized_pnl=0.0,
        current_price=100.0,
        entry_price=100.0,
        metadata={"source": "strategy"},
    )
    fake_position_manager = _FakeStrategyPositionManager(
        {
            ("binance", "BTC/USDT", "main", alpha_strategy): alpha_short,
            ("binance", "BTC/USDT", "main", beta_strategy): beta_long,
        }
    )

    monkeypatch.setattr(execution_engine_module, "position_manager", fake_position_manager)
    monkeypatch.setattr(
        execution_engine_module.order_manager,
        "create_order",
        AsyncMock(
            side_effect=[
                SimpleNamespace(
                    id="order-alpha",
                    price=100.0,
                    fee=0.0,
                    status=SimpleNamespace(value="filled"),
                    amount=1.0,
                    filled=1.0,
                ),
                SimpleNamespace(
                    id="order-beta",
                    price=100.0,
                    fee=0.0,
                    status=SimpleNamespace(value="filled"),
                    amount=1.5,
                    filled=1.5,
                ),
            ]
        ),
    )
    monkeypatch.setattr(execution_engine_module.order_manager, "get_last_error", lambda: "")
    monkeypatch.setattr(execution_engine_module.order_manager, "get_order_metadata", lambda order_id: {})
    monkeypatch.setattr(execution_engine_module.risk_manager, "pre_trade_check", lambda **kwargs: True)
    monkeypatch.setattr(execution_engine_module.risk_manager, "record_trade", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        execution_engine_module.decision_engine,
        "evaluate_order_intent",
        AsyncMock(
            side_effect=[
                SimpleNamespace(allowed=True, trace_id="trace-alpha"),
                SimpleNamespace(allowed=True, trace_id="trace-beta"),
            ]
        ),
    )
    monkeypatch.setattr(engine, "_resolve_order_context", AsyncMock(return_value=(100.0, 100.0)))
    monkeypatch.setattr(engine, "_get_account_equity", AsyncMock(return_value=1000.0))
    monkeypatch.setattr(engine, "_consume_paper_order_cost", lambda order_id: {"fee_usd": 0.0, "slippage_cost_usd": 0.0})
    monkeypatch.setattr(engine, "_notify_callbacks", AsyncMock(return_value=None))

    alpha_result = asyncio.run(
        engine._execute_manual_order_single(
            exchange="binance",
            symbol="BTC/USDT",
            side="buy",
            order_type="market",
            amount=1.0,
            price=100.0,
            leverage=1.0,
            stop_loss=None,
            take_profit=None,
            trailing_stop_pct=None,
            trailing_stop_distance=None,
            trigger_price=None,
            order_mode="normal",
            iceberg_parts=1,
            algo_slices=1,
            algo_interval_sec=0,
            account_id="main",
            reduce_only=False,
            strategy=alpha_strategy,
            params={},
        )
    )
    beta_result = asyncio.run(
        engine._execute_manual_order_single(
            exchange="binance",
            symbol="BTC/USDT",
            side="sell",
            order_type="market",
            amount=1.5,
            price=100.0,
            leverage=1.0,
            stop_loss=None,
            take_profit=None,
            trailing_stop_pct=None,
            trailing_stop_distance=None,
            trigger_price=None,
            order_mode="normal",
            iceberg_parts=1,
            algo_slices=1,
            algo_interval_sec=0,
            account_id="main",
            reduce_only=False,
            strategy=beta_strategy,
            params={},
        )
    )

    assert alpha_result is not None
    assert beta_result is not None
    assert [item["strategy"] for item in fake_position_manager.close_calls] == [alpha_strategy, beta_strategy]
    assert ("binance", "BTC/USDT", "main", alpha_strategy) not in fake_position_manager.positions
    assert ("binance", "BTC/USDT", "main", beta_strategy) not in fake_position_manager.positions
