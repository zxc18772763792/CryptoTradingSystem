from __future__ import annotations

import asyncio
import importlib
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

from core.strategies import Signal, SignalType
from core.trading.execution_engine import ExecutionEngine


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
