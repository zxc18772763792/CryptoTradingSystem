import asyncio
import importlib

import pytest

module = importlib.import_module("core.trading.execution_engine")
positions = importlib.import_module("core.trading.position_manager")


def test_restarts_do_not_reuse_marked_equity_as_principal_or_forget_fees(monkeypatch):
    monkeypatch.setattr(module.settings, "PAPER_INITIAL_EQUITY", 10000)
    monkeypatch.setattr(module.position_manager, "get_total_realized_pnl", lambda: 100)
    monkeypatch.setattr(module.position_manager, "get_total_pnl", lambda: 20)
    monkeypatch.setattr(module.risk_manager, "get_risk_report", lambda: {"equity": {"current": 10120}})
    monkeypatch.setattr(module.risk_manager, "update_equity", lambda *a, **k: None)
    monkeypatch.setattr(module.runtime_state, "update_equity_snapshot", lambda *a, **k: None)
    monkeypatch.setattr(module.order_manager, "get_order_metadata", lambda _: {"strategy": "agent", "paper_fee_usd": 2, "paper_slippage_cost_usd": 3})
    engine = module.ExecutionEngine()
    engine._consume_paper_order_cost("test-order")
    for _ in range(3):
        engine = module.ExecutionEngine()
        engine._cached_equity = 10120
        monkeypatch.setattr(engine, "_activate_runtime_mode", lambda *a, **k: None)
        engine.set_paper_trading(True, sync_runtime_state=False)
        assert asyncio.run(engine.get_account_equity_snapshot()) == pytest.approx(10118)
        assert engine._consume_paper_order_cost("test-order")["fee_usd"] == 0
        assert engine._paper_fee_ledger["test-order"]["strategy"] == "agent"


def test_partial_and_full_close_are_persisted_without_waiting_for_price_tick(monkeypatch):
    manager = positions.PositionManager()
    manager.set_scope("paper")
    manager._persist_throttle_seconds = 99999
    manager.open_position(exchange="binance", symbol="BTC/USDT", side=positions.PositionSide.LONG,
                          quantity=2, entry_price=100, strategy="agent")
    manager.close_position("binance", "BTC/USDT", 110, quantity=1, strategy="agent")
    restored = positions.PositionManager()
    restored.set_scope("paper")
    assert restored.get_total_realized_pnl() == pytest.approx(10)
    assert restored.get_total_pnl() == pytest.approx(10)
    manager.close_position("binance", "BTC/USDT", 110, strategy="agent")
    restored = positions.PositionManager()
    restored.set_scope("paper")
    assert not restored.get_all_positions()
    assert restored.get_total_realized_pnl() == pytest.approx(20)
