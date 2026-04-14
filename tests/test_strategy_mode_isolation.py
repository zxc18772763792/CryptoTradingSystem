from __future__ import annotations

import asyncio
from types import SimpleNamespace

from core.strategies.strategy_base import StrategyBase
from core.strategies.strategy_manager import StrategyManager
from core.trading.account_manager import account_manager
from core.trading.order_manager import OrderManager, OrderRequest
from core.exchanges.base_exchange import OrderSide, OrderType


class _NoopStrategy(StrategyBase):
    def generate_signals(self, data):
        return []

    def get_required_data(self):
        return {"type": "kline", "columns": ["close"], "min_length": 1}


def test_strategy_manager_keeps_strategy_runtime_mode_isolated(monkeypatch):
    accounts = {
        "main": {
            "account_id": "main",
            "mode": "paper",
            "exchange": "binance",
            "enabled": True,
            "metadata": {},
        }
    }

    def _get_account(account_id):
        row = accounts.get(account_id)
        return dict(row) if row else None

    def _create_account(*, account_id, **payload):
        accounts[account_id] = {"account_id": account_id, **payload}
        return dict(accounts[account_id])

    def _update_account(account_id, updates):
        existing = dict(accounts.get(account_id) or {"account_id": account_id})
        existing.update(updates)
        accounts[account_id] = existing
        return dict(existing)

    def _get_account_mode(account_id, default="paper"):
        row = accounts.get(account_id) or {}
        return str(row.get("mode") or default)

    monkeypatch.setattr(account_manager, "get_account", _get_account)
    monkeypatch.setattr(account_manager, "create_account", _create_account)
    monkeypatch.setattr(account_manager, "update_account", _update_account)
    monkeypatch.setattr(account_manager, "get_account_mode", _get_account_mode)

    manager = StrategyManager()
    assert manager.register_strategy(
        name="paper_alpha",
        strategy_class=_NoopStrategy,
        params={"exchange": "binance"},
        symbols=["BTC/USDT"],
        timeframe="1h",
        metadata={"runtime_mode": "paper"},
    )
    assert manager.register_strategy(
        name="live_beta",
        strategy_class=_NoopStrategy,
        params={"exchange": "binance"},
        symbols=["ETH/USDT"],
        timeframe="1h",
        metadata={"runtime_mode": "live"},
    )

    assert manager.get_strategy_runtime_mode("paper_alpha") == "paper"
    assert manager.get_strategy_runtime_mode("live_beta") == "live"
    assert set(manager.get_all_strategies("paper").keys()) == {"paper_alpha"}
    assert set(manager.get_all_strategies("live").keys()) == {"live_beta"}
    assert accounts[manager.get_strategy_runtime("live_beta")["account_id"]]["mode"] == "live"


def test_order_manager_routes_orders_by_account_mode(monkeypatch):
    manager = OrderManager()
    manager.set_paper_trading(True)
    calls = []

    monkeypatch.setattr(
        account_manager,
        "get_account_mode",
        lambda account_id, default="paper": "live" if account_id == "live_acc" else "paper",
    )

    async def _create_paper_order(request):
        calls.append(("paper", request.account_id))
        return SimpleNamespace(id="paper_order")

    async def _create_real_order(request):
        calls.append(("live", request.account_id))
        return SimpleNamespace(id="live_order")

    monkeypatch.setattr(manager, "_create_paper_order", _create_paper_order)
    monkeypatch.setattr(manager, "_create_real_order", _create_real_order)

    async def _run():
        await manager.create_order(
            OrderRequest(
                symbol="BTC/USDT",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                amount=1.0,
                exchange="binance",
                account_id="live_acc",
            )
        )
        await manager.create_order(
            OrderRequest(
                symbol="BTC/USDT",
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                amount=1.0,
                exchange="binance",
                account_id="paper_acc",
            )
        )

    asyncio.run(_run())

    assert calls == [("live", "live_acc"), ("paper", "paper_acc")]


def test_order_manager_clear_paper_history_preserves_live_orders():
    manager = OrderManager()
    manager._orders = {
        "paper_1": SimpleNamespace(id="paper_1"),
        "live_1": SimpleNamespace(id="live_1"),
    }
    manager._order_meta = {
        "paper_1": {"mode": "paper"},
        "live_1": {"mode": "live"},
    }

    result = manager.clear_paper_history(mode="paper")

    assert result["orders_cleared"] == 1
    assert "paper_1" not in manager._orders
    assert "live_1" in manager._orders
