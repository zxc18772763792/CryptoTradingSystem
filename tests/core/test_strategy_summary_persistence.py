from types import SimpleNamespace
from importlib import import_module

from core.strategies.strategy_manager import StrategyConfig, StrategyManager


risk_module = import_module("core.risk.risk_manager")
execution_module = import_module("core.trading.execution_engine")
position_module = import_module("core.trading.position_manager")


class _DummyState:
    value = "stopped"


class _DummyStrategy:
    is_running = False
    state = _DummyState()

    def get_info(self):
        return {"name": "alpha_strategy", "state": "stopped"}

    def get_recent_signals(self, limit: int = 10):
        return []


def test_dashboard_summary_uses_live_review_when_runtime_history_is_empty(monkeypatch):
    manager = StrategyManager()
    manager._strategies["alpha_strategy"] = _DummyStrategy()
    manager._configs["alpha_strategy"] = StrategyConfig(
        name="alpha_strategy",
        strategy_class=type("DemoStrategy", (), {}),
        params={},
        symbols=["BTC/USDT"],
        timeframe="1h",
        allocation=0.25,
    )

    monkeypatch.setattr(
        risk_module.risk_manager,
        "get_risk_report",
        lambda: {"equity": {"current": 1000.0}},
        raising=False,
    )
    monkeypatch.setattr(
        risk_module.risk_manager,
        "get_trade_history",
        lambda limit=5000: [],
        raising=False,
    )
    monkeypatch.setattr(
        position_module.position_manager,
        "get_positions_by_strategy",
        lambda name: [SimpleNamespace(unrealized_pnl=12.5)] if name == "alpha_strategy" else [],
        raising=False,
    )
    monkeypatch.setattr(
        execution_module.execution_engine,
        "get_live_trade_review",
        lambda **kwargs: {
            "items": [
                {
                    "strategy": "alpha_strategy",
                    "timestamp": "2026-04-09T08:00:00+00:00",
                    "pnl": 5.0,
                    "notional": 200.0,
                },
                {
                    "strategy": "alpha_strategy",
                    "timestamp": "2026-04-09T09:00:00+00:00",
                    "pnl": -2.0,
                    "notional": 100.0,
                },
            ]
        },
        raising=False,
    )

    summary = manager.get_dashboard_summary(signal_limit=5)
    performance = summary["strategy_performance"]["alpha_strategy"]

    assert performance["trade_count"] == 2
    assert performance["realized_pnl"] == 3.0
    assert performance["unrealized_pnl"] == 12.5
    assert performance["last_update"] == "2026-04-09T09:00:00+00:00"
