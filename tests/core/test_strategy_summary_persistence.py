from types import SimpleNamespace
from importlib import import_module

from core.strategies.strategy_manager import StrategyConfig, StrategyManager
from core.strategies.persistence import _build_payload


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


def test_dashboard_summary_marks_live_strategy_positions_from_exchange_cache(monkeypatch):
    manager = StrategyManager()
    manager._strategies["live_alpha"] = _DummyStrategy()
    manager._configs["live_alpha"] = StrategyConfig(
        name="live_alpha",
        strategy_class=type("DemoStrategy", (), {}),
        params={},
        symbols=["XRP/USDT"],
        timeframe="5m",
        exchange="binance",
        allocation=0.25,
        metadata={"runtime_mode": "live"},
    )

    local_position = SimpleNamespace(
        symbol="XRP/USDT",
        exchange="binance",
        side=SimpleNamespace(value="short"),
        entry_price=1.3558,
        current_price=1.3558,
        quantity=394.1,
        unrealized_pnl=0.0,
        unrealized_pnl_pct=0.0,
        metadata={},
    )

    def update_price(price):
        local_position.current_price = float(price)
        local_position.unrealized_pnl = (local_position.entry_price - float(price)) * local_position.quantity
        local_position.unrealized_pnl_pct = (local_position.entry_price - float(price)) / local_position.entry_price

    local_position.update_price = update_price

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
        lambda name, scope=None: [local_position] if name == "live_alpha" else [],
        raising=False,
    )
    monkeypatch.setattr(
        position_module.position_manager,
        "get_all_positions",
        lambda scope=None: [
            SimpleNamespace(
                symbol="XRP/USDT:USDT",
                exchange="binance",
                side=SimpleNamespace(value="short"),
                current_price=1.3566,
                unrealized_pnl=-0.31528,
                unrealized_pnl_pct=-0.00059006,
                metadata={"source": "exchange_live"},
            )
        ]
        if scope == "live"
        else [],
        raising=False,
    )
    monkeypatch.setattr(
        execution_module.execution_engine,
        "get_live_trade_review",
        lambda **kwargs: {"items": []},
        raising=False,
    )

    summary = manager.get_dashboard_summary(signal_limit=5)
    performance = summary["strategy_performance"]["live_alpha"]

    assert local_position.current_price == 1.3566
    assert performance["unrealized_pnl"] == -0.3153
    assert performance["return_pct"] < 0


def test_dashboard_summary_counts_runtime_modes(monkeypatch):
    manager = StrategyManager()
    manager._strategies["paper_alpha"] = _DummyStrategy()
    manager._strategies["live_beta"] = _DummyStrategy()
    manager._configs["paper_alpha"] = StrategyConfig(
        name="paper_alpha",
        strategy_class=type("DemoStrategy", (), {}),
        params={},
        symbols=["BTC/USDT"],
        timeframe="1h",
        allocation=0.2,
        metadata={"runtime_mode": "paper"},
    )
    manager._configs["live_beta"] = StrategyConfig(
        name="live_beta",
        strategy_class=type("DemoStrategy", (), {}),
        params={},
        symbols=["ETH/USDT"],
        timeframe="1h",
        allocation=0.2,
        metadata={"runtime_mode": "live"},
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
        lambda name, scope=None: [],
        raising=False,
    )
    monkeypatch.setattr(
        execution_module.execution_engine,
        "get_live_trade_review",
        lambda **kwargs: {"items": []},
        raising=False,
    )

    summary = manager.get_dashboard_summary(signal_limit=5)

    assert summary["registered_by_mode"] == {"paper": 1, "live": 1}
    assert summary["running_by_mode"] == {"paper": 0, "live": 0}


def test_strategy_snapshot_payload_keeps_runtime_mode():
    payload = _build_payload(
        {
            "name": "alpha_strategy",
            "params": {"exchange": "binance"},
            "symbols": ["BTC/USDT"],
            "timeframe": "15m",
            "exchange": "binance",
            "allocation": 0.2,
            "runtime_mode": "live",
            "runtime": {"runtime_limit_minutes": 60, "started_at": None, "runtime_mode": "live"},
            "state": "running",
            "metadata": {"source": "ai_research"},
        }
    )

    assert payload["runtime_mode"] == "live"
    assert payload["metadata"]["runtime_mode"] == "live"
