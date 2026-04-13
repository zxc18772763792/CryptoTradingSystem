from importlib import import_module

from core.trading.position_manager import PositionSide


risk_module = import_module("core.risk.risk_manager")
position_module = import_module("core.trading.position_manager")


def test_risk_manager_restores_trade_history_from_persisted_scope(tmp_path, monkeypatch):
    monkeypatch.setattr(risk_module.settings, "CACHE_PATH", tmp_path, raising=False)
    monkeypatch.setattr(risk_module.settings, "TRADING_MODE", "paper", raising=False)

    manager = risk_module.RiskManager(use_persisted_overlay=False)
    manager.record_trade(
        {
            "strategy": "alpha_strategy",
            "symbol": "BTC/USDT",
            "exchange": "binance",
            "side": "buy",
            "signal_type": "buy",
            "fill_price": 68000.0,
            "quantity": 0.01,
            "notional": 680.0,
            "pnl": 3.5,
        }
    )

    restored = risk_module.RiskManager(use_persisted_overlay=False)
    history = restored.get_trade_history(limit=10)

    assert len(history) == 1
    assert history[0]["strategy"] == "alpha_strategy"
    assert history[0]["fill_price"] == 68000.0

    restored.set_account_scope("live")
    assert restored.get_trade_history(limit=10) == []

    restored.set_account_scope("paper")
    assert len(restored.get_trade_history(limit=10)) == 1


def test_position_manager_restores_positions_from_persisted_scope(tmp_path, monkeypatch):
    monkeypatch.setattr(position_module.settings, "CACHE_PATH", tmp_path, raising=False)
    monkeypatch.setattr(position_module.settings, "TRADING_MODE", "paper", raising=False)

    manager = position_module.PositionManager()
    manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=2.0,
        strategy="alpha_strategy",
        account_id="main",
    )
    manager.update_position_price("binance", "BTC/USDT", 105.0, account_id="main", strategy="alpha_strategy")

    restored = position_module.PositionManager()
    position = restored.get_position("binance", "BTC/USDT", account_id="main", strategy="alpha_strategy")

    assert position is not None
    assert position.strategy == "alpha_strategy"
    assert position.unrealized_pnl == 10.0

    restored.set_scope("live")
    assert restored.get_all_positions() == []

    restored.set_scope("paper")
    assert len(restored.get_all_positions()) == 1


def test_position_manager_persists_multiple_strategies_per_symbol(tmp_path, monkeypatch):
    monkeypatch.setattr(position_module.settings, "CACHE_PATH", tmp_path, raising=False)
    monkeypatch.setattr(position_module.settings, "TRADING_MODE", "paper", raising=False)

    manager = position_module.PositionManager()
    manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=1.0,
        strategy="alpha_strategy",
        account_id="main",
    )
    manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.SHORT,
        entry_price=101.0,
        quantity=2.0,
        strategy="beta_strategy",
        account_id="main",
    )

    restored = position_module.PositionManager()
    alpha = restored.get_position("binance", "BTC/USDT", account_id="main", strategy="alpha_strategy")
    beta = restored.get_position("binance", "BTC/USDT", account_id="main", strategy="beta_strategy")
    ambiguous = restored.get_position("binance", "BTC/USDT", account_id="main")

    assert alpha is not None
    assert beta is not None
    assert alpha.strategy == "alpha_strategy"
    assert beta.strategy == "beta_strategy"
    assert ambiguous is None
    assert len(restored.get_positions("binance", "BTC/USDT", account_id="main")) == 2
