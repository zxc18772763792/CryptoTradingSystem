from importlib import import_module
from threading import Thread

import pytest

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


def test_risk_manager_record_trade_is_thread_safe(tmp_path, monkeypatch):
    monkeypatch.setattr(risk_module.settings, "CACHE_PATH", tmp_path, raising=False)
    monkeypatch.setattr(risk_module.settings, "TRADING_MODE", "paper", raising=False)

    manager = risk_module.RiskManager(use_persisted_overlay=False)
    manager._trade_history_limit = 1000

    def record_many(start: int) -> None:
        for offset in range(25):
            manager.record_trade(
                {
                    "strategy": "concurrent_strategy",
                    "symbol": "BTC/USDT",
                    "exchange": "binance",
                    "side": "buy",
                    "signal_type": "buy",
                    "fill_price": 68000.0,
                    "quantity": 0.01,
                    "notional": 680.0,
                    "pnl": 1.0,
                    "sequence": start + offset,
                }
            )

    threads = [Thread(target=record_many, args=(idx * 25,)) for idx in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert manager.get_risk_metrics().daily_trades == 100
    assert len(manager.get_trade_history(limit=200)) == 100
    assert manager.get_risk_metrics().daily_pnl_usd == 100.0


def test_risk_manager_record_trade_accepts_explicit_scope(tmp_path, monkeypatch):
    monkeypatch.setattr(risk_module.settings, "CACHE_PATH", tmp_path, raising=False)
    monkeypatch.setattr(risk_module.settings, "TRADING_MODE", "paper", raising=False)

    manager = risk_module.RiskManager(use_persisted_overlay=False)
    manager.record_trade(
        {
            "strategy": "live_alpha",
            "symbol": "ETH/USDT",
            "exchange": "binance",
            "side": "sell",
            "signal_type": "sell",
            "fill_price": 3000.0,
            "quantity": 0.1,
            "notional": 300.0,
            "pnl": 7.0,
        },
        scope="live",
    )

    assert manager.get_account_scope() == "paper"
    assert manager.get_trade_history(limit=10, scope="paper") == []
    live_history = manager.get_trade_history(limit=10, scope="live")
    assert len(live_history) == 1
    assert live_history[0]["mode"] == "live"
    assert manager.get_risk_metrics(scope="paper").daily_trades == 0
    assert manager.get_risk_metrics(scope="live").daily_trades == 1


def test_risk_manager_update_equity_accepts_explicit_scope(tmp_path, monkeypatch):
    monkeypatch.setattr(risk_module.settings, "CACHE_PATH", tmp_path, raising=False)
    monkeypatch.setattr(risk_module.settings, "TRADING_MODE", "paper", raising=False)

    manager = risk_module.RiskManager(use_persisted_overlay=False)
    manager.update_equity(1000.0, scope="paper")
    manager.update_equity(5000.0, scope="live")

    assert manager.get_account_scope() == "paper"
    assert manager.get_risk_report(scope="paper")["scope"] == "paper"
    assert manager.get_risk_report(scope="paper")["equity"]["current"] == 1000.0
    assert manager.get_risk_report(scope="live")["scope"] == "live"
    assert manager.get_risk_report(scope="live")["equity"]["current"] == 5000.0


def test_risk_manager_scoped_equity_alert_text_is_readable(tmp_path, monkeypatch):
    monkeypatch.setattr(risk_module.settings, "CACHE_PATH", tmp_path, raising=False)
    monkeypatch.setattr(risk_module.settings, "TRADING_MODE", "paper", raising=False)

    manager = risk_module.RiskManager(use_persisted_overlay=False)
    manager.balance_volatility_alert_pct = 0.01
    manager.update_equity(1000.0, scope="paper")
    manager.update_equity(1125.0, scope="paper")

    alerts = manager.get_risk_report(scope="paper")["alerts"]
    assert alerts[-1]["title"] == "账户波动预警"
    assert alerts[-1]["message"] == "账户权益短时上升12.50%"
    assert "涓" not in alerts[-1]["message"]


def test_risk_manager_skips_test_stub_trade_history(tmp_path, monkeypatch):
    monkeypatch.setattr(risk_module.settings, "CACHE_PATH", tmp_path, raising=False)
    monkeypatch.setattr(risk_module.settings, "TRADING_MODE", "live", raising=False)

    manager = risk_module.RiskManager(use_persisted_overlay=False)
    manager.record_trade(
        {
            "strategy": "stub",
            "symbol": "BTC/USDT",
            "exchange": "binance",
            "side": "buy",
            "signal_type": "buy",
            "fill_price": 100.02,
            "quantity": 4.5,
            "notional": 450.09,
            "pnl": 349292.56,
        }
    )
    manager.record_trade(
        {
            "strategy": "real_strategy",
            "symbol": "ETH/USDT",
            "exchange": "binance",
            "side": "buy",
            "signal_type": "buy",
            "fill_price": 2000.0,
            "quantity": 0.1,
            "notional": 200.0,
            "pnl": 1.0,
        }
    )

    history = manager.get_trade_history(limit=10)

    assert len(history) == 1
    assert history[0]["strategy"] == "real_strategy"


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
    manager.flush()  # price updates are throttled; force-persist before restoring

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
    manager.flush()

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


def test_position_manager_skips_persisted_test_stub_position(tmp_path, monkeypatch):
    monkeypatch.setattr(position_module.settings, "CACHE_PATH", tmp_path, raising=False)
    monkeypatch.setattr(position_module.settings, "TRADING_MODE", "live", raising=False)

    runtime_state = tmp_path / "runtime_state"
    runtime_state.mkdir(parents=True)
    (runtime_state / "positions_live.json").write_text(
        """
{
  "scope": "live",
  "open_positions": [
    {
      "symbol": "BTC/USDT",
      "exchange": "binance",
      "side": "long",
      "entry_price": 100.02,
      "current_price": 77872.3,
      "quantity": 4.5,
      "value": 350425.35,
      "strategy": "stub",
      "account_id": "acct_A",
      "metadata": {"source": "strategy"}
    },
    {
      "symbol": "ETH/USDT",
      "exchange": "binance",
      "side": "long",
      "entry_price": 2000.0,
      "current_price": 2010.0,
      "quantity": 0.1,
      "value": 201.0,
      "strategy": "real_strategy",
      "account_id": "main",
      "metadata": {"source": "strategy"}
    }
  ],
  "closed_positions": []
}
""".strip(),
        encoding="utf-8",
    )

    restored = position_module.PositionManager()

    assert restored.get_position("binance", "BTC/USDT", account_id="acct_A", strategy="stub") is None
    assert len(restored.get_all_positions()) == 1
    assert restored.get_all_positions()[0].symbol == "ETH/USDT"


def test_position_manager_throttles_open_close_persistence_and_trims_history(tmp_path, monkeypatch):
    monkeypatch.setattr(position_module.settings, "CACHE_PATH", tmp_path, raising=False)
    monkeypatch.setattr(position_module.settings, "TRADING_MODE", "paper", raising=False)
    monkeypatch.setattr(position_module.settings, "POSITION_HISTORY_LIMIT", 100, raising=False)

    manager = position_module.PositionManager()
    writes = []

    def track_persist(*, force=False):
        writes.append(force)
        position_module.PositionManager._persist_scope_state(manager, force=force)

    monkeypatch.setattr(manager, "_persist_scope_state", track_persist)
    manager._persist_throttle_seconds = 60.0

    for idx in range(105):
        symbol = f"COIN{idx}/USDT"
        manager.open_position(
            exchange="binance",
            symbol=symbol,
            side=PositionSide.LONG,
            entry_price=100.0,
            quantity=1.0,
            strategy="trim_strategy",
            account_id="main",
        )
        manager.close_position(
            exchange="binance",
            symbol=symbol,
            close_price=101.0,
            account_id="main",
            strategy="trim_strategy",
        )

    assert writes
    assert all(force is False for force in writes)
    assert len(manager.get_closed_positions()) == 100
    assert manager.get_closed_positions()[0].symbol == "COIN5/USDT"


def test_position_manager_ambiguous_close_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(position_module.settings, "CACHE_PATH", tmp_path, raising=False)
    monkeypatch.setattr(position_module.settings, "TRADING_MODE", "paper", raising=False)

    manager = position_module.PositionManager()
    manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        quantity=1.0,
        strategy="alpha",
        account_id="acct_A",
    )
    manager.open_position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=101.0,
        quantity=1.0,
        strategy="beta",
        account_id="acct_B",
    )

    with pytest.raises(position_module.AmbiguousPositionError):
        manager.close_position(exchange="binance", symbol="BTC/USDT", close_price=102.0)

    assert "Ambiguous position close" in manager.get_last_close_error()
    assert len(manager.get_all_positions()) == 2


def test_position_to_dict_exposes_gross_and_net_realized_pnl():
    position = position_module.Position(
        exchange="binance",
        symbol="BTC/USDT",
        side=PositionSide.LONG,
        entry_price=100.0,
        current_price=110.0,
        quantity=1.0,
        value=110.0,
        realized_pnl=10.0,
        metadata={"fee_usd": 0.4, "slippage_cost_usd": 0.1},
    )

    payload = position.to_dict()

    assert payload["realized_pnl"] == 10.0
    assert payload["gross_realized_pnl"] == 10.0
    assert payload["cost_usd"] == pytest.approx(0.5)
    assert payload["net_realized_pnl"] == pytest.approx(9.5)
