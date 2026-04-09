from __future__ import annotations

import asyncio
from datetime import datetime
from types import SimpleNamespace

import pandas as pd


def _ohlcv_frame(index: pd.DatetimeIndex, *, start_price: float) -> pd.DataFrame:
    rows = []
    price = float(start_price)
    for _ in index:
        rows.append(
            {
                "open": price,
                "high": price + 8.0,
                "low": price - 8.0,
                "close": price + 2.0,
                "volume": 10.0,
            }
        )
        price += 1.0
    return pd.DataFrame(rows, index=index)


def test_monitor_data_falls_back_to_fresh_ohlcv_and_prefers_executed_trades(monkeypatch):
    from web.api import strategies as strategies_api

    strategy_name = "alpha_live_monitor"

    class DummyStrategy:
        def get_recent_signals(self, limit: int = 200):
            return [
                SimpleNamespace(
                    timestamp=datetime.fromisoformat("2026-03-27T16:10:00"),
                    signal_type="buy",
                    price=68000.0,
                    strength=0.25,
                    stop_loss=67000.0,
                    take_profit=69500.0,
                )
            ]

    async def fake_load_klines_from_parquet(
        *,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time=None,
        end_time=None,
    ):
        if timeframe == "15m":
            return pd.DataFrame()
        if timeframe == "1m":
            idx = pd.date_range("2026-03-27 16:00:00", periods=240, freq="1min")
            return _ohlcv_frame(idx, start_price=68300.0)
        return pd.DataFrame()

    monkeypatch.setattr(strategies_api.strategy_manager, "get_strategy", lambda name: DummyStrategy())
    monkeypatch.setattr(
        strategies_api.strategy_manager,
        "get_strategy_info",
        lambda name: {
            "name": name,
            "symbols": ["BTC/USDT"],
            "timeframe": "15m",
            "state": "running",
            "exchange": "binance",
        },
    )
    monkeypatch.setattr(
        strategies_api.strategy_manager,
        "_configs",
        {strategy_name: SimpleNamespace(allocation=0.15)},
        raising=False,
    )
    monkeypatch.setattr(
        strategies_api.data_storage,
        "load_klines_from_parquet",
        fake_load_klines_from_parquet,
    )
    monkeypatch.setattr(
        strategies_api.execution_engine,
        "get_live_trade_review",
        lambda **kwargs: {
            "count": 1,
            "items": [
                {
                    "timestamp": "2026-03-27T08:16:37.658032+00:00",
                    "strategy": strategy_name,
                    "signal_type": "sell",
                    "fill_price": 68435.2,
                    "pnl": 1.25,
                    "signal": {
                        "strength": 0.92,
                        "stop_loss": 70423.16,
                        "take_profit": 62902.24,
                    },
                }
            ],
        },
    )
    monkeypatch.setattr(
        strategies_api.risk_manager,
        "get_risk_report",
        lambda: {"equity": {"current": 2000.0}},
    )
    monkeypatch.setattr(
        strategies_api.risk_manager,
        "get_trade_history",
        lambda limit=5000: [],
    )
    monkeypatch.setattr(
        strategies_api.position_manager,
        "get_positions_by_strategy",
        lambda name: [],
    )

    payload = asyncio.run(
        strategies_api.get_strategy_monitor_data(strategy_name, bars=120)
    )

    assert payload["name"] == strategy_name
    assert payload["ohlcv_source_timeframe"] == "1m"
    assert len(payload["ohlcv"]) > 0
    assert payload["signals"] == [
        {
            "t": "2026-03-27T08:16:37.658032+00:00",
            "type": "sell",
            "price": 68435.2,
            "strength": 0.92,
            "stop_loss": 70423.16,
            "take_profit": 62902.24,
        }
    ]
    assert len(payload["equity"]) >= 2
    assert payload["equity"][0]["t"] is not None
    assert payload["equity"][-1]["t"] is not None
    assert payload["metrics"]["trade_count"] == 1
    assert payload["metrics"]["realized_pnl"] == 1.25


def test_monitor_data_restores_persisted_trade_markers_and_positions(monkeypatch):
    from web.api import strategies as strategies_api

    strategy_name = "paper_restart_monitor"

    class DummyStrategy:
        def get_recent_signals(self, limit: int = 200):
            return []

    async def fake_load_klines_from_parquet(
        *,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time=None,
        end_time=None,
    ):
        idx = pd.date_range("2026-04-09 08:00:00", periods=120, freq="1h")
        return _ohlcv_frame(idx, start_price=68000.0)

    monkeypatch.setattr(strategies_api.strategy_manager, "get_strategy", lambda name: DummyStrategy())
    monkeypatch.setattr(
        strategies_api.strategy_manager,
        "get_strategy_info",
        lambda name: {
            "name": name,
            "symbols": ["BTC/USDT"],
            "timeframe": "1h",
            "state": "running",
            "exchange": "binance",
        },
    )
    monkeypatch.setattr(
        strategies_api.strategy_manager,
        "_configs",
        {strategy_name: SimpleNamespace(allocation=0.15)},
        raising=False,
    )
    monkeypatch.setattr(
        strategies_api.data_storage,
        "load_klines_from_parquet",
        fake_load_klines_from_parquet,
    )
    monkeypatch.setattr(
        strategies_api.execution_engine,
        "get_live_trade_review",
        lambda **kwargs: {"count": 0, "items": []},
    )
    monkeypatch.setattr(
        strategies_api.risk_manager,
        "get_risk_report",
        lambda: {"equity": {"current": 2000.0}},
    )
    monkeypatch.setattr(
        strategies_api.risk_manager,
        "get_trade_history",
        lambda limit=5000: [
            {
                "timestamp": "2026-04-09T08:16:37.658032+00:00",
                "strategy": strategy_name,
                "signal_type": "buy",
                "side": "buy",
                "fill_price": 68435.2,
                "strength": 1.0,
                "stop_loss": 67000.0,
                "take_profit": 69500.0,
                "pnl": 1.25,
                "notional": 136.87,
            }
        ],
    )
    monkeypatch.setattr(
        strategies_api.position_manager,
        "get_positions_by_strategy",
        lambda name: [
            SimpleNamespace(
                symbol="BTC/USDT",
                side=strategies_api.PositionSide.LONG,
                entry_price=68435.2,
                current_price=68800.0,
                quantity=0.002,
                unrealized_pnl=0.7296,
                unrealized_pnl_pct=0.0053289,
                opened_at=datetime.fromisoformat("2026-04-09T08:16:37"),
            )
        ],
    )

    payload = asyncio.run(
        strategies_api.get_strategy_monitor_data(strategy_name, bars=120)
    )

    assert payload["signals"] == [
        {
            "t": "2026-04-09T08:16:37.658032+00:00",
            "type": "buy",
            "price": 68435.2,
            "strength": 1.0,
            "stop_loss": 67000.0,
            "take_profit": 69500.0,
        }
    ]
    assert payload["metrics"]["trade_count"] == 1
    assert payload["metrics"]["realized_pnl"] == 1.25
    assert payload["metrics"]["unrealized_pnl"] == 0.7296
    assert payload["positions"][0]["entry_time"] == "2026-04-09T08:16:37"


def test_monitor_data_enriches_pairs_strategy_with_dual_leg_series(monkeypatch):
    from web.api import strategies as strategies_api

    strategy_name = "pairs_live_monitor"
    idx = pd.date_range("2026-03-27 00:00:00", periods=180, freq="1h")
    primary_df = _ohlcv_frame(idx, start_price=1800.0)
    pair_df = _ohlcv_frame(idx, start_price=2700.0)
    pair_df["close"] = pair_df["close"] * 1.08 + pd.Series(
        [0.0] * 140 + [-18.0, -15.0, -12.0, -8.0, -5.0, -2.0, 0.0, 1.0, 2.0, 3.0] + [0.0] * 30,
        index=idx,
    ).values

    class DummyStrategy:
        def get_recent_signals(self, limit: int = 200):
            return []

    async def fake_load_klines_from_parquet(
        *,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time=None,
        end_time=None,
    ):
        mapping = {
            "SOL/USDT": primary_df,
            "ETH/USDT": pair_df,
        }
        return mapping.get(str(symbol), pd.DataFrame()).copy()

    monkeypatch.setattr(strategies_api.strategy_manager, "get_strategy", lambda name: DummyStrategy())
    monkeypatch.setattr(
        strategies_api.strategy_manager,
        "get_strategy_info",
        lambda name: {
            "name": name,
            "strategy_type": "PairsTradingStrategy",
            "symbols": ["SOL/USDT"],
            "timeframe": "1h",
            "state": "running",
            "exchange": "binance",
            "params": {
                "pair_symbol": "ETH/USDT",
                "lookback_period": 20,
                "entry_z_score": 2.0,
                "exit_z_score": 0.6,
                "hedge_ratio_method": "ols",
                "allow_negative_hedge_ratio": True,
                "min_hedge_ratio": -5.0,
                "max_hedge_ratio": 5.0,
            },
        },
    )
    monkeypatch.setattr(
        strategies_api.strategy_manager,
        "_configs",
        {strategy_name: SimpleNamespace(allocation=0.15)},
        raising=False,
    )
    monkeypatch.setattr(
        strategies_api.data_storage,
        "load_klines_from_parquet",
        fake_load_klines_from_parquet,
    )
    monkeypatch.setattr(
        strategies_api.execution_engine,
        "get_live_trade_review",
        lambda **kwargs: {"count": 0, "items": []},
    )
    monkeypatch.setattr(
        strategies_api.risk_manager,
        "get_risk_report",
        lambda: {"equity": {"current": 2000.0}},
    )
    monkeypatch.setattr(
        strategies_api.risk_manager,
        "get_trade_history",
        lambda limit=5000: [],
    )
    monkeypatch.setattr(
        strategies_api.position_manager,
        "get_positions_by_strategy",
        lambda name: [],
    )

    payload = asyncio.run(
        strategies_api.get_strategy_monitor_data(strategy_name, bars=120)
    )

    assert payload["portfolio_mode"] == "pairs_spread_dual_leg"
    assert payload["pair_symbol"] == "ETH/USDT"
    assert payload["pair_metrics"]["lookback_period"] == 20
    assert payload["pair_metrics"]["pair_regime"] in {"positive_corr", "negative_corr"}
    assert payload["pair_metrics"]["hedge_ratio_last"] is not None
    assert payload["pair_metrics"]["signal_bias"] in {
        "neutral",
        "long_spread_bias",
        "short_spread_bias",
        "exit_zone",
    }
    assert payload["ohlcv"]
    first = payload["ohlcv"][-1]
    assert "pair_close" in first
    assert "spread" in first
    assert "z_score" in first
    assert "hedge_ratio" in first
