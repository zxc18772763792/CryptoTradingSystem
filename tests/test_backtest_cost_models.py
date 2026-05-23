import asyncio
from datetime import datetime

import numpy as np
import pandas as pd

from core.backtest.common_pnl import build_common_pnl_summary
from core.backtest.backtest_engine import BacktestConfig, BacktestEngine
from core.backtest.funding_provider import FundingProviderConfig, FundingRateProvider
from core.strategies import Signal, SignalType, StrategyBase
from strategies.quantitative.multi_factor_hf import MultiFactorHFStrategy


def _sample_df(rows: int = 420) -> pd.DataFrame:
    rng = np.random.default_rng(77)
    idx = pd.date_range("2025-01-01", periods=rows, freq="5min")
    close = 50000 + np.cumsum(rng.normal(0, 18, rows) + np.sin(np.arange(rows) / 25.0) * 4)
    close = pd.Series(close, index=idx).abs() + 100
    open_ = close.shift(1).fillna(close.iloc[0])
    high = np.maximum(open_, close) + np.abs(rng.normal(0, 5, rows))
    low = np.minimum(open_, close) - np.abs(rng.normal(0, 5, rows))
    volume = np.abs(rng.normal(1800, 300, rows)) + 50
    funding = np.where((idx.hour % 8 == 0) & (idx.minute == 0), 0.0001, 0.0)
    return pd.DataFrame(
        {
            "open": open_.values,
            "high": high.values,
            "low": low.values,
            "close": close.values,
            "volume": volume,
            "funding_rate": funding,
            "symbol": ["BTC/USDT"] * rows,
        },
        index=idx,
    )


def test_backtest_engine_dynamic_cost_breakdown():
    df = _sample_df()
    strategy = MultiFactorHFStrategy(name="hf_bt_test", params={})
    cfg = BacktestConfig(
        initial_capital=10000,
        position_size_pct=0.1,
        max_positions=1,
        enable_shorting=True,
        leverage=2.0,
        fee_model="maker_taker",
        maker_fee=0.0002,
        taker_fee=0.0005,
        slippage_model="dynamic",
        dynamic_slip={"min_slip": 0.00005, "k_atr": 0.15, "k_rv": 0.8, "k_spread": 0.5},
        include_funding=True,
    )
    engine = BacktestEngine(cfg)
    result = asyncio.run(engine.run_backtest(strategy, df, symbol="BTC/USDT"))

    assert isinstance(result.cost_breakdown, dict)
    for key in ["gross_pnl", "fee", "slippage_cost", "funding_pnl", "realized_total"]:
        assert key in result.cost_breakdown
    assert result.turnover_notional >= 0

    trades = result.trades
    assert isinstance(trades, list)
    if trades:
        t = trades[-1]
        assert hasattr(t, "gross_pnl")
        assert hasattr(t, "fee")
        assert hasattr(t, "slippage_cost")
        assert hasattr(t, "funding_pnl")
        assert hasattr(t, "net_pnl")


def test_backtest_engine_uses_funding_provider_when_column_missing(tmp_path):
    class _AlwaysLongAfterWarmup(StrategyBase):
        def __init__(self):
            super().__init__(name="always_long_test")
            self._entered = False

        def get_required_data(self):
            return {}

        def generate_signals(self, data: pd.DataFrame):
            if len(data) < 3:
                return []
            if not self._entered:
                self._entered = True
                return [
                    Signal(
                        symbol="BTC/USDT",
                        signal_type=SignalType.BUY,
                        price=float(data["close"].iloc[-1]),
                        timestamp=pd.Timestamp(data.index[-1]).to_pydatetime(),
                        strategy_name=self.name,
                        strength=1.0,
                        metadata={"generic_check_exit_enabled": False},
                    )
                ]
            return []

    df = _sample_df().drop(columns=["funding_rate"])
    strategy = _AlwaysLongAfterWarmup()
    provider = FundingRateProvider(FundingProviderConfig(cache_dir=str(tmp_path / "funding")))
    # 8h funding points across sample window; positive rates should create funding cashflows.
    fidx = pd.date_range(df.index.min().floor("8h"), df.index.max().ceil("8h"), freq="8h")
    provider.merge_series("BTC/USDT", pd.Series([0.0001] * len(fidx), index=fidx), save=False)
    cfg = BacktestConfig(
        initial_capital=10000,
        position_size_pct=0.1,
        max_positions=1,
        enable_shorting=True,
        leverage=2.0,
        fee_model="flat",
        commission_rate=0.0005,
        slippage_model="flat",
        slippage=0.0002,
        include_funding=True,
        funding_source="local",
    )
    engine = BacktestEngine(cfg, funding_provider=provider)
    result = asyncio.run(engine.run_backtest(strategy, df, symbol="BTC/USDT"))

    assert isinstance(result.cost_breakdown, dict)
    assert "funding_pnl" in result.cost_breakdown
    # Funding provider path should produce funding entries because strategy holds across boundaries.
    funding_trades = [t for t in result.trades if getattr(t, "trade_stage", "") == "funding"]
    assert len(funding_trades) > 0


def test_backtest_cost_breakdown_counts_funding_once():
    engine = BacktestEngine(
        BacktestConfig(
            initial_capital=10000,
            position_size_pct=0.1,
            max_positions=1,
            leverage=1.0,
            commission_rate=0.0,
            slippage=0.0,
            include_funding=True,
        )
    )
    entry_ts = pd.Timestamp("2026-01-01T00:00:00Z").to_pydatetime()
    exit_ts = pd.Timestamp("2026-01-01T01:00:00Z").to_pydatetime()
    signal = Signal(
        symbol="BTC/USDT",
        signal_type=SignalType.BUY,
        price=100.0,
        timestamp=entry_ts,
        strategy_name="unit_test",
        strength=1.0,
    )

    asyncio.run(engine._execute_buy(signal, current_price=100.0, timestamp=entry_ts, window=None))
    engine._positions["BTC/USDT"]["funding_pnl"] = -10.0
    engine._trades.append(
        engine._trades[-1].__class__(
            timestamp=entry_ts,
            symbol="BTC/USDT",
            side="funding",
            quantity=0.0,
            price=100.0,
            commission=0.0,
            slippage=0.0,
            pnl=-10.0,
            strategy="unit_test",
            gross_pnl=0.0,
            fee=0.0,
            slippage_cost=0.0,
            funding_pnl=-10.0,
            net_pnl=-10.0,
            notional=1000.0,
            execution_role="funding",
            trade_stage="funding",
        )
    )
    asyncio.run(engine._close_position("BTC/USDT", 100.0, exit_ts, "long", None, signal))

    result = engine._calculate_result()

    close_trade = [t for t in result.trades if t.trade_stage == "close"][0]
    assert close_trade.net_pnl == -10.0
    assert result.cost_breakdown["funding_pnl"] == -10.0
    assert result.cost_breakdown["net_pnl"] == -10.0
    assert result.cost_breakdown["realized_total"] == -10.0


def test_common_pnl_summary_schema():
    payload = build_common_pnl_summary(
        source="web_quick_backtest",
        unit="pct_return",
        gross_pnl=12.34567,
        fee=1.23456,
        slippage_cost=None,
        funding_pnl=0.0,
        net_pnl=11.11111,
        turnover=None,
        trade_count=8,
        win_rate=62.5,
        cost_model_version="web_api_backtest_v1",
        metadata={"strategy": "MAStrategy"},
    )

    assert payload["source"] == "web_quick_backtest"
    assert payload["unit"] == "pct_return"
    assert payload["gross_pnl"] == 12.34567
    assert payload["fee"] == 1.23456
    assert payload["slippage_cost"] is None
    assert payload["net_pnl"] == 11.11111
    assert payload["trade_count"] == 8
    assert payload["win_rate"] == 62.5


def test_backtest_engine_leverage_keeps_position_pct_as_notional():
    engine = BacktestEngine(
        BacktestConfig(
            initial_capital=10000,
            position_size_pct=0.1,
            max_positions=1,
            leverage=2.0,
            fee_model="flat",
            commission_rate=0.0,
            slippage_model="flat",
            slippage=0.0,
        )
    )
    signal = Signal(
        symbol="BTC/USDT",
        signal_type=SignalType.BUY,
        price=100.0,
        timestamp=datetime(2025, 1, 1),
        strategy_name="unit_test",
        strength=1.0,
    )

    asyncio.run(engine._execute_buy(signal, current_price=100.0, timestamp=signal.timestamp, window=None))

    position = engine._positions["BTC/USDT"]
    assert position["notional_entry"] == 1000.0
    assert position["margin"] == 500.0
    assert position["quantity"] == 10.0
    assert engine._capital == 9500.0
