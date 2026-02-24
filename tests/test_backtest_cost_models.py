import asyncio

import numpy as np
import pandas as pd

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
