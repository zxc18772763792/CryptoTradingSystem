"""Synthetic smoke check for every strategy marked backtest-supported."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.strategy_registry import STRATEGY_REGISTRY, get_strategy_defaults, is_strategy_backtest_supported
from strategies.quantitative.intraday_cross_section import INTRADAY_CROSS_SECTION_SPECS
from web.api.backtest import _run_backtest_core


def _freq(timeframe: str) -> str:
    return {"5m": "5min", "15m": "15min", "1h": "1h", "6h": "6h"}.get(str(timeframe), "1h")


def _single_frame(symbol: str, *, rows: int = 1200, timeframe: str = "1h", seed: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed=seed)
    idx = pd.date_range("2026-01-01", periods=rows, freq=_freq(timeframe), tz="UTC")
    t = np.arange(rows, dtype=float)
    close = pd.Series(100.0 + np.cumsum(0.02 + np.sin(t / 17.0) * 0.18 + rng.normal(0, 0.08, rows)), index=idx)
    close = close.abs() + 10.0
    open_ = close.shift(1).fillna(close.iloc[0])
    high = np.maximum(open_, close) * (1.003 + (np.sin(t / 11.0) + 1.0) * 0.0004)
    low = np.minimum(open_, close) * (0.997 - (np.cos(t / 13.0) + 1.0) * 0.0003)
    volume = np.abs(1200.0 + np.sin(t / 9.0) * 160.0 + rng.normal(0, 25, rows)) + 10.0
    return pd.DataFrame(
        {
            "open": open_.to_numpy(),
            "high": high.to_numpy(),
            "low": low.to_numpy(),
            "close": close.to_numpy(),
            "volume": volume,
            "symbol": [symbol] * rows,
        },
        index=idx,
    )


def _bundle(*, rows: int, timeframe: str, assets: int = 8) -> dict[str, pd.DataFrame]:
    return {
        f"SMOKE{i + 1}/USDT": _single_frame(
            f"SMOKE{i + 1}/USDT",
            rows=rows,
            timeframe=timeframe,
            seed=100 + i,
        )
        for i in range(assets)
    }


def _params_for(strategy: str, symbols: list[str]) -> dict:
    params = dict(get_strategy_defaults(strategy) or {})
    if strategy == "FamaFactorArbitrageStrategy":
        params.update(
            {
                "universe_symbols": symbols,
                "min_universe_size": 4,
                "top_n": 2,
                "quantile": 0.25,
                "min_abs_score": 0.0,
            }
        )
    elif strategy in INTRADAY_CROSS_SECTION_SPECS:
        params.update(
            {
                "universe_symbols": symbols,
                "min_universe_size": 4,
                "long_quantile": 0.25,
                "short_quantile": 0.25,
                "max_symbol_weight": 0.25,
            }
        )
    return params


def main() -> int:
    failures: list[str] = []
    checked = 0
    for strategy, meta in STRATEGY_REGISTRY.items():
        if not is_strategy_backtest_supported(strategy):
            continue
        timeframe = str(meta.get("timeframe") or get_strategy_defaults(strategy).get("timeframe") or "1h")
        if strategy in INTRADAY_CROSS_SECTION_SPECS:
            spec = INTRADAY_CROSS_SECTION_SPECS[strategy]
            timeframe = spec.timeframe
            rows = int(spec.lookback_bars + spec.rebalance_bars + 64)
            market_bundle = _bundle(rows=rows, timeframe=timeframe)
            symbols = list(market_bundle)
            df = market_bundle[symbols[0]]
            params = _params_for(strategy, symbols)
        elif strategy == "FamaFactorArbitrageStrategy":
            rows = 900
            market_bundle = _bundle(rows=rows, timeframe=timeframe)
            symbols = list(market_bundle)
            df = market_bundle[symbols[0]]
            params = _params_for(strategy, symbols)
        elif strategy == "PairsTradingStrategy":
            rows = 900
            market_bundle = {
                "BTC/USDT": _single_frame("BTC/USDT", rows=rows, timeframe=timeframe, seed=21),
                "ETH/USDT": _single_frame("ETH/USDT", rows=rows, timeframe=timeframe, seed=22),
            }
            df = market_bundle["BTC/USDT"]
            params = dict(get_strategy_defaults(strategy) or {})
            params["pair_symbol"] = "ETH/USDT"
        else:
            market_bundle = None
            rows = 1200
            df = _single_frame("BTC/USDT", rows=rows, timeframe=timeframe)
            params = _params_for(strategy, ["BTC/USDT"])
        try:
            result = _run_backtest_core(
                strategy=strategy,
                df=df,
                timeframe=timeframe,
                initial_capital=10000.0,
                params=params,
                market_bundle=market_bundle,
                include_series=False,
            )
            final_capital = float(result.get("final_capital") or 0.0)
            if final_capital <= 0.0 or not np.isfinite(final_capital):
                raise RuntimeError(f"invalid final_capital={final_capital}")
            checked += 1
        except Exception as exc:
            failures.append(f"{strategy}: {exc}")

    if failures:
        print("backtest-supported strategy smoke failures:")
        for item in failures:
            print(f"  - {item}")
        return 1
    print(f"backtest-supported strategy smoke passed: {checked} strategies")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
