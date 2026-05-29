"""Self-check for the core 5m cross-sectional Binance USD-M strategies."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.strategy_registry import get_strategy_defaults
from strategies.quantitative.intraday_cross_section import (
    INTRADAY_CROSS_SECTION_SPECS,
    build_intraday_cross_section_weights,
    build_ohlcv_panels,
)
from web.api.backtest import _run_backtest_core

CORE_INTRADAY_STRATEGIES = [
    "ResidualMom48hStrategy",
    "Ret24hReversalStrategy",
    "RelRet24hReversalStrategy",
    "CloseLocation48hStrategy",
]


def _frames(rows: int = 620, assets: int = 6) -> dict[str, pd.DataFrame]:
    index = pd.date_range("2026-01-01", periods=rows, freq="5min", tz="UTC")
    out: dict[str, pd.DataFrame] = {}
    for i in range(assets):
        symbol = f"CHECK{i + 1}/USDT"
        close = pd.Series(100 + np.linspace(0, i * 8.0, rows) + np.sin(np.arange(rows) / (12 + i)), index=index)
        open_ = close.shift(1).fillna(close.iloc[0])
        high = np.maximum(open_, close) * (1.002 + i * 0.0002)
        low = np.minimum(open_, close) * (0.998 - i * 0.0002)
        out[symbol] = pd.DataFrame(
            {
                "open": open_.to_numpy(),
                "high": high.to_numpy(),
                "low": low.to_numpy(),
                "close": close.to_numpy(),
                "volume": np.full(rows, 10000.0 + i * 1000.0),
                "symbol": [symbol] * rows,
            },
            index=index,
        )
    return out


def main() -> int:
    frames = _frames()
    panels = build_ohlcv_panels(frames)
    symbols = list(frames)
    for class_name in CORE_INTRADAY_STRATEGIES:
        spec = INTRADAY_CROSS_SECTION_SPECS[class_name]
        params = {
            **get_strategy_defaults(class_name),
            "universe_symbols": symbols,
            "long_quantile": 0.34,
            "short_quantile": 0.34,
            "min_universe_size": 4,
            "max_symbol_weight": 0.25,
        }
        weights = build_intraday_cross_section_weights(spec, panels, params=params)
        result = _run_backtest_core(
            strategy=class_name,
            df=frames[symbols[0]],
            timeframe="5m",
            initial_capital=10000.0,
            params=params,
            market_bundle=frames,
            include_series=False,
        )
        latest = weights["rebalance_rows"][-1]
        print(
            f"{class_name} strategy_id={spec.strategy_id} "
            f"long={latest['long_symbols']} short={latest['short_symbols']} "
            f"final_capital={result['final_capital']} trades={result['total_trades']}"
        )
    print(f"core intraday cross-section self-check passed: {len(CORE_INTRADAY_STRATEGIES)} strategies")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
