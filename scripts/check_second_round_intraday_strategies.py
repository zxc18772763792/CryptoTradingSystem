"""Self-check for the second-round cross-sectional strategy pack."""
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


SECOND_ROUND_STRATEGIES = [
    "ReturnEntropy4hStrategy",
    "FalseBreakoutSupply24hStrategy",
    "RangeAsymmetry48hStrategy",
    "SessionAsiaFlow24hStrategy",
    "SessionFlowRotation24hStrategy",
    "VolumeWeightedReturn24hStrategy",
    "WickImbalance48hStrategy",
    "TurnoverEntropy48hStrategy",
    "BodyVolumeCorr24hStrategy",
    "CorrBreakdown24h72hStrategy",
    "ExtremeRecency48hStrategy",
    "UpDownBetaSpread24h72hStrategy",
    "DirectionalRangeEfficiency48hStrategy",
    "CrossSectionalStress4hStrategy",
    "SignImbalance4hStrategy",
    "VWAPSlope24hStrategy",
    "VWAPGap48hStrategy",
    "RelativeVolShock24hStrategy",
    "LeadMarketResponse24h72hStrategy",
    "BreakCountBalance24hStrategy",
]


def _freq(timeframe: str) -> str:
    return {"5m": "5min", "15m": "15min", "1h": "1h"}.get(str(timeframe), "5min")


def _frames(*, rows: int, assets: int = 8, timeframe: str = "5m") -> dict[str, pd.DataFrame]:
    index = pd.date_range("2026-01-01", periods=rows, freq=_freq(timeframe), tz="UTC")
    t = np.arange(rows, dtype=float)
    out: dict[str, pd.DataFrame] = {}
    for i in range(assets):
        symbol = f"ROUND2{i + 1}/USDT"
        drift = (i - assets / 2) * 0.006
        wave = np.sin(t / (13.0 + i)) * (0.8 + i * 0.05)
        pulse = np.cos(t / (31.0 + i * 2.0)) * (0.4 + i * 0.04)
        close = pd.Series(100.0 + np.cumsum(drift + wave * 0.03 + pulse * 0.02), index=index).abs() + 20.0 + i
        open_ = close.shift(1).fillna(close.iloc[0] * (1.0 - 0.0005 * (i + 1)))
        range_mult = 0.002 + i * 0.00025 + (np.sin(t / 17.0) + 1.0) * 0.00035
        high = np.maximum(open_, close) * (1.0 + range_mult)
        low = np.minimum(open_, close) * (1.0 - range_mult)
        volume = 9000.0 + i * 1300.0 + (np.sin(t / (9.0 + i)) + 1.2) * (700.0 + i * 80.0)
        out[symbol] = pd.DataFrame(
            {
                "open": open_.to_numpy(),
                "high": high.to_numpy(),
                "low": low.to_numpy(),
                "close": close.to_numpy(),
                "volume": volume,
                "symbol": [symbol] * rows,
            },
            index=index,
        )
    return out


def main() -> int:
    for class_name in SECOND_ROUND_STRATEGIES:
        spec = INTRADAY_CROSS_SECTION_SPECS[class_name]
        rows = int(spec.lookback_bars + spec.rebalance_bars + 64)
        frames = _frames(rows=rows, assets=8, timeframe=spec.timeframe)
        panels = build_ohlcv_panels(frames)
        symbols = list(frames)
        params = {
            **get_strategy_defaults(class_name),
            "universe_symbols": symbols,
            "long_quantile": 0.25,
            "short_quantile": 0.25,
            "min_universe_size": 4,
            "max_symbol_weight": 0.25,
        }
        weights = build_intraday_cross_section_weights(spec, panels, params=params)
        result = _run_backtest_core(
            strategy=class_name,
            df=frames[symbols[0]],
            timeframe=spec.timeframe,
            initial_capital=10000.0,
            params=params,
            market_bundle=frames,
            include_series=False,
        )
        if result.get("portfolio_mode") != "intraday_cross_section_long_short":
            raise RuntimeError(f"{class_name} did not use intraday cross-section backtest mode")
        if not weights["rebalance_rows"]:
            raise RuntimeError(f"{class_name} produced no rebalance rows")
        if float(result.get("final_capital") or 0.0) <= 0.0:
            raise RuntimeError(f"{class_name} produced invalid final capital")
        latest = weights["rebalance_rows"][-1]
        print(
            f"{class_name} strategy_id={spec.strategy_id} timeframe={spec.timeframe} "
            f"mode={spec.execution_mode} long={latest['long_symbols']} short={latest['short_symbols']} "
            f"final_capital={result['final_capital']} trades={result['total_trades']}"
        )
    print("second-round intraday cross-section self-check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
