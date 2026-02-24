"""Run a reproducible multi-strategy research sweep and emit final-solution artifacts."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data import data_storage  # noqa: E402
from web.api.backtest import _run_backtest_core, get_backtest_strategy_catalog  # noqa: E402


RESAMPLE_RULES = {
    "10s": "10S",
    "30s": "30S",
    "1m": "1T",
    "5m": "5T",
    "15m": "15T",
    "1h": "1H",
    "4h": "4H",
    "1d": "1D",
}


def _parse_symbols(value: str) -> List[str]:
    return [item.strip().upper().replace("_", "/") for item in str(value or "").split(",") if item.strip()]


def _parse_csv(value: str) -> List[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if df.empty:
        return df
    rule = RESAMPLE_RULES.get(str(timeframe))
    if not rule:
        return pd.DataFrame()
    src = df.copy()
    src.index = pd.to_datetime(src.index)
    src = src.sort_index()
    ohlc = src[["open", "high", "low", "close"]].resample(rule).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    )
    volume = src[["volume"]].resample(rule).sum()
    return pd.concat([ohlc, volume], axis=1).dropna(subset=["open", "high", "low", "close"])


def _score(metrics: Dict[str, float]) -> float:
    return (
        float(metrics.get("total_return", 0.0)) * 0.35
        + float(metrics.get("sharpe_ratio", 0.0)) * 18.0
        - float(metrics.get("max_drawdown", 0.0)) * 0.40
        + float(metrics.get("win_rate", 0.0)) * 0.05
        - float(metrics.get("anomaly_bar_ratio", 0.0)) * 200.0
    )


async def _load_symbol_frames(
    exchange: str,
    symbol: str,
    fast_days: int,
    slow_days: int,
    fast_timeframes: List[str],
    slow_timeframes: List[str],
) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}
    now = datetime.utcnow()

    fast_start = now - timedelta(days=max(1, int(fast_days)))
    base_1s = await data_storage.load_klines_from_parquet(
        exchange=exchange,
        symbol=symbol,
        timeframe="1s",
        start_time=fast_start,
        end_time=now,
    )
    if not base_1s.empty:
        base_1s.index = pd.to_datetime(base_1s.index)
        base_1s = base_1s.sort_index()
        for tf in fast_timeframes:
            if tf in RESAMPLE_RULES:
                frames[tf] = _resample_ohlcv(base_1s, tf)

    slow_start = now - timedelta(days=max(1, int(slow_days)))
    slow_1h = await data_storage.load_klines_from_parquet(
        exchange=exchange,
        symbol=symbol,
        timeframe="1h",
        start_time=slow_start,
        end_time=now,
    )
    if not slow_1h.empty:
        slow_1h.index = pd.to_datetime(slow_1h.index)
        slow_1h = slow_1h.sort_index()
        for tf in slow_timeframes:
            if tf == "1h":
                frames[tf] = slow_1h[["open", "high", "low", "close", "volume"]].copy()
            elif tf in RESAMPLE_RULES:
                frames[tf] = _resample_ohlcv(slow_1h, tf)
    elif not base_1s.empty:
        for tf in slow_timeframes:
            if tf in RESAMPLE_RULES:
                frames[tf] = _resample_ohlcv(base_1s, tf)

    # Keep only non-empty frames.
    return {k: v for k, v in frames.items() if not v.empty}


def _robust_candidates(ok_df: pd.DataFrame) -> pd.DataFrame:
    if ok_df.empty:
        return pd.DataFrame()
    agg = ok_df.groupby(["strategy", "timeframe"]).agg(
        n=("symbol", "count"),
        avg_score=("score", "mean"),
        avg_return=("total_return", "mean"),
        avg_sharpe=("sharpe_ratio", "mean"),
        avg_dd=("max_drawdown", "mean"),
        avg_win=("win_rate", "mean"),
        avg_trades=("total_trades", "mean"),
    ).reset_index()
    agg = agg.sort_values(["n", "avg_score", "avg_return"], ascending=[False, False, False])
    return agg


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run final-solution strategy research.")
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--symbols", default="BTC/USDT,ETH/USDT")
    parser.add_argument("--fast-timeframes", default="10s,30s,1m,5m,15m")
    parser.add_argument("--slow-timeframes", default="1h,4h,1d")
    parser.add_argument("--fast-days", type=int, default=30)
    parser.add_argument("--slow-days", type=int, default=365)
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--commission-rate", type=float, default=0.0004)
    parser.add_argument("--slippage-bps", type=float, default=2.0)
    parser.add_argument("--output-dir", default="data/research")
    args = parser.parse_args()

    exchange = str(args.exchange or "binance").strip().lower()
    symbols = _parse_symbols(args.symbols)
    fast_timeframes = [x.lower() for x in _parse_csv(args.fast_timeframes) if x.lower() in RESAMPLE_RULES]
    slow_timeframes = [x.lower() for x in _parse_csv(args.slow_timeframes) if x.lower() in RESAMPLE_RULES]
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    strategy_catalog = get_backtest_strategy_catalog()
    strategies = [x["name"] for x in strategy_catalog if bool(x.get("backtest_supported"))]

    data_rows: Dict[str, Dict[str, int]] = {}
    frames_by_symbol: Dict[str, Dict[str, pd.DataFrame]] = {}
    for symbol in symbols:
        frames = await _load_symbol_frames(
            exchange=exchange,
            symbol=symbol,
            fast_days=max(1, int(args.fast_days)),
            slow_days=max(1, int(args.slow_days)),
            fast_timeframes=fast_timeframes,
            slow_timeframes=slow_timeframes,
        )
        frames_by_symbol[symbol] = frames
        data_rows[symbol] = {tf: int(len(df)) for tf, df in frames.items()}

    rows: List[Dict[str, object]] = []
    for symbol, frames in frames_by_symbol.items():
        for timeframe, df in frames.items():
            for strategy in strategies:
                try:
                    metrics = _run_backtest_core(
                        strategy=strategy,
                        df=df,
                        timeframe=timeframe,
                        initial_capital=max(10.0, float(args.initial_capital)),
                        include_series=False,
                        commission_rate=max(0.0, float(args.commission_rate)),
                        slippage_bps=max(0.0, float(args.slippage_bps)),
                    )
                    payload: Dict[str, object] = {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "strategy": strategy,
                        "rows": int(len(df)),
                        **metrics,
                        "error": "",
                    }
                    payload["score"] = _score(payload)  # type: ignore[arg-type]
                    rows.append(payload)
                except Exception as e:
                    rows.append(
                        {
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "strategy": strategy,
                            "rows": int(len(df)),
                            "error": str(e),
                            "score": -999999,
                        }
                    )

    result_df = pd.DataFrame(rows)
    if "error" not in result_df.columns:
        result_df["error"] = ""
    ok_df = result_df[result_df["error"].fillna("") == ""].copy()
    if not ok_df.empty:
        ok_df = ok_df.sort_values(["score", "total_return", "sharpe_ratio"], ascending=[False, False, False])

    robust_df = _robust_candidates(ok_df)
    if robust_df.empty:
        robust_filtered = pd.DataFrame()
    else:
        robust_filtered = robust_df[
            (robust_df["n"] >= 2)
            & (robust_df["avg_trades"] >= 5)
            & (robust_df["avg_sharpe"] > 0)
            & (robust_df["avg_dd"] < 20)
        ].copy()
        robust_filtered = robust_filtered.sort_values(["avg_score", "avg_return"], ascending=[False, False])

    raw_path = output_dir / f"final_solution_research_raw_{run_ts}.csv"
    robust_path = output_dir / f"final_solution_research_robust_{run_ts}.csv"
    summary_path = output_dir / f"final_solution_summary_{run_ts}.json"

    result_df.to_csv(raw_path, index=False, encoding="utf-8-sig")
    robust_df.to_csv(robust_path, index=False, encoding="utf-8-sig")

    summary = {
        "timestamp": run_ts,
        "exchange": exchange,
        "symbols": symbols,
        "strategies_tested": len(strategies),
        "fast_timeframes": fast_timeframes,
        "slow_timeframes": slow_timeframes,
        "fast_days": int(args.fast_days),
        "slow_days": int(args.slow_days),
        "base_cost": {
            "commission_rate": max(0.0, float(args.commission_rate)),
            "slippage_bps": max(0.0, float(args.slippage_bps)),
        },
        "data_rows": data_rows,
        "runs_total": int(len(result_df)),
        "runs_ok": int(len(ok_df)),
        "top_base": ok_df.head(25).to_dict("records") if not ok_df.empty else [],
        "robust_candidates": robust_df.head(20).to_dict("records") if not robust_df.empty else [],
        "robust_filtered": robust_filtered.head(20).to_dict("records") if not robust_filtered.empty else [],
        "raw_csv": str(raw_path),
        "robust_csv": str(robust_path),
    }
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "summary_json": str(summary_path),
                "raw_csv": str(raw_path),
                "robust_csv": str(robust_path),
                "runs_ok": int(len(ok_df)),
                "top1": (
                    ok_df.iloc[0][
                        ["symbol", "timeframe", "strategy", "total_return", "sharpe_ratio", "max_drawdown", "score"]
                    ].to_dict()
                    if len(ok_df)
                    else None
                ),
                "robust_filtered_count": int(len(robust_filtered)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
