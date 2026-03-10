"""Run comprehensive top-100 universe research and output top-3 strategy configs."""
from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.data import data_storage  # noqa: E402
from core.data.factor_library import build_factor_library  # noqa: E402
from web.api.backtest import _run_backtest_core, get_backtest_strategy_catalog  # noqa: E402


RESAMPLE_RULES = {
    "1m": "1T",
    "3m": "3T",
    "5m": "5T",
    "15m": "15T",
    "30m": "30T",
    "1h": "1H",
    "2h": "2H",
    "4h": "4H",
    "6h": "6H",
    "12h": "12H",
    "1d": "1D",
}


@dataclass
class StrategyAggregate:
    strategy: str
    timeframe: str
    score: float
    n_symbols: int
    avg_total_return: float
    median_total_return: float
    avg_sharpe: float
    median_sharpe: float
    avg_max_drawdown: float
    avg_win_rate: float
    positive_ratio: float
    avg_total_trades: float
    top_symbols: List[str]
    details: List[Dict[str, Any]]


def _parse_csv(value: str) -> List[str]:
    return [x.strip() for x in str(value or "").split(",") if x.strip()]


def _normalize_symbol(value: str) -> str:
    text = str(value or "").strip().upper().replace("_", "/")
    if not text:
        return text
    if "/" not in text and text.endswith("USDT"):
        text = f"{text[:-4]}/USDT"
    return text


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        out = float(v)
    except Exception:
        return default
    if np.isnan(out) or np.isinf(out):
        return default
    return out


def _resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if df.empty:
        return df
    rule = RESAMPLE_RULES.get(str(timeframe).lower())
    if not rule:
        return pd.DataFrame()

    src = df.copy()
    src.index = pd.to_datetime(src.index)
    src = src.sort_index()
    ohlc = src[["open", "high", "low", "close"]].resample(rule).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
        }
    )
    volume = src[["volume"]].resample(rule).sum()
    out = pd.concat([ohlc, volume], axis=1).dropna(subset=["open", "high", "low", "close"])
    return out


def _timeframe_to_seconds(timeframe: str) -> int:
    tf = str(timeframe or "1h").strip().lower()
    if not tf:
        return 3600
    unit = tf[-1]
    try:
        value = int(tf[:-1])
    except Exception:
        return 3600
    value = max(1, value)
    if unit == "s":
        return value
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 3600
    if unit == "d":
        return value * 86400
    return 3600


def _score_row(
    total_return: float,
    sharpe: float,
    max_drawdown: float,
    win_rate: float,
    positive_ratio: float,
) -> float:
    return (
        total_return * 0.40
        + sharpe * 15.0
        - max_drawdown * 0.35
        + positive_ratio * 20.0
        + win_rate * 0.03
    )


def _aggregate_strategy(
    strategy: str,
    timeframe: str,
    symbol_rows: List[Dict[str, Any]],
) -> Optional[StrategyAggregate]:
    if not symbol_rows:
        return None

    df = pd.DataFrame(symbol_rows).copy()
    if df.empty:
        return None

    avg_total_return = _safe_float(df["total_return"].mean())
    median_total_return = _safe_float(df["total_return"].median())
    avg_sharpe = _safe_float(df["sharpe_ratio"].mean())
    median_sharpe = _safe_float(df["sharpe_ratio"].median())
    avg_max_drawdown = _safe_float(df["max_drawdown"].mean())
    avg_win_rate = _safe_float(df["win_rate"].mean())
    positive_ratio = _safe_float((df["total_return"] > 0).mean() * 100.0)
    avg_total_trades = _safe_float(df["total_trades"].mean())

    score = _score_row(
        total_return=median_total_return,
        sharpe=median_sharpe,
        max_drawdown=avg_max_drawdown,
        win_rate=avg_win_rate,
        positive_ratio=positive_ratio,
    )

    top_symbols = (
        df.sort_values(["total_return", "sharpe_ratio"], ascending=[False, False])["symbol"]
        .head(12)
        .astype(str)
        .tolist()
    )

    return StrategyAggregate(
        strategy=strategy,
        timeframe=timeframe,
        score=float(score),
        n_symbols=int(len(df)),
        avg_total_return=float(avg_total_return),
        median_total_return=float(median_total_return),
        avg_sharpe=float(avg_sharpe),
        median_sharpe=float(median_sharpe),
        avg_max_drawdown=float(avg_max_drawdown),
        avg_win_rate=float(avg_win_rate),
        positive_ratio=float(positive_ratio),
        avg_total_trades=float(avg_total_trades),
        top_symbols=top_symbols,
        details=df.sort_values(["total_return", "sharpe_ratio"], ascending=[False, False]).to_dict("records"),
    )


def _fama_metrics(
    close_df: pd.DataFrame,
    volume_df: pd.DataFrame,
    factor_timeframe: str,
    lookback_bars: int,
    rebalance_bars: int,
    quantile: float,
    top_n: int,
    fee_rate: float,
    slippage_bps: float,
    min_abs_score: float = 0.0,
) -> Dict[str, Any]:
    close_df = close_df.sort_index().ffill()
    volume_df = volume_df.sort_index().fillna(0.0)

    symbols = [c for c in close_df.columns if c in volume_df.columns]
    close_df = close_df[symbols]
    volume_df = volume_df[symbols]
    if close_df.empty or len(close_df) < lookback_bars + rebalance_bars + 2:
        return {}

    period_returns: List[float] = []
    period_meta: List[Dict[str, Any]] = []
    prev_long: set[str] = set()
    prev_short: set[str] = set()

    step = max(1, int(rebalance_bars))
    lookback = max(120, int(lookback_bars))
    q = max(0.05, min(0.45, float(quantile)))
    n = max(1, int(top_n))

    for t in range(lookback, len(close_df) - 1, step):
        end_idx = min(t + step, len(close_df) - 1)
        hist_close = close_df.iloc[t - lookback : t]
        hist_vol = volume_df.iloc[t - lookback : t]

        factor_result = build_factor_library(close_df=hist_close, volume_df=hist_vol, quantile=q)
        scores = factor_result.asset_scores.copy()
        if scores.empty:
            continue

        scores["symbol"] = scores["symbol"].astype(str).str.upper()
        scores = scores.dropna(subset=["score"]).sort_values("score", ascending=False)
        score_floor = max(0.0, float(min_abs_score or 0.0))
        if score_floor > 0:
            scores = scores[scores["score"].abs() >= score_floor]
        if len(scores) < max(4, 2 * n):
            continue

        long_list = scores.head(n)["symbol"].astype(str).tolist()
        short_list = scores.tail(n)["symbol"].astype(str).tolist()
        long_set = set(long_list)
        short_set = set(short_list)

        p0 = close_df.iloc[t]
        p1 = close_df.iloc[end_idx]
        ret = (p1 / p0.replace(0, np.nan) - 1.0).replace([np.inf, -np.inf], np.nan)

        long_ret = _safe_float(ret.loc[[x for x in long_list if x in ret.index]].mean())
        short_ret = _safe_float(ret.loc[[x for x in short_list if x in ret.index]].mean())

        turnover_long = 1.0 if not prev_long else 1.0 - (len(prev_long & long_set) / max(1, len(long_set)))
        turnover_short = 1.0 if not prev_short else 1.0 - (len(prev_short & short_set) / max(1, len(short_set)))
        one_way_cost = max(0.0, float(fee_rate)) + max(0.0, float(slippage_bps)) / 10000.0
        cost = one_way_cost * (turnover_long + turnover_short)

        period_ret = (long_ret - short_ret) - cost
        period_returns.append(period_ret)
        period_meta.append(
            {
                "timestamp": pd.Timestamp(close_df.index[end_idx]).isoformat(),
                "long_ret": long_ret,
                "short_ret": short_ret,
                "net_ret": period_ret,
                "cost": cost,
                "long_size": len(long_set),
                "short_size": len(short_set),
            }
        )

        prev_long = long_set
        prev_short = short_set

    if not period_returns:
        return {}

    series = pd.Series(period_returns, dtype=float)
    equity = (1.0 + series).cumprod()
    peak = equity.cummax()
    dd = (equity / peak - 1.0).fillna(0.0)

    total_return = (float(equity.iloc[-1]) - 1.0) * 100.0
    max_drawdown = abs(float(dd.min()) * 100.0)
    mean_ret = float(series.mean())
    std_ret = float(series.std(ddof=0))
    tf_seconds = max(1, _timeframe_to_seconds(factor_timeframe))
    ann_factor = np.sqrt((365.0 * 24.0 * 3600.0) / float(tf_seconds * max(1, step)))
    sharpe = (mean_ret / std_ret * ann_factor) if std_ret > 1e-12 else 0.0
    win_rate = float((series > 0).mean() * 100.0)
    positive_ratio = float((series > 0).mean() * 100.0)

    score = _score_row(
        total_return=total_return,
        sharpe=sharpe,
        max_drawdown=max_drawdown,
        win_rate=win_rate,
        positive_ratio=positive_ratio,
    )

    return {
        "strategy": "FamaFactorArbitrageStrategy",
        "timeframe": str(factor_timeframe).lower(),
        "score": float(score),
        "n_symbols": int(close_df.shape[1]),
        "avg_total_return": float(total_return),
        "median_total_return": float(total_return),
        "avg_sharpe": float(sharpe),
        "median_sharpe": float(sharpe),
        "avg_max_drawdown": float(max_drawdown),
        "avg_win_rate": float(win_rate),
        "positive_ratio": float(positive_ratio),
        "avg_total_trades": float(len(series)),
        "top_symbols": scores.head(max(12, n))["symbol"].astype(str).tolist() if "scores" in locals() else [],
        "details": period_meta[-200:],
        "config_hint": {
            "lookback_bars": lookback,
            "rebalance_interval_minutes": int(step * tf_seconds / 60),
            "quantile": q,
            "top_n": n,
            "min_abs_score": max(0.0, float(min_abs_score or 0.0)),
            "alpha_threshold": max(0.0, float(min_abs_score or 0.0)),
        },
    }


def _build_top3_params(
    strategy_name: str,
    row: Dict[str, Any],
    alpha_threshold: Optional[float],
    cooldown_min: Optional[int],
    max_vol: Optional[float],
    max_spread: Optional[float],
) -> Dict[str, Any]:
    params: Dict[str, Any] = dict(row.get("config_hint", {}) or {})
    strategy = str(strategy_name or "").strip()

    alpha = None if alpha_threshold is None else max(0.0, float(alpha_threshold))
    cooldown = None if cooldown_min is None else max(0, int(cooldown_min))
    vol_limit = None if max_vol is None else max(0.0, float(max_vol))
    spread_limit = None if max_spread is None else max(0.0, float(max_spread))

    if strategy == "FamaFactorArbitrageStrategy":
        if alpha is not None:
            params["alpha_threshold"] = alpha
            params["min_abs_score"] = alpha
        if cooldown is not None:
            params["cooldown_min"] = max(1, cooldown)
            params["rebalance_interval_minutes"] = max(1, cooldown)
        if vol_limit is not None:
            params["max_vol"] = vol_limit
        if spread_limit is not None:
            params["max_spread"] = spread_limit
        return params

    if strategy == "CEXArbitrageStrategy":
        if alpha is not None:
            params["alpha_threshold"] = alpha
            params["min_spread"] = alpha
        if cooldown is not None:
            params["cooldown_min"] = cooldown
        if vol_limit is not None:
            params["max_vol"] = vol_limit
        if spread_limit is not None:
            params["max_spread"] = spread_limit
        return params

    if strategy == "TriangularArbitrageStrategy":
        if alpha is not None:
            params["alpha_threshold"] = alpha
            params["min_profit"] = alpha
        if cooldown is not None:
            params["cooldown_min"] = cooldown
        if spread_limit is not None:
            params["max_spread"] = spread_limit
        return params

    return params


async def _load_base_frames(
    exchange: str,
    symbols: List[str],
    base_timeframe: str,
    min_rows: int,
) -> Dict[str, pd.DataFrame]:
    async def _load_one(sym: str) -> Tuple[str, pd.DataFrame]:
        df = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=sym,
            timeframe=base_timeframe,
        )
        if df.empty:
            return sym, pd.DataFrame()
        out = df.copy()
        out.index = pd.to_datetime(out.index)
        out = out.sort_index()
        out = out[["open", "high", "low", "close", "volume"]].dropna(subset=["close"])
        if len(out) < min_rows:
            return sym, pd.DataFrame()
        return sym, out

    pairs = await asyncio.gather(*[_load_one(sym) for sym in symbols])
    return {sym: df for sym, df in pairs if not df.empty}


def _prepare_frames_by_tf(
    symbol_base: Dict[str, pd.DataFrame],
    base_timeframe: str,
    timeframes: List[str],
    min_rows: int,
) -> Dict[str, Dict[str, pd.DataFrame]]:
    out: Dict[str, Dict[str, pd.DataFrame]] = {}
    base_tf = str(base_timeframe).lower()
    for tf in timeframes:
        tf_key = str(tf).lower()
        out[tf_key] = {}
        for sym, df in symbol_base.items():
            if tf_key == base_tf:
                frame = df.copy()
            else:
                frame = _resample_ohlcv(df, tf_key)
            if len(frame) >= min_rows:
                out[tf_key][sym] = frame
    return out


async def main() -> None:
    parser = argparse.ArgumentParser(description="Top-100 universe comprehensive strategy research")
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--universe-json", default="data/research/top100_universe_latest.json")
    parser.add_argument("--base-timeframe", default="1h")
    parser.add_argument("--timeframes", default="1h,4h")
    parser.add_argument("--strategies", default="", help="Comma-separated strategy names; empty means all backtest-supported.")
    parser.add_argument("--max-symbols", type=int, default=100)
    parser.add_argument("--min-base-rows", type=int, default=720)
    parser.add_argument("--min-rows", type=int, default=720)
    parser.add_argument("--max-bars-per-symbol", type=int, default=0, help="Use only latest N bars per symbol per timeframe; 0 means full.")
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--commission-rate", type=float, default=0.0004)
    parser.add_argument("--slippage-bps", type=float, default=2.0)
    parser.add_argument("--fama-lookback-bars", type=int, default=720)
    parser.add_argument("--fama-rebalance-bars", type=int, default=24)
    parser.add_argument("--fama-quantile", type=float, default=0.25)
    parser.add_argument("--fama-top-n", type=int, default=8)
    parser.add_argument(
        "--alpha-threshold",
        type=float,
        default=None,
        help="Optional common alpha threshold alias (mapped by strategy type).",
    )
    parser.add_argument(
        "--cooldown-min",
        type=int,
        default=None,
        help="Optional common cooldown minutes (or rebalance interval for Fama).",
    )
    parser.add_argument(
        "--max-vol",
        type=float,
        default=None,
        help="Optional volatility cap for supported strategies.",
    )
    parser.add_argument(
        "--max-spread",
        type=float,
        default=None,
        help="Optional spread cap for supported strategies.",
    )
    parser.add_argument("--disable-fama", action="store_true")
    parser.add_argument("--output-json", default="data/research/top100_comprehensive_research_latest.json")
    args = parser.parse_args()

    universe_path = Path(args.universe_json)
    if not universe_path.exists():
        raise FileNotFoundError(f"universe json not found: {universe_path}")
    payload = json.loads(universe_path.read_text(encoding="utf-8"))
    selected = payload.get("selected_universe") or []
    symbols = [_normalize_symbol(x.get("pair") or x.get("symbol") or "") for x in selected]
    symbols = [x for x in symbols if x]
    symbols = symbols[: max(10, int(args.max_symbols))]

    exchange = str(args.exchange or "binance").strip().lower()
    base_timeframe = str(args.base_timeframe or "1h").strip().lower()
    if base_timeframe not in RESAMPLE_RULES:
        raise ValueError(f"unsupported base timeframe: {base_timeframe}")
    timeframes = [x.lower() for x in _parse_csv(args.timeframes) if x.lower() in RESAMPLE_RULES]
    if base_timeframe not in timeframes:
        timeframes.insert(0, base_timeframe)

    symbol_base = await _load_base_frames(
        exchange=exchange,
        symbols=symbols,
        base_timeframe=base_timeframe,
        min_rows=max(120, int(args.min_base_rows)),
    )
    frames_by_tf = _prepare_frames_by_tf(
        symbol_base=symbol_base,
        base_timeframe=base_timeframe,
        timeframes=timeframes,
        min_rows=max(120, int(args.min_rows)),
    )

    strategy_catalog = [
        x
        for x in get_backtest_strategy_catalog()
        if bool(x.get("backtest_supported"))
    ]
    strategy_names = [str(x.get("name")) for x in strategy_catalog if str(x.get("name") or "").strip()]
    user_strategies = [x for x in _parse_csv(args.strategies) if x]
    if user_strategies:
        allowed = {x.strip() for x in user_strategies}
        strategy_names = [x for x in strategy_names if x in allowed]
    if not strategy_names:
        raise ValueError("no strategy selected after filtering")

    aggregates: List[Dict[str, Any]] = []
    research_rows: List[Dict[str, Any]] = []
    max_bars = max(0, int(args.max_bars_per_symbol))
    total_runs = int(sum(len(frames_by_tf.get(tf, {})) for tf in timeframes) * len(strategy_names))
    run_idx = 0

    for strategy in strategy_names:
        for tf in timeframes:
            frames = frames_by_tf.get(tf, {})
            symbol_rows: List[Dict[str, Any]] = []
            for sym, df in frames.items():
                run_idx += 1
                if run_idx % 100 == 0:
                    print(f"progress: {run_idx}/{total_runs} ({(run_idx / max(1, total_runs)):.1%})")
                try:
                    use_df = df.tail(max_bars) if max_bars > 0 else df
                    metrics = _run_backtest_core(
                        strategy=strategy,
                        df=use_df,
                        timeframe=tf,
                        initial_capital=max(10.0, float(args.initial_capital)),
                        include_series=False,
                        commission_rate=max(0.0, float(args.commission_rate)),
                        slippage_bps=max(0.0, float(args.slippage_bps)),
                    )
                    row = {
                        "strategy": strategy,
                        "timeframe": tf,
                        "symbol": sym,
                        "total_return": _safe_float(metrics.get("total_return")),
                        "sharpe_ratio": _safe_float(metrics.get("sharpe_ratio")),
                        "max_drawdown": _safe_float(metrics.get("max_drawdown")),
                        "win_rate": _safe_float(metrics.get("win_rate")),
                        "total_trades": _safe_float(metrics.get("total_trades")),
                    }
                    symbol_rows.append(row)
                    research_rows.append(dict(row))
                except Exception:
                    continue

            agg = _aggregate_strategy(strategy=strategy, timeframe=tf, symbol_rows=symbol_rows)
            if agg:
                aggregates.append(
                    {
                        "strategy": agg.strategy,
                        "timeframe": agg.timeframe,
                        "score": round(agg.score, 6),
                        "n_symbols": agg.n_symbols,
                        "avg_total_return": round(agg.avg_total_return, 6),
                        "median_total_return": round(agg.median_total_return, 6),
                        "avg_sharpe": round(agg.avg_sharpe, 6),
                        "median_sharpe": round(agg.median_sharpe, 6),
                        "avg_max_drawdown": round(agg.avg_max_drawdown, 6),
                        "avg_win_rate": round(agg.avg_win_rate, 6),
                        "positive_ratio": round(agg.positive_ratio, 6),
                        "avg_total_trades": round(agg.avg_total_trades, 6),
                        "top_symbols": agg.top_symbols,
                    }
                )

    if symbol_base and not bool(args.disable_fama):
        close_df = pd.DataFrame({sym: df["close"] for sym, df in symbol_base.items()}).sort_index()
        volume_df = pd.DataFrame({sym: df["volume"] for sym, df in symbol_base.items()}).sort_index()
        valid_cols = [
            c
            for c in close_df.columns
            if int(close_df[c].notna().sum()) >= max(240, int(args.fama_lookback_bars))
        ]
        close_df = close_df[valid_cols]
        volume_df = volume_df[valid_cols]
        fama = _fama_metrics(
            close_df=close_df,
            volume_df=volume_df,
            factor_timeframe=base_timeframe,
            lookback_bars=max(240, int(args.fama_lookback_bars)),
            rebalance_bars=max(1, int(args.fama_rebalance_bars)),
            quantile=max(0.05, min(0.45, float(args.fama_quantile))),
            top_n=max(2, int(args.fama_top_n)),
            fee_rate=max(0.0, float(args.commission_rate)),
            slippage_bps=max(0.0, float(args.slippage_bps)),
            min_abs_score=max(0.0, float(args.alpha_threshold)) if args.alpha_threshold is not None else 0.0,
        )
        if fama:
            aggregates.append(
                {
                    "strategy": fama["strategy"],
                    "timeframe": fama["timeframe"],
                    "score": round(float(fama["score"]), 6),
                    "n_symbols": int(fama["n_symbols"]),
                    "avg_total_return": round(float(fama["avg_total_return"]), 6),
                    "median_total_return": round(float(fama["median_total_return"]), 6),
                    "avg_sharpe": round(float(fama["avg_sharpe"]), 6),
                    "median_sharpe": round(float(fama["median_sharpe"]), 6),
                    "avg_max_drawdown": round(float(fama["avg_max_drawdown"]), 6),
                    "avg_win_rate": round(float(fama["avg_win_rate"]), 6),
                    "positive_ratio": round(float(fama["positive_ratio"]), 6),
                    "avg_total_trades": round(float(fama["avg_total_trades"]), 6),
                    "top_symbols": list(fama.get("top_symbols", []))[:20],
                    "config_hint": dict(fama.get("config_hint", {})),
                }
            )

    ranked = sorted(aggregates, key=lambda x: float(x.get("score", -999999)), reverse=True)

    top3: List[Dict[str, Any]] = []
    for row in ranked:
        params = _build_top3_params(
            strategy_name=str(row["strategy"]),
            row=row,
            alpha_threshold=args.alpha_threshold,
            cooldown_min=args.cooldown_min,
            max_vol=args.max_vol,
            max_spread=args.max_spread,
        )
        top3.append(
            {
                "strategy_type": row["strategy"],
                "timeframe": row["timeframe"],
                "exchange": exchange,
                "symbols": list(row.get("top_symbols", []))[:12],
                "allocation": 0.3333,
                "params": params,
                "score": row["score"],
            }
        )
        if len(top3) >= 3:
            break

    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "exchange": exchange,
        "base_timeframe": base_timeframe,
        "universe_count_input": len(symbols),
        "universe_count_loaded": len(symbol_base),
        "timeframes": timeframes,
        "initial_capital": float(args.initial_capital),
        "commission_rate": float(args.commission_rate),
        "slippage_bps": float(args.slippage_bps),
        "alpha_threshold": args.alpha_threshold,
        "cooldown_min": args.cooldown_min,
        "max_vol": args.max_vol,
        "max_spread": args.max_spread,
        "ranking": ranked,
        "top3": top3,
        "rows_total": len(research_rows),
    }
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "output_json": str(output_path.resolve()),
                "universe_count_loaded": len(symbol_base),
                "rows_total": len(research_rows),
                "top3": top3,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
