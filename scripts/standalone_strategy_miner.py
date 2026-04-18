"""Standalone CLI for mining the most profitable strategy from local data.

The script is intentionally independent from the web UI. It reads the local
parquet store, runs a curated set of backtests, and emits machine-readable
artifacts with two leaderboards:

1. current champion: weighted toward recent market behavior
2. robust champion: weighted toward cross-symbol / cross-window stability

The recommended pick defaults to the robust champion unless the recent winner
also has enough trade count and breadth to justify promotion.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


_HERE = Path(__file__).resolve()
_ROOT = _HERE.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.data import data_storage  # noqa: E402
from web.api.backtest import _run_backtest_core  # noqa: E402


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

DEFAULT_FAST_SYMBOLS = [
    "BTC/USDT",
    "ETH/USDT",
]

DEFAULT_SLOW_SYMBOLS = [
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
]

DEFAULT_FAST_TIMEFRAMES = ["10s", "15m"]
DEFAULT_SLOW_TIMEFRAMES = ["1h", "4h"]

DEFAULT_FAST_STRATEGIES = [
    "VWAPReversionStrategy",
    "ADXTrendStrategy",
    "MomentumStrategy",
]

DEFAULT_SLOW_STRATEGIES = [
    "ADXTrendStrategy",
    "DonchianBreakoutStrategy",
    "MACDStrategy",
]


@dataclass(frozen=True)
class WindowConfig:
    label: str
    phase: str
    days: int
    symbols: List[str]
    timeframes: List[str]
    strategies: List[str]


def _parse_csv(value: str) -> List[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _parse_symbols(value: str) -> List[str]:
    return [item.strip().upper().replace("_", "/") for item in _parse_csv(value)]


def _canonical_symbol(value: str) -> str:
    return str(value or "").strip().upper().replace("_", "/")


def _symbol_dir_name(symbol: str) -> str:
    return _canonical_symbol(symbol).replace("/", "_")


def _resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
    rule = RESAMPLE_RULES.get(str(timeframe or "").lower())
    if not rule:
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    src = df.copy()
    src.index = pd.to_datetime(src.index)
    src = src.sort_index()
    out = src[["open", "high", "low", "close"]].resample(rule).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}
    )
    volume = src[["volume"]].resample(rule).sum()
    out = pd.concat([out, volume], axis=1)
    return out.dropna(subset=["open", "high", "low", "close"])


def _data_file_exists(data_root: Path, exchange: str, symbol: str, timeframe: str) -> bool:
    symbol_root = data_root / "historical" / exchange / _symbol_dir_name(symbol)
    return (symbol_root / f"{timeframe}.parquet").exists() or (symbol_root / f"{timeframe}_parts").exists()


def _discover_symbols(
    data_root: Path,
    exchange: str,
    preferred: Sequence[str],
    *,
    required_timeframes: Sequence[str],
    max_count: int,
) -> List[str]:
    found: List[str] = []
    for raw_symbol in preferred:
        symbol = _canonical_symbol(raw_symbol)
        if not symbol:
            continue
        ok = all(_data_file_exists(data_root, exchange, symbol, tf) for tf in required_timeframes)
        if ok:
            found.append(symbol)
        if len(found) >= max_count:
            break
    return found


async def _load_frame_for_timeframe(
    *,
    exchange: str,
    symbol: str,
    timeframe: str,
    days: int,
) -> pd.DataFrame:
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=max(1, int(days)))

    preferred_sources: List[Tuple[str, bool]] = [(timeframe, False)]
    fallback_map = {
        "10s": "1s",
        "30s": "1m",
        "5m": "1m",
        "15m": "1m",
        "4h": "1h",
        "1d": "1h",
    }
    fallback_source = fallback_map.get(str(timeframe).lower())
    if fallback_source:
        preferred_sources.append((fallback_source, True))

    for source_tf, needs_resample in preferred_sources:
        frame = await data_storage.load_klines_from_parquet(
            exchange=exchange,
            symbol=symbol,
            timeframe=source_tf,
            start_time=start_time,
            end_time=end_time,
        )
        if frame is None or frame.empty:
            continue
        frame = frame[["open", "high", "low", "close", "volume"]].copy()
        frame.index = pd.to_datetime(frame.index)
        frame = frame.sort_index()
        if needs_resample:
            frame = _resample_ohlcv(frame, timeframe)
        return frame
    return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])


def _score_run(metrics: Dict[str, Any]) -> float:
    total_return = float(metrics.get("total_return", 0.0) or 0.0)
    sharpe_ratio = float(metrics.get("sharpe_ratio", 0.0) or 0.0)
    max_drawdown = float(metrics.get("max_drawdown", 0.0) or 0.0)
    win_rate = float(metrics.get("win_rate", 0.0) or 0.0)
    total_trades = float(metrics.get("total_trades", 0.0) or 0.0)
    anomaly_bar_ratio = float(metrics.get("anomaly_bar_ratio", 0.0) or 0.0)
    quality_flag = str(metrics.get("quality_flag") or "ok").strip().lower()

    trade_bonus = min(total_trades, 25.0) * 0.40
    trade_penalty = max(0.0, 3.0 - total_trades) * 12.0
    quality_penalty = 0.0 if quality_flag == "ok" else 25.0

    return (
        total_return * 0.45
        + sharpe_ratio * 16.0
        - max_drawdown * 0.35
        + win_rate * 0.03
        + trade_bonus
        - anomaly_bar_ratio * 200.0
        - trade_penalty
        - quality_penalty
    )


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if math.isnan(out) or math.isinf(out):
        return float(default)
    return out


def _aggregate_candidates(result_df: pd.DataFrame) -> pd.DataFrame:
    if result_df.empty:
        return pd.DataFrame()

    ok_df = result_df[result_df["error"].fillna("") == ""].copy()
    if ok_df.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    for (strategy, timeframe), group in ok_df.groupby(["strategy", "timeframe"], dropna=False):
        base = group[group["phase"] == "base"].copy()
        recent = group[group["phase"] == "recent"].copy()

        avg_base_score = _safe_float(base["score"].mean(), default=np.nan)
        avg_recent_score = _safe_float(recent["score"].mean(), default=np.nan)
        positive_ratio = _safe_float((group["total_return"] > 0).mean())
        recent_positive_ratio = _safe_float((recent["total_return"] > 0).mean())
        avg_trades = _safe_float(group["total_trades"].mean())
        recent_avg_trades = _safe_float(recent["total_trades"].mean())
        avg_sharpe = _safe_float(group["sharpe_ratio"].mean())
        avg_return = _safe_float(group["total_return"].mean())
        avg_drawdown = _safe_float(group["max_drawdown"].mean())
        score_std = _safe_float(group["score"].std(ddof=0))
        min_score = _safe_float(group["score"].min())

        robust_score = 0.0
        if not math.isnan(avg_base_score):
            robust_score += avg_base_score * 0.55
        if not math.isnan(avg_recent_score):
            robust_score += avg_recent_score * 0.45
        robust_score += positive_ratio * 12.0
        robust_score -= score_std * 0.25
        robust_score -= max(0.0, 5.0 - avg_trades) * 4.0
        robust_score += min_score * 0.10

        current_score = 0.0
        if not math.isnan(avg_recent_score):
            current_score += avg_recent_score
        current_score += recent_positive_ratio * 10.0
        current_score -= max(0.0, 4.0 - recent_avg_trades) * 6.0

        rows.append(
            {
                "strategy": strategy,
                "timeframe": timeframe,
                "runs": int(len(group)),
                "symbols": int(group["symbol"].nunique()),
                "avg_return": round(avg_return, 4),
                "avg_sharpe": round(avg_sharpe, 4),
                "avg_drawdown": round(avg_drawdown, 4),
                "avg_trades": round(avg_trades, 4),
                "positive_ratio": round(positive_ratio, 4),
                "recent_positive_ratio": round(recent_positive_ratio, 4),
                "avg_base_score": None if math.isnan(avg_base_score) else round(avg_base_score, 4),
                "avg_recent_score": None if math.isnan(avg_recent_score) else round(avg_recent_score, 4),
                "recent_avg_trades": round(recent_avg_trades, 4),
                "score_std": round(score_std, 4),
                "min_score": round(min_score, 4),
                "robust_score": round(robust_score, 4),
                "current_score": round(current_score, 4),
            }
        )

    if not rows:
        return pd.DataFrame()

    summary = pd.DataFrame(rows)
    return summary.sort_values(["robust_score", "current_score"], ascending=[False, False]).reset_index(drop=True)


def _pick_leaders(summary_df: pd.DataFrame) -> Dict[str, Any]:
    if summary_df.empty:
        return {"best_current": None, "best_robust": None, "recommended": None, "reason": "no candidates"}

    best_current = summary_df.sort_values(
        ["current_score", "robust_score"], ascending=[False, False]
    ).iloc[0].to_dict()
    best_robust = summary_df.sort_values(
        ["robust_score", "current_score"], ascending=[False, False]
    ).iloc[0].to_dict()

    current_is_promotable = (
        _safe_float(best_current.get("recent_avg_trades"), 0.0) >= 5.0
        and _safe_float(best_current.get("recent_positive_ratio"), 0.0) >= 0.60
        and _safe_float(best_current.get("current_score"), -9999.0)
        >= _safe_float(best_robust.get("current_score"), -9999.0) - 2.0
    )

    if current_is_promotable:
        recommended = dict(best_current)
        reason = "recent winner also has enough recent trades and breadth"
    else:
        recommended = dict(best_robust)
        reason = "defaulted to robust winner because the recent winner is too sparse or unstable"

    return {
        "best_current": best_current,
        "best_robust": best_robust,
        "recommended": recommended,
        "reason": reason,
    }


def _render_markdown_summary(
    *,
    summary: Dict[str, Any],
    leaderboard_df: pd.DataFrame,
    raw_df: pd.DataFrame,
) -> str:
    lines = [
        "# Standalone Strategy Miner",
        "",
        f"- Generated at: `{summary['generated_at']}`",
        f"- Exchange: `{summary['exchange']}`",
        f"- Fast symbols: `{', '.join(summary['fast_symbols'])}`",
        f"- Slow symbols: `{', '.join(summary['slow_symbols'])}`",
        "",
        "## Recommended",
        "",
        f"- Recommendation: `{summary['leaders']['recommended']['strategy']}` on `{summary['leaders']['recommended']['timeframe']}`",
        f"- Reason: {summary['leaders']['reason']}",
        "",
        "## Top Candidates",
        "",
        "| Rank | Strategy | Timeframe | Robust Score | Current Score | Avg Return | Avg Sharpe | Avg Trades |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]

    for idx, row in leaderboard_df.head(10).reset_index(drop=True).iterrows():
        lines.append(
            f"| {idx + 1} | {row['strategy']} | {row['timeframe']} | {row['robust_score']:.2f} | "
            f"{row['current_score']:.2f} | {row['avg_return']:.2f}% | {row['avg_sharpe']:.2f} | {row['avg_trades']:.1f} |"
        )

    lines.extend(
        [
            "",
            "## Raw Winner Snapshot",
            "",
        ]
    )

    raw_ok = raw_df[raw_df["error"].fillna("") == ""].copy()
    raw_ok = raw_ok.sort_values(["score", "total_return", "sharpe_ratio"], ascending=[False, False, False])
    for idx, row in raw_ok.head(8).reset_index(drop=True).iterrows():
        lines.append(
            f"{idx + 1}. `{row['strategy']}` @ `{row['symbol']}` `{row['timeframe']}` "
            f"`{row['window_label']}` score={row['score']:.2f} return={row['total_return']:.2f}% "
            f"sharpe={row['sharpe_ratio']:.2f} dd={row['max_drawdown']:.2f}% trades={row['total_trades']}"
        )

    return "\n".join(lines) + "\n"


async def _run_window(
    *,
    exchange: str,
    config: WindowConfig,
    initial_capital: float,
    commission_rate: float,
    slippage_bps: float,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    frame_cache: Dict[Tuple[str, str], pd.DataFrame] = {}

    for symbol in config.symbols:
        for timeframe in config.timeframes:
            cache_key = (symbol, timeframe)
            if cache_key not in frame_cache:
                frame_cache[cache_key] = await _load_frame_for_timeframe(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    days=config.days,
                )
            df = frame_cache[cache_key]
            if df is None or df.empty:
                rows.append(
                    {
                        "phase": config.phase,
                        "window_label": config.label,
                        "days": int(config.days),
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "strategy": "",
                        "error": "no data",
                    }
                )
                continue

            for strategy in config.strategies:
                try:
                    metrics = _run_backtest_core(
                        strategy=strategy,
                        df=df,
                        timeframe=timeframe,
                        initial_capital=max(100.0, float(initial_capital)),
                        include_series=False,
                        commission_rate=max(0.0, float(commission_rate)),
                        slippage_bps=max(0.0, float(slippage_bps)),
                    )
                    payload: Dict[str, Any] = {
                        "phase": config.phase,
                        "window_label": config.label,
                        "days": int(config.days),
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "strategy": strategy,
                        "rows": int(len(df)),
                        **metrics,
                        "error": "",
                    }
                    payload["score"] = round(_score_run(payload), 6)
                    rows.append(payload)
                except Exception as exc:
                    rows.append(
                        {
                            "phase": config.phase,
                            "window_label": config.label,
                            "days": int(config.days),
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "strategy": strategy,
                            "rows": int(len(df)),
                            "error": str(exc),
                        }
                    )
    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mine the best strategy from the local crypto database/parquet store"
    )
    parser.add_argument("--exchange", default="binance")
    parser.add_argument("--fast-symbols", default=",".join(DEFAULT_FAST_SYMBOLS))
    parser.add_argument("--slow-symbols", default=",".join(DEFAULT_SLOW_SYMBOLS))
    parser.add_argument("--max-fast-symbols", type=int, default=len(DEFAULT_FAST_SYMBOLS))
    parser.add_argument("--max-slow-symbols", type=int, default=len(DEFAULT_SLOW_SYMBOLS))
    parser.add_argument("--fast-timeframes", default=",".join(DEFAULT_FAST_TIMEFRAMES))
    parser.add_argument("--slow-timeframes", default=",".join(DEFAULT_SLOW_TIMEFRAMES))
    parser.add_argument("--fast-strategies", default=",".join(DEFAULT_FAST_STRATEGIES))
    parser.add_argument("--slow-strategies", default=",".join(DEFAULT_SLOW_STRATEGIES))
    parser.add_argument("--recent-fast-days", type=int, default=14)
    parser.add_argument("--base-fast-days", type=int, default=30)
    parser.add_argument("--recent-slow-days", type=int, default=90)
    parser.add_argument("--base-slow-days", type=int, default=180)
    parser.add_argument("--initial-capital", type=float, default=10000.0)
    parser.add_argument("--commission-rate", type=float, default=0.0004)
    parser.add_argument("--slippage-bps", type=float, default=2.0)
    parser.add_argument("--output-dir", default="data/research/standalone_miner")
    return parser.parse_args()


async def main_async(args: argparse.Namespace) -> Dict[str, Any]:
    exchange = str(args.exchange or "binance").strip().lower()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    data_root = _ROOT / "data"
    preferred_fast_symbols = _parse_symbols(args.fast_symbols)
    preferred_slow_symbols = _parse_symbols(args.slow_symbols)
    fast_timeframes = [tf.lower() for tf in _parse_csv(args.fast_timeframes)]
    slow_timeframes = [tf.lower() for tf in _parse_csv(args.slow_timeframes)]
    fast_strategies = _parse_csv(args.fast_strategies)
    slow_strategies = _parse_csv(args.slow_strategies)

    fast_symbols = _discover_symbols(
        data_root,
        exchange,
        preferred_fast_symbols,
        required_timeframes=["1m"],
        max_count=max(1, int(args.max_fast_symbols)),
    )
    fast_symbols = [
        symbol
        for symbol in fast_symbols
        if any(_data_file_exists(data_root, exchange, symbol, tf) for tf in fast_timeframes)
    ]
    slow_symbols = _discover_symbols(
        data_root,
        exchange,
        preferred_slow_symbols,
        required_timeframes=["1h"],
        max_count=max(1, int(args.max_slow_symbols)),
    )

    if not fast_symbols and not slow_symbols:
        raise RuntimeError("no symbols discovered for strategy mining")

    await data_storage.initialize()
    try:
        windows: List[WindowConfig] = []
        if fast_symbols and fast_timeframes and fast_strategies:
            windows.extend(
                [
                    WindowConfig(
                        label="fast_recent",
                        phase="recent",
                        days=max(1, int(args.recent_fast_days)),
                        symbols=fast_symbols,
                        timeframes=fast_timeframes,
                        strategies=fast_strategies,
                    ),
                    WindowConfig(
                        label="fast_base",
                        phase="base",
                        days=max(1, int(args.base_fast_days)),
                        symbols=fast_symbols,
                        timeframes=fast_timeframes,
                        strategies=fast_strategies,
                    ),
                ]
            )
        if slow_symbols and slow_timeframes and slow_strategies:
            windows.extend(
                [
                    WindowConfig(
                        label="slow_recent",
                        phase="recent",
                        days=max(1, int(args.recent_slow_days)),
                        symbols=slow_symbols,
                        timeframes=slow_timeframes,
                        strategies=slow_strategies,
                    ),
                    WindowConfig(
                        label="slow_base",
                        phase="base",
                        days=max(1, int(args.base_slow_days)),
                        symbols=slow_symbols,
                        timeframes=slow_timeframes,
                        strategies=slow_strategies,
                    ),
                ]
            )

        raw_rows: List[Dict[str, Any]] = []
        for window in windows:
            raw_rows.extend(
                await _run_window(
                    exchange=exchange,
                    config=window,
                    initial_capital=float(args.initial_capital),
                    commission_rate=float(args.commission_rate),
                    slippage_bps=float(args.slippage_bps),
                )
            )
    finally:
        await data_storage.close()

    raw_df = pd.DataFrame(raw_rows)
    if raw_df.empty:
        raise RuntimeError("strategy miner produced no rows")

    leaderboard_df = _aggregate_candidates(raw_df)
    leaders = _pick_leaders(leaderboard_df)

    raw_csv = output_dir / f"standalone_strategy_miner_raw_{run_ts}.csv"
    leaderboard_csv = output_dir / f"standalone_strategy_miner_leaderboard_{run_ts}.csv"
    summary_json = output_dir / f"standalone_strategy_miner_summary_{run_ts}.json"
    summary_md = output_dir / f"standalone_strategy_miner_summary_{run_ts}.md"

    raw_df.to_csv(raw_csv, index=False, encoding="utf-8-sig")
    leaderboard_df.to_csv(leaderboard_csv, index=False, encoding="utf-8-sig")

    summary_payload: Dict[str, Any] = {
        "generated_at": run_ts,
        "exchange": exchange,
        "fast_symbols": fast_symbols,
        "slow_symbols": slow_symbols,
        "fast_timeframes": fast_timeframes,
        "slow_timeframes": slow_timeframes,
        "fast_strategies": fast_strategies,
        "slow_strategies": slow_strategies,
        "recent_fast_days": int(args.recent_fast_days),
        "base_fast_days": int(args.base_fast_days),
        "recent_slow_days": int(args.recent_slow_days),
        "base_slow_days": int(args.base_slow_days),
        "commission_rate": float(args.commission_rate),
        "slippage_bps": float(args.slippage_bps),
        "rows_total": int(len(raw_df)),
        "rows_ok": int((raw_df["error"].fillna("") == "").sum()),
        "leaders": leaders,
        "raw_csv": str(raw_csv),
        "leaderboard_csv": str(leaderboard_csv),
    }

    summary_json.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    summary_md.write_text(
        _render_markdown_summary(
            summary=summary_payload,
            leaderboard_df=leaderboard_df,
            raw_df=raw_df,
        ),
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                "summary_json": str(summary_json),
                "summary_markdown": str(summary_md),
                "raw_csv": str(raw_csv),
                "leaderboard_csv": str(leaderboard_csv),
                "best_robust": leaders.get("best_robust"),
                "best_current": leaders.get("best_current"),
                "recommended": leaders.get("recommended"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return summary_payload


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
