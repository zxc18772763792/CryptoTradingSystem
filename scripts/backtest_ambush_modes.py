"""Backtest the OI/mcap ambush strategy family (modes A/B/C) on the enriched dataset.

Consumes the dataset produced by scripts/build_ambush_dataset.py, runs each mode
through core.backtest.BacktestEngine per symbol, aggregates trades into an
equal-risk portfolio with daily mark-to-market, splits in-sample (first 8 months)
vs out-of-sample (last 4 months) by entry time, and runs a one-at-a-time
parameter sensitivity sweep. Outputs a markdown report + csv/json artifacts to
reports/ambush_modes_<date>/.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

from core.backtest.backtest_engine import BacktestConfig, BacktestEngine  # noqa: E402
from strategies import (  # noqa: E402
    AccumulationAmbushStrategy,
    IgnitionFastFollowStrategy,
    SqueezeFuelStrategy,
)

DATA_DIR = PROJECT_ROOT / "data" / "research" / "ambush_modes"
REPORT_ROOT = PROJECT_ROOT / "reports"

MODES: Dict[str, Any] = {
    "A_accumulation": AccumulationAmbushStrategy,
    "B_squeeze": SqueezeFuelStrategy,
    "C_ignition": IgnitionFastFollowStrategy,
}
# 模式A按现货持有建模（无资金费）；B/C 是永续仓位，计入资金费。
MODE_INCLUDE_FUNDING = {"A_accumulation": False, "B_squeeze": True, "C_ignition": True}

PORTFOLIO_CAPITAL = 100_000.0
PER_TRADE_WEIGHT = 0.02
MAX_CONCURRENT = 10

SENSITIVITY: Dict[str, Dict[str, List[Any]]] = {
    "A_accumulation": {"oi_rise_min": [0.20, 0.35], "oi_mcap_min": [0.10, 0.25]},
    "B_squeeze": {"funding_max": [-0.0003, -0.001], "oi_mcap_min": [0.20, 0.50]},
    "C_ignition": {"volume_z_min": [3.0, 5.0], "ret_1h_min": [0.04, 0.08]},
}


def _read_parquet(sub: str, base: str) -> Optional[pd.DataFrame]:
    path = DATA_DIR / sub / f"{base}.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"read {path.name} failed: {exc}")
        return None
    return df if len(df) else None


def _shift_ffill(target_index: pd.DatetimeIndex, series: pd.Series, shift: pd.Timedelta, limit_bars: int) -> pd.Series:
    """Shift availability forward (anti-lookahead) then forward-fill onto target grid."""
    shifted = series.copy()
    shifted.index = shifted.index + shift
    shifted = shifted[~shifted.index.duplicated(keep="last")].sort_index()
    return shifted.reindex(target_index, method="ffill", limit=limit_bars)


def _load_current_mcap_map() -> Dict[str, float]:
    path = DATA_DIR / "current_mcap.json"
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return {str(k): float(v) for k, v in raw.items() if v}
    except Exception:  # noqa: BLE001
        return {}


CURRENT_MCAP = _load_current_mcap_map()
SUPPLY_APPROX_BASES: List[str] = []


def load_enriched_frame(base: str) -> Optional[pd.DataFrame]:
    klines = _read_parquet("klines_1h", base)
    if klines is None or len(klines) < 500:
        return None
    frame = klines[["open", "high", "low", "close", "volume"]].copy()
    frame = frame[~frame.index.duplicated(keep="last")].sort_index()
    idx = frame.index

    oi_4h = _read_parquet("oi_4h", base)
    oi_1d = _read_parquet("oi_1d", base)
    oi = pd.Series(np.nan, index=idx)
    if oi_1d is not None:
        # Daily OI close is only known at the end of the day → +24h availability shift.
        oi = _shift_ffill(idx, oi_1d["oi_usd"], pd.Timedelta(hours=24), limit_bars=120)
    if oi_4h is not None:
        oi_fast = _shift_ffill(idx, oi_4h["oi_usd"], pd.Timedelta(hours=4), limit_bars=72)
        oi = oi_fast.combine_first(oi)
    frame["oi_usd"] = oi

    funding = _read_parquet("funding", base)
    if funding is not None:
        # Rates are stamped at settlement time → already known from that moment on.
        frame["funding_rate"] = (
            funding["funding_rate"][~funding.index.duplicated(keep="last")]
            .sort_index()
            .reindex(idx, method="ffill", limit=24)
        )
    else:
        frame["funding_rate"] = np.nan

    mcap = _read_parquet("mcap_1d", base)
    if mcap is not None:
        frame["mcap_usd"] = _shift_ffill(idx, mcap["mcap_usd"], pd.Timedelta(hours=24), limit_bars=168)
    elif base in CURRENT_MCAP:
        # 供应量近似：mcap_t ≈ 当前市值 × price_t / price_now。对有大解锁的币会高估
        # 历史市值（= 低估历史 OI/市值），偏保守；来源在报告中单独披露。
        last_close = float(pd.to_numeric(frame["close"], errors="coerce").dropna().iloc[-1])
        if last_close > 0:
            frame["mcap_usd"] = pd.to_numeric(frame["close"], errors="coerce") * (
                CURRENT_MCAP[base] / last_close
            )
            SUPPLY_APPROX_BASES.append(base)
        else:
            frame["mcap_usd"] = np.nan
    else:
        frame["mcap_usd"] = np.nan

    frame["symbol"] = f"{base}/USDT"
    return frame


def precheck(base: str, frame: pd.DataFrame, mode: str, params: Dict[str, Any]) -> bool:
    """Vectorized necessary conditions so the bar-loop engine only runs where a signal is possible."""
    oi = pd.to_numeric(frame["oi_usd"], errors="coerce")
    mcap = pd.to_numeric(frame["mcap_usd"], errors="coerce")
    funding = pd.to_numeric(frame["funding_rate"], errors="coerce")
    ratio = oi / mcap
    mcap_min = float(params.get("mcap_min_usd", 20e6))
    mcap_max = float(params.get("mcap_max_usd", 1000e6))
    in_band = (mcap >= mcap_min) & (mcap <= mcap_max)
    ratio_ok = ratio >= float(params.get("oi_mcap_min", 0.15))
    if not bool((in_band & ratio_ok).any()):
        return False
    if mode == "B_squeeze":
        return bool((funding <= float(params.get("funding_max", -0.0005))).any())
    if mode == "C_ignition":
        ret_1h = pd.to_numeric(frame["close"], errors="coerce").pct_change()
        return bool((ret_1h >= float(params.get("ret_1h_min", 0.06))).any())
    return True


# Engine bars are expensive (~4ms) — feed it only merged windows around bars where
# the mode's *necessary* conditions hold. The strategy itself stays the source of
# truth; windows pad enough history (warmup) and future (max holding) around hits.
WINDOW_PAD = {
    "A_accumulation": (620, 580),
    "B_squeeze": (300, 220),
    "C_ignition": (790, 80),
}


def candidate_subframe(frame: pd.DataFrame, mode: str, params: Dict[str, Any]) -> Optional[pd.DataFrame]:
    oi = pd.to_numeric(frame["oi_usd"], errors="coerce")
    mcap = pd.to_numeric(frame["mcap_usd"], errors="coerce")
    funding = pd.to_numeric(frame["funding_rate"], errors="coerce")
    close = pd.to_numeric(frame["close"], errors="coerce")
    mask = (
        (mcap >= float(params.get("mcap_min_usd", 20e6)))
        & (mcap <= float(params.get("mcap_max_usd", 1000e6)))
        & ((oi / mcap) >= float(params.get("oi_mcap_min", 0.15)))
    )
    if mode == "B_squeeze":
        mask &= funding <= float(params.get("funding_max", -0.0005))
    elif mode == "C_ignition":
        mask &= close.pct_change() >= float(params.get("ret_1h_min", 0.06))
    elif mode == "A_accumulation":
        rise_bars = int(params.get("oi_rise_bars", 168))
        oi_change = oi / oi.shift(rise_bars) - 1.0
        funding_avg = funding.rolling(int(params.get("funding_avg_bars", 168)), min_periods=24).mean()
        # 放宽 5% 余量，避免向量近似与策略内 NaN 清洗的边界差异漏掉触发点。
        mask &= (oi_change >= float(params.get("oi_rise_min", 0.25)) * 0.95) & (
            funding_avg <= float(params.get("funding_avg_max", 0.00005)) + 1e-6
        )
    hits = np.flatnonzero(mask.fillna(False).to_numpy())
    if not len(hits):
        return None
    before, after = WINDOW_PAD[mode]
    intervals: List[List[int]] = []
    for hit in hits:
        lo, hi = max(0, hit - before), min(len(frame), hit + after)
        if intervals and lo <= intervals[-1][1]:
            intervals[-1][1] = max(intervals[-1][1], hi)
        else:
            intervals.append([lo, hi])
    covered = sum(hi - lo for lo, hi in intervals)
    if covered >= 0.7 * len(frame):
        return frame
    parts = [frame.iloc[lo:hi] for lo, hi in intervals]
    return pd.concat(parts)


def _pair_trades(result_trades: List[Any], base: str, mode: str) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    open_row: Optional[Any] = None
    for trade in result_trades:
        stage = getattr(trade, "trade_stage", "")
        if stage == "open":
            open_row = trade
        elif stage == "close" and open_row is not None:
            total_net = float(open_row.net_pnl) + float(trade.net_pnl)
            notional = float(open_row.notional) or 1.0
            records.append(
                {
                    "mode": mode,
                    "base": base,
                    "entry_time": pd.Timestamp(open_row.timestamp).tz_localize(None)
                    if pd.Timestamp(open_row.timestamp).tzinfo
                    else pd.Timestamp(open_row.timestamp),
                    "exit_time": pd.Timestamp(trade.timestamp).tz_localize(None)
                    if pd.Timestamp(trade.timestamp).tzinfo
                    else pd.Timestamp(trade.timestamp),
                    "entry_price": float(open_row.price),
                    "exit_price": float(trade.price),
                    "ret_pct": total_net / notional,
                    "gross_ret_pct": float(trade.gross_pnl) / notional,
                    "funding_ret_pct": float(trade.funding_pnl) / notional,
                    "exit_reason": getattr(trade, "exit_reason", None) or "unknown",
                }
            )
            open_row = None
    return records


async def run_mode(
    mode: str,
    bases: List[str],
    frames: Dict[str, pd.DataFrame],
    param_overrides: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    strategy_cls = MODES[mode]
    defaults = strategy_cls.default_params()
    effective = dict(defaults)
    if param_overrides:
        effective.update(param_overrides)
    all_trades: List[Dict[str, Any]] = []
    ran = 0
    for base in bases:
        frame = frames[base]
        if not precheck(base, frame, mode, effective):
            continue
        frame = candidate_subframe(frame, mode, effective)
        if frame is None or len(frame) < 300:
            continue
        strategy = strategy_cls(f"{mode}:{base}", params=dict(param_overrides or {}))
        config = BacktestConfig(
            initial_capital=10_000.0,
            position_size_pct=0.98,
            max_positions=1,
            enable_shorting=False,
            leverage=1.0,
            fee_model="maker_taker",
            taker_fee=0.0005,
            slippage_model="dynamic",
            include_funding=MODE_INCLUDE_FUNDING[mode],
            funding_interval_hours=8,
        )
        engine = BacktestEngine(config)
        try:
            result = await engine.run_backtest(strategy, frame, symbol=f"{base}/USDT")
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"{mode} {base} backtest failed: {exc}")
            continue
        all_trades.extend(_pair_trades(result.trades, base, mode))
        ran += 1
    logger.info(f"{mode}: engine ran on {ran}/{len(bases)} symbols, trades={len(all_trades)}")
    return all_trades


def simulate_portfolio(
    trades: List[Dict[str, Any]],
    daily_close: Dict[str, pd.Series],
) -> Dict[str, Any]:
    """Equal-risk portfolio: 2% per entry, max 10 concurrent, daily mark-to-market."""
    if not trades:
        return {"trades": 0}
    ordered = sorted(trades, key=lambda t: t["entry_time"])
    start = min(t["entry_time"] for t in ordered).normalize()
    end = max(t["exit_time"] for t in ordered).normalize() + pd.Timedelta(days=1)
    days = pd.date_range(start, end, freq="1D")

    capital = PORTFOLIO_CAPITAL
    open_positions: List[Dict[str, Any]] = []
    taken = 0
    skipped_concurrency = 0
    equity_rows: List[Tuple[pd.Timestamp, float]] = []
    trade_iter = iter(ordered)
    pending = next(trade_iter, None)

    for day in days:
        day_end = day + pd.Timedelta(days=1)
        while pending is not None and pending["entry_time"] < day_end:
            still_open = [p for p in open_positions if p["trade"]["exit_time"] > pending["entry_time"]]
            if len(still_open) >= MAX_CONCURRENT:
                skipped_concurrency += 1
            else:
                stake = capital * PER_TRADE_WEIGHT
                open_positions.append({"trade": pending, "stake": stake})
                taken += 1
            pending = next(trade_iter, None)

        closed = [p for p in open_positions if p["trade"]["exit_time"] <= day_end]
        for pos in closed:
            capital += pos["stake"] * pos["trade"]["ret_pct"]
        open_positions = [p for p in open_positions if p["trade"]["exit_time"] > day_end]

        mtm = 0.0
        for pos in open_positions:
            trade = pos["trade"]
            series = daily_close.get(trade["base"])
            if series is None:
                continue
            marks = series[series.index <= day]
            if marks.empty:
                continue
            mark_price = float(marks.iloc[-1])
            if trade["entry_price"] > 0 and math.isfinite(mark_price):
                mtm += pos["stake"] * (mark_price / trade["entry_price"] - 1.0)
        equity_rows.append((day, capital + mtm))

    equity = pd.Series({ts: value for ts, value in equity_rows}).sort_index()
    returns = equity.pct_change().dropna()
    peak = equity.cummax()
    drawdown = (equity - peak) / peak
    n_days = max(1, (equity.index[-1] - equity.index[0]).days)
    total_ret = equity.iloc[-1] / PORTFOLIO_CAPITAL - 1.0
    annualized = (1.0 + total_ret) ** (365.0 / n_days) - 1.0 if total_ret > -1 else -1.0
    sharpe = float(returns.mean() / returns.std() * math.sqrt(365.0)) if returns.std() > 0 else 0.0

    rets = np.array([t["ret_pct"] for t in ordered], dtype=float)
    wins = rets[rets > 0]
    losses = rets[rets <= 0]
    return {
        "trades": int(len(ordered)),
        "taken": int(taken),
        "skipped_concurrency": int(skipped_concurrency),
        "win_rate": float(len(wins) / len(rets)) if len(rets) else 0.0,
        "avg_ret": float(rets.mean()) if len(rets) else 0.0,
        "median_ret": float(np.median(rets)) if len(rets) else 0.0,
        "avg_win": float(wins.mean()) if len(wins) else 0.0,
        "avg_loss": float(losses.mean()) if len(losses) else 0.0,
        "best": float(rets.max()) if len(rets) else 0.0,
        "worst": float(rets.min()) if len(rets) else 0.0,
        "p95": float(np.percentile(rets, 95)) if len(rets) else 0.0,
        "profit_factor": float(wins.sum() / abs(losses.sum())) if len(losses) and losses.sum() != 0 else float("inf"),
        "portfolio_total_return": float(total_ret),
        "portfolio_annualized": float(annualized),
        "portfolio_max_drawdown": float(drawdown.min()) if len(drawdown) else 0.0,
        "portfolio_sharpe": sharpe,
        "period_days": int(n_days),
        "equity": equity,
    }


def _fmt_pct(value: Any) -> str:
    try:
        numeric = float(value)
    except Exception:
        return "-"
    if not math.isfinite(numeric):
        return "inf"
    return f"{numeric * 100:.1f}%"


def summarize(tag: str, stats: Dict[str, Any]) -> str:
    if not stats or not stats.get("trades"):
        return f"| {tag} | 0 | - | - | - | - | - | - | - | - |"
    return (
        f"| {tag} | {stats['trades']} | {_fmt_pct(stats['win_rate'])} | {_fmt_pct(stats['avg_ret'])} "
        f"| {_fmt_pct(stats['avg_win'])} | {_fmt_pct(stats['avg_loss'])} | {_fmt_pct(stats['best'])} "
        f"| {_fmt_pct(stats['portfolio_total_return'])} | {_fmt_pct(stats['portfolio_annualized'])} "
        f"| {_fmt_pct(stats['portfolio_max_drawdown'])} |"
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oos-split", default="2026-03-18")
    parser.add_argument("--skip-sensitivity", action="store_true")
    args = parser.parse_args()

    bases = sorted(p.stem for p in (DATA_DIR / "klines_1h").glob("*.parquet"))
    frames: Dict[str, pd.DataFrame] = {}
    excluded: Dict[str, str] = {}
    for base in bases:
        frame = load_enriched_frame(base)
        if frame is None:
            excluded[base] = "insufficient_klines"
            continue
        if pd.to_numeric(frame["mcap_usd"], errors="coerce").notna().sum() < 24:
            excluded[base] = "no_mcap_history"
            continue
        if pd.to_numeric(frame["oi_usd"], errors="coerce").notna().sum() < 24:
            excluded[base] = "no_oi_history"
            continue
        frames[base] = frame
    usable = sorted(frames.keys())
    logger.info(f"usable symbols: {len(usable)}, excluded: {len(excluded)}")

    daily_close = {
        base: frame["close"].resample("1D").last().dropna() for base, frame in frames.items()
    }

    split_ts = pd.Timestamp(args.oos_split)
    report_dir = REPORT_ROOT / f"ambush_modes_{datetime.now(timezone.utc):%Y-%m-%d}"
    report_dir.mkdir(parents=True, exist_ok=True)

    # 等权持有整个宇宙的基准（每日再平衡近似：归一化收盘均值）。
    normalized = []
    for base, series in daily_close.items():
        if len(series) > 30:
            normalized.append(series / series.iloc[0])
    benchmark = pd.concat(normalized, axis=1).mean(axis=1) if normalized else pd.Series(dtype=float)
    benchmark_ret = float(benchmark.iloc[-1] / benchmark.iloc[0] - 1.0) if len(benchmark) else None
    benchmark_dd = None
    if len(benchmark):
        peak = benchmark.cummax()
        benchmark_dd = float(((benchmark - peak) / peak).min())
        benchmark.to_csv(report_dir / "benchmark_equal_weight.csv")

    all_trades: List[Dict[str, Any]] = []
    mode_stats: Dict[str, Dict[str, Any]] = {}
    for mode in MODES:
        trades = await run_mode(mode, usable, frames)
        all_trades.extend(trades)
        full = simulate_portfolio(trades, daily_close)
        insample = simulate_portfolio([t for t in trades if t["entry_time"] < split_ts], daily_close)
        oos = simulate_portfolio([t for t in trades if t["entry_time"] >= split_ts], daily_close)
        mode_stats[mode] = {"full": full, "is": insample, "oos": oos}
        if isinstance(full.get("equity"), pd.Series):
            full["equity"].to_csv(report_dir / f"equity_{mode}.csv")

    if all_trades:
        trades_df = pd.DataFrame(all_trades).sort_values("entry_time")
        trades_df.to_csv(report_dir / "trades.csv", index=False)

    sensitivity_rows: List[Dict[str, Any]] = []
    if not args.skip_sensitivity:
        for mode, grid in SENSITIVITY.items():
            for param, values in grid.items():
                for value in values:
                    trades = await run_mode(mode, usable, frames, {param: value})
                    stats = simulate_portfolio(trades, daily_close)
                    sensitivity_rows.append(
                        {
                            "mode": mode,
                            "param": param,
                            "value": value,
                            "trades": stats.get("trades", 0),
                            "win_rate": stats.get("win_rate"),
                            "avg_ret": stats.get("avg_ret"),
                            "portfolio_total_return": stats.get("portfolio_total_return"),
                            "portfolio_max_drawdown": stats.get("portfolio_max_drawdown"),
                        }
                    )
        if sensitivity_rows:
            pd.DataFrame(sensitivity_rows).to_csv(report_dir / "sensitivity.csv", index=False)

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe_usable": usable,
        "excluded": excluded,
        "oos_split": str(split_ts.date()),
        "mcap_supply_approx_bases": sorted(set(SUPPLY_APPROX_BASES)),
        "benchmark_equal_weight_return": benchmark_ret,
        "benchmark_equal_weight_max_drawdown": benchmark_dd,
        "portfolio": {"capital": PORTFOLIO_CAPITAL, "per_trade": PER_TRADE_WEIGHT, "max_concurrent": MAX_CONCURRENT},
        "modes": {
            mode: {
                window: {k: v for k, v in stats.items() if k != "equity"}
                for window, stats in windows.items()
            }
            for mode, windows in mode_stats.items()
        },
    }
    (report_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    lines = [
        "# OI/市值 埋伏策略族回测摘要（自动生成）",
        "",
        f"- 生成时间: {summary['generated_at']}",
        f"- 可用标的: {len(usable)}（排除 {len(excluded)}），样本外切分: {summary['oos_split']}",
        f"- 组合假设: 初始 {PORTFOLIO_CAPITAL:,.0f}，单笔 {PER_TRADE_WEIGHT:.0%}，最多 {MAX_CONCURRENT} 并发",
        "",
        "| 模式/窗口 | 笔数 | 胜率 | 单笔均值 | 均盈 | 均亏 | 最佳 | 组合收益 | 年化 | 最大回撤 |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for mode, windows in mode_stats.items():
        lines.append(summarize(f"{mode} 全样本", windows["full"]))
        lines.append(summarize(f"{mode} IS", windows["is"]))
        lines.append(summarize(f"{mode} OOS", windows["oos"]))
    (report_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    logger.info(f"report artifacts -> {report_dir}")


if __name__ == "__main__":
    asyncio.run(main())
