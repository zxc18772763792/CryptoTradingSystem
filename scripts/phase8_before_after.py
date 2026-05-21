"""Phase 8 before/after comparison report for the exit-logic overhaul.

Runs the same strategy on the same historical OHLCV slice twice:

- **Before** (legacy): SL/TP not honored at intra-bar level, no trailing,
  no time-stop, no Tier 2 management. Closes only on opposite directional
  signal or explicit ``CLOSE_LONG/SHORT``. This is the behavior any
  pre-2026-05-21 backtest would have produced.
- **After** (current): all Codex / Claude additions enabled: protective
  SL/TP at intra-bar level, ATR trailing, time stop, ``check_exit`` calls.

Output: ``reports/phase8_before_after_<strategy>_<symbol>_<timeframe>.md``
plus a console summary.

Usage:
    python scripts/phase8_before_after.py
    python scripts/phase8_before_after.py --strategy BollingerBandsStrategy --symbol BTC/USDT --timeframe 1h --days 90
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(REPO_ROOT))

from core.backtest.backtest_engine import BacktestConfig, BacktestEngine, BacktestResult  # noqa: E402


_DEFAULT_STRATEGIES: List[Tuple[str, str]] = [
    ("strategies.technical.bollinger_strategy", "BollingerBandsStrategy"),
    ("strategies.technical.rsi_strategy", "RSIStrategy"),
    ("strategies.technical.ma_strategy", "MAStrategy"),
]


@dataclass
class RunOutcome:
    label: str
    total_trades: int
    win_rate: float
    avg_pnl: float
    total_pnl: float
    sharpe: float
    max_drawdown: float
    exit_reason_dist: Counter
    avg_max_unrealized_pct: float


def _load_strategy(module_path: str, class_name: str) -> Any:
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)
    return cls()


def _load_data(symbol: str, timeframe: str, days: int) -> pd.DataFrame:
    sym_dir = symbol.replace("/", "_").replace(":", "_")
    path = REPO_ROOT / f"data/historical/binance/{sym_dir}/{timeframe}.parquet"
    if not path.exists():
        raise FileNotFoundError(
            f"Historical data not found: {path}. "
            f"Available symbols under data/historical/binance/; adjust --symbol."
        )
    df = pd.read_parquet(path)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    if days and days > 0:
        cutoff = df.index.max() - timedelta(days=int(days))
        df = df[df.index >= cutoff]
    return df


def _config_legacy() -> BacktestConfig:
    """Recreate pre-2026-05-21 behavior: ignore SL/TP, no Tier 2, no time stop,
    no strategy.check_exit calls. Only opposite-directional signals close positions -
    matching what every backtest before the Phase 1/3 changes would have done."""
    return BacktestConfig(
        honor_signal_stop_loss=False,
        honor_signal_take_profit=False,
        enable_protective_check=False,
        enable_trailing_stop=False,
        enable_time_stop=False,
        enable_strategy_check_exit=False,
    )


def _config_current() -> BacktestConfig:
    """Current behavior: all the new exit channels enabled."""
    return BacktestConfig(
        honor_signal_stop_loss=True,
        honor_signal_take_profit=True,
        enable_protective_check=True,
        enable_trailing_stop=True,
        enable_time_stop=True,
    )


def _summarize(label: str, result: BacktestResult) -> RunOutcome:
    closes = [t for t in result.trades if str(getattr(t, "trade_stage", "")) == "close"]
    n = len(closes)
    wins = sum(1 for t in closes if (t.net_pnl or 0) > 0)
    total_pnl = float(sum(t.net_pnl or 0 for t in closes))
    avg_pnl = total_pnl / n if n else 0.0
    win_rate = (wins / n) if n else 0.0

    exit_dist: Counter = Counter()
    max_unreal: List[float] = []
    for t in closes:
        reason = getattr(t, "exit_reason", None) or "unattributed"
        exit_dist[reason] += 1
        mu = getattr(t, "max_unrealized_pct", None)
        if mu is not None:
            max_unreal.append(float(mu))

    return RunOutcome(
        label=label,
        total_trades=n,
        win_rate=win_rate,
        avg_pnl=avg_pnl,
        total_pnl=total_pnl,
        sharpe=float(getattr(result, "sharpe_ratio", 0.0) or 0.0),
        max_drawdown=float(getattr(result, "max_drawdown", 0.0) or 0.0),
        exit_reason_dist=exit_dist,
        avg_max_unrealized_pct=(sum(max_unreal) / len(max_unreal)) if max_unreal else 0.0,
    )


async def _run_one(strategy_factory, data: pd.DataFrame, symbol: str, config: BacktestConfig, label: str) -> RunOutcome:
    engine = BacktestEngine(config=config)
    result = await engine.run_backtest(strategy_factory(), data, symbol=symbol)
    return _summarize(label, result)


def _pct(x: float) -> str:
    return f"{x * 100.0:+.2f}%"


def _render_report(
    *,
    strategy_name: str,
    symbol: str,
    timeframe: str,
    days: int,
    bars: int,
    before: RunOutcome,
    after: RunOutcome,
) -> str:
    lines: List[str] = []
    lines.append(f"# Phase 8 Before/After: `{strategy_name}` on `{symbol}` ({timeframe})")
    lines.append("")
    lines.append(f"- Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append(f"- Lookback: {days} days ({bars} bars)")
    lines.append(f"- **Before** = pre-2026-05-21 backtest (ignores SL/TP/trailing/time_stop)")
    lines.append(f"- **After**  = current behavior (all exit channels enabled)")
    lines.append("")
    lines.append("## Trade-level metrics")
    lines.append("")
    lines.append("| Metric | Before | After | Delta |")
    lines.append("|---|---:|---:|---:|")
    delta_trades = after.total_trades - before.total_trades
    delta_wr = after.win_rate - before.win_rate
    delta_pnl = after.total_pnl - before.total_pnl
    delta_avg = after.avg_pnl - before.avg_pnl
    delta_sharpe = after.sharpe - before.sharpe
    delta_dd = after.max_drawdown - before.max_drawdown
    lines.append(f"| Total closed trades | {before.total_trades} | {after.total_trades} | {delta_trades:+d} |")
    lines.append(f"| Win rate | {_pct(before.win_rate)} | {_pct(after.win_rate)} | {_pct(delta_wr)} |")
    lines.append(f"| Total net PnL | {before.total_pnl:.4f} | {after.total_pnl:.4f} | {delta_pnl:+.4f} |")
    lines.append(f"| Avg PnL per trade | {before.avg_pnl:.4f} | {after.avg_pnl:.4f} | {delta_avg:+.4f} |")
    lines.append(f"| Sharpe | {before.sharpe:.3f} | {after.sharpe:.3f} | {delta_sharpe:+.3f} |")
    lines.append(f"| Max drawdown | {_pct(before.max_drawdown)} | {_pct(after.max_drawdown)} | {_pct(delta_dd)} |")
    lines.append(f"| Avg max-unrealized | {_pct(before.avg_max_unrealized_pct)} | {_pct(after.avg_max_unrealized_pct)} | n/a |")
    lines.append("")
    lines.append("## Exit reason distribution")
    lines.append("")
    lines.append("| Reason | Before (count) | Before % | After (count) | After % |")
    lines.append("|---|---:|---:|---:|---:|")
    all_reasons = sorted(set(before.exit_reason_dist) | set(after.exit_reason_dist))
    for reason in all_reasons:
        b = before.exit_reason_dist.get(reason, 0)
        a = after.exit_reason_dist.get(reason, 0)
        b_pct = (b / before.total_trades * 100) if before.total_trades else 0.0
        a_pct = (a / after.total_trades * 100) if after.total_trades else 0.0
        lines.append(f"| `{reason}` | {b} | {b_pct:.1f}% | {a} | {a_pct:.1f}% |")
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    if before.total_trades > 0 and after.total_trades > before.total_trades * 1.3:
        lines.append(f"- After-mode has **{delta_trades:+d}** more closed trades - "
                     "consistent with SL/TP/trailing now firing intra-bar.")
    if abs(delta_wr) > 0.05:
        if delta_wr > 0:
            lines.append(f"- Win rate **rose** by {_pct(delta_wr)} - protective stops cut losses earlier "
                         "and trailing locked profits.")
        else:
            lines.append(f"- Win rate **fell** by {_pct(delta_wr)} - protective SLs are cutting winners that "
                         "would have recovered. Consider widening the ATR multiplier.")
    if delta_sharpe > 0.1:
        lines.append(f"- Sharpe improved by {delta_sharpe:+.2f}: exit channels reduce variance more than they cap winners.")
    elif delta_sharpe < -0.1:
        lines.append(f"- Sharpe degraded by {delta_sharpe:+.2f}: too-tight exits are clipping good trades.")
    if abs(delta_dd) > 0.02:
        if delta_dd < 0:
            lines.append(f"- Max drawdown shrank by {_pct(-delta_dd)} (less negative) - risk control working.")
        else:
            lines.append(f"- Max drawdown grew by {_pct(delta_dd)} - investigate.")
    sl_after = after.exit_reason_dist.get("stop_loss", 0)
    if after.total_trades and sl_after / after.total_trades > 0.5:
        lines.append(f"- **`stop_loss` share {sl_after / after.total_trades * 100:.0f}%** still dominates - Phase 7 "
                     "ATR-widening (Phase 7 toggle) or strategy-side `check_exit` tightening may help.")
    return "\n".join(lines) + "\n"


async def run_comparison(*, module_path: str, class_name: str, symbol: str, timeframe: str, days: int) -> Tuple[str, RunOutcome, RunOutcome]:
    def _factory():
        return _load_strategy(module_path, class_name)

    data = _load_data(symbol=symbol, timeframe=timeframe, days=days)
    before = await _run_one(_factory, data, symbol, _config_legacy(), label="before")
    after = await _run_one(_factory, data, symbol, _config_current(), label="after")
    report = _render_report(
        strategy_name=class_name,
        symbol=symbol,
        timeframe=timeframe,
        days=days,
        bars=len(data),
        before=before,
        after=after,
    )
    return report, before, after


def _save_report(report: str, *, strategy: str, symbol: str, timeframe: str) -> Path:
    sym_safe = symbol.replace("/", "_").replace(":", "_")
    out_dir = REPO_ROOT / "reports"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"phase8_before_after_{strategy}_{sym_safe}_{timeframe}.md"
    out_path.write_text(report, encoding="utf-8")
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 8 before/after comparison of exit logic.")
    parser.add_argument("--strategy", default="all", help="Class name (e.g. BollingerBandsStrategy) or 'all'.")
    parser.add_argument("--symbol", default="BTC/USDT")
    parser.add_argument("--timeframe", default="1h")
    parser.add_argument("--days", type=int, default=90)
    args = parser.parse_args()

    if args.strategy == "all":
        targets = _DEFAULT_STRATEGIES
    else:
        # Find module by class name in the defaults; fallback to bollinger module.
        found = None
        for mod_path, cls_name in _DEFAULT_STRATEGIES:
            if cls_name == args.strategy:
                found = (mod_path, cls_name)
                break
        if not found:
            raise SystemExit(
                f"Unknown strategy {args.strategy!r}. Supported via --strategy: "
                + ", ".join(c for _, c in _DEFAULT_STRATEGIES)
                + " or 'all'."
            )
        targets = [found]

    loop = asyncio.new_event_loop()
    try:
        for mod_path, cls_name in targets:
            print(f"\n=== {cls_name} on {args.symbol} {args.timeframe} ({args.days}d) ===")
            report, before, after = loop.run_until_complete(
                run_comparison(
                    module_path=mod_path,
                    class_name=cls_name,
                    symbol=args.symbol,
                    timeframe=args.timeframe,
                    days=args.days,
                )
            )
            out_path = _save_report(report, strategy=cls_name, symbol=args.symbol, timeframe=args.timeframe)
            print(report)
            print(f"[WROTE] {out_path}")
    finally:
        loop.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
