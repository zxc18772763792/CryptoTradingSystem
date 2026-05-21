"""Gate post-change live exit samples for the exit-logic overhaul.

This script intentionally reports ``PENDING`` instead of failing when there are
not enough new live close samples yet. It is the repeatable check for the
remaining Phase 0/5/8 items that cannot be proven from offline tests alone.

Examples:
    python scripts/verify_post_change_live_exits.py --since 2026-05-21T00:00:00+00:00
    python scripts/verify_post_change_live_exits.py --days 30 --min-samples 20 --output reports/post_change_live_exit_gate.md
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core.backtest.backtest_engine import BacktestConfig, BacktestEngine
from scripts import audit_close_reason_coverage as coverage
from scripts import audit_exit_reasons as exit_audit
from strategies.technical.bollinger_strategy import BollingerBandsStrategy


STRATEGY_EXIT_REASONS = {
    "bollinger_middle_reversion",
    "bollinger_squeeze_recontract",
    "rsi_long_exit",
    "rsi_short_exit",
    "ma_diff_compression",
    "ema_diff_compression",
    "macd_histogram_flip",
    "generic_sma_profit_lock",
    "vwap_mean_reversion_completed",
    "signal_close",
    "signal_close_legacy",
}


@dataclass
class GateRow:
    gate: str
    value: str
    status: str
    note: str = ""


@dataclass
class LiveGateSummary:
    generated_at: datetime
    days: int
    since: Optional[datetime]
    min_samples: int
    journal_closes: int
    known_mode_closes: int
    bollinger_live_closes: int
    rows: List[GateRow]


def _status_for_ratio(numerator: int, denominator: int, threshold: float, *, min_samples: int) -> Tuple[str, float]:
    if denominator < min_samples:
        return "PENDING", 0.0 if denominator <= 0 else numerator / denominator
    ratio = numerator / denominator if denominator else 0.0
    return ("PASS" if ratio >= threshold else "FAIL"), ratio


def _parse_time(value: Any) -> Optional[datetime]:
    return exit_audit._parse_time(value)


def _filter_since(rows: Iterable[Dict[str, Any]], since: Optional[datetime]) -> List[Dict[str, Any]]:
    if since is None:
        return list(rows)
    cutoff = since.astimezone(timezone.utc)
    out: List[Dict[str, Any]] = []
    for row in rows:
        ts = _parse_time(row.get("timestamp") or row.get("time") or row.get("created_at"))
        if ts is None or ts >= cutoff:
            out.append(row)
    return out


def _load_journal_close_rows(*, days: int, since: Optional[datetime], now: datetime) -> List[Dict[str, Any]]:
    rows = exit_audit._filter_by_days(exit_audit._load_jsonl(exit_audit.LIVE_JOURNAL_PATH), days, now)
    rows = _filter_since(rows, since)
    return [row for row in rows if row.get("action") == "close"]


def _safe_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    if pd.isna(out):
        return None
    return out


def _slippage_bps(row: Dict[str, Any]) -> Optional[float]:
    for key in ("close_slippage_bps", "slippage_bps"):
        val = _safe_float(row.get(key))
        if val is not None:
            return abs(val)
    pct = _safe_float(row.get("close_slippage_pct") or row.get("slippage_pct"))
    if pct is not None:
        return abs(pct) * 10_000.0
    return None


def _is_limit_mode(mode: str) -> bool:
    text = mode.strip().lower()
    return text in {"limit", "limit_first", "post_only_limit"} or text.startswith("limit")


def _strategy_name(row: Dict[str, Any]) -> str:
    signal = row.get("signal")
    parts = [str(row.get("strategy") or row.get("strategy_name") or "")]
    if isinstance(signal, dict):
        parts.append(str(signal.get("strategy_name") or ""))
    return " ".join(parts).lower()


def _is_bollinger(row: Dict[str, Any]) -> bool:
    return "bollinger" in _strategy_name(row)


def _pnl(row: Dict[str, Any]) -> float:
    return float(row.get("net_pnl_usd") if row.get("net_pnl_usd") is not None else row.get("pnl") or 0.0)


def _pct(value: float) -> str:
    return f"{float(value) * 100.0:.1f}%"


def _fmt_bps(value: Optional[float]) -> str:
    return "n/a" if value is None else f"{value:.2f} bps"


def _load_local_btc_1h(days: int) -> pd.DataFrame:
    path = REPO_ROOT / "data/historical/binance/BTC_USDT/1h.parquet"
    if not path.exists():
        return pd.DataFrame()
    data = pd.read_parquet(path)
    if not isinstance(data.index, pd.DatetimeIndex):
        if "timestamp" in data.columns:
            data.index = pd.to_datetime(data["timestamp"])
        elif "open_time" in data.columns:
            data.index = pd.to_datetime(data["open_time"])
    if not isinstance(data.index, pd.DatetimeIndex):
        return pd.DataFrame()
    data = data.sort_index()
    if days > 0:
        cutoff = data.index.max() - pd.Timedelta(days=days)
        data = data[data.index >= cutoff]
    return data.copy()


async def _bollinger_backtest_win_rate(days: int) -> Optional[float]:
    data = _load_local_btc_1h(days)
    if data.empty:
        return None
    result = await BacktestEngine(BacktestConfig()).run_backtest(
        BollingerBandsStrategy("BollingerBandsStrategy", {"period": 20, "num_std": 2.0}),
        data,
        symbol="BTC/USDT",
    )
    closes = [trade for trade in result.trades if trade.trade_stage == "close"]
    if not closes:
        return None
    wins = sum(1 for trade in closes if float(trade.net_pnl or 0.0) > 0.0)
    return wins / len(closes)


def _add_gate(rows: List[GateRow], gate: str, value: str, status: str, note: str = "") -> None:
    rows.append(GateRow(gate=gate, value=value, status=status, note=note))


def build_summary(
    *,
    days: int = 30,
    since: Optional[datetime] = None,
    min_samples: int = 10,
    coverage_threshold: float = 0.95,
    limit_threshold: float = 0.50,
    max_slippage_bps: float = 3.0,
    signal_close_threshold: float = 0.15,
    max_win_rate_delta: float = 0.10,
) -> LiveGateSummary:
    now = datetime.now(timezone.utc)
    since_utc = since.astimezone(timezone.utc) if since else None
    close_rows = _load_journal_close_rows(days=days, since=since_utc, now=now)
    close_count = len(close_rows)
    rows: List[GateRow] = []

    cov = coverage.audit_close_reason_coverage(days=days, now=now, since=since_utc)
    if cov.journal_closes < min_samples:
        journal_status = "PENDING"
    else:
        journal_status = "PASS" if cov.journal_coverage >= coverage_threshold else "FAIL"
    _add_gate(
        rows,
        "Journal close_reason coverage",
        f"{_pct(cov.journal_coverage)} ({cov.journal_closes - cov.journal_missing_close_reason}/{cov.journal_closes})",
        journal_status,
        f"minimum samples: {min_samples}",
    )

    vwap_status = "PENDING" if cov.vwap_close_rows == 0 else (
        "PASS"
        if cov.vwap_reason_rows == cov.vwap_close_rows and cov.vwap_sell_misclassified_rows == 0
        else "FAIL"
    )
    _add_gate(
        rows,
        "VWAP journal close semantics",
        f"{cov.vwap_reason_rows}/{cov.vwap_close_rows} reason rows, {cov.vwap_sell_misclassified_rows} SELL rows",
        vwap_status,
        "pending until VWAPReversion has post-change close rows",
    )

    reason_counts = Counter(exit_audit._close_reason(row) for row in close_rows)
    active_count = sum(count for reason, count in reason_counts.items() if reason in STRATEGY_EXIT_REASONS)
    active_status, active_ratio = _status_for_ratio(
        active_count,
        close_count,
        signal_close_threshold,
        min_samples=min_samples,
    )
    _add_gate(
        rows,
        "Active strategy close share",
        f"{_pct(active_ratio)} ({active_count}/{close_count})",
        active_status,
        "strategy-specific close_reason or signal_close-like rows",
    )

    modes = [str(row.get("close_order_mode") or "unknown").strip().lower() for row in close_rows]
    known_modes = [mode for mode in modes if mode and mode != "unknown"]
    limit_count = sum(1 for mode in known_modes if _is_limit_mode(mode))
    limit_status, limit_ratio = _status_for_ratio(
        limit_count,
        len(known_modes),
        limit_threshold,
        min_samples=min_samples,
    )
    _add_gate(
        rows,
        "LIMIT close order share",
        f"{_pct(limit_ratio)} ({limit_count}/{len(known_modes)})",
        limit_status,
        "unknown historical modes are excluded",
    )

    slips = [_slippage_bps(row) for row in close_rows]
    known_slips = [val for val in slips if val is not None]
    avg_slip = sum(known_slips) / len(known_slips) if known_slips else None
    if len(known_slips) < min_samples:
        slip_status = "PENDING"
    else:
        slip_status = "PASS" if avg_slip is not None and avg_slip <= max_slippage_bps else "FAIL"
    _add_gate(
        rows,
        "Average close slippage",
        f"{_fmt_bps(avg_slip)} ({len(known_slips)} samples)",
        slip_status,
        f"target <= {max_slippage_bps:.2f} bps",
    )

    bollinger_rows = [row for row in close_rows if _is_bollinger(row)]
    live_wr = (
        sum(1 for row in bollinger_rows if _pnl(row) > 0.0) / len(bollinger_rows)
        if bollinger_rows
        else None
    )
    backtest_wr = asyncio.run(_bollinger_backtest_win_rate(days))
    if len(bollinger_rows) < min_samples or live_wr is None or backtest_wr is None:
        bollinger_status = "PENDING"
        value = (
            f"live n={len(bollinger_rows)}, live={_pct(live_wr or 0.0)}, "
            f"backtest={'n/a' if backtest_wr is None else _pct(backtest_wr)}"
        )
    else:
        delta = abs(live_wr - backtest_wr)
        bollinger_status = "PASS" if delta <= max_win_rate_delta else "FAIL"
        value = f"live={_pct(live_wr)}, backtest={_pct(backtest_wr)}, delta={_pct(delta)}"
    _add_gate(
        rows,
        "Bollinger live/backtest win-rate delta",
        value,
        bollinger_status,
        f"target delta <= {_pct(max_win_rate_delta)}",
    )

    return LiveGateSummary(
        generated_at=now,
        days=int(days or 0),
        since=since_utc,
        min_samples=int(min_samples),
        journal_closes=close_count,
        known_mode_closes=len(known_modes),
        bollinger_live_closes=len(bollinger_rows),
        rows=rows,
    )


def render_markdown(summary: LiveGateSummary) -> str:
    lines = [
        f"# Post-Change Live Exit Gate ({summary.generated_at.date().isoformat()})",
        "",
        f"- Lookback days: `{summary.days}` (`0` means all local history)",
        f"- Since: `{summary.since.isoformat() if summary.since else 'NONE'}`",
        f"- Minimum samples per gate: `{summary.min_samples}`",
        f"- Journal close rows: `{summary.journal_closes}`",
        f"- Known close_order_mode rows: `{summary.known_mode_closes}`",
        f"- Bollinger live close rows: `{summary.bollinger_live_closes}`",
        "",
        "| Gate | Value | Status | Note |",
        "|---|---:|---|---|",
    ]
    for row in summary.rows:
        lines.append(f"| {row.gate} | {row.value} | {row.status} | {row.note} |")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `PENDING` means the code path is instrumented but the local logs do not yet contain enough post-change live close samples.",
            "- `unknown` close order modes are excluded from LIMIT-share statistics because they predate mode persistence.",
            "- Re-run this report after live trading has produced enough close rows.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify post-change live exit gates.")
    parser.add_argument("--days", type=int, default=30, help="Lookback days. Use 0 for all local history.")
    parser.add_argument("--since", default=None, help="Only count rows at or after this ISO timestamp.")
    parser.add_argument("--min-samples", type=int, default=10)
    parser.add_argument("--coverage-threshold", type=float, default=0.95)
    parser.add_argument("--limit-threshold", type=float, default=0.50)
    parser.add_argument("--max-slippage-bps", type=float, default=3.0)
    parser.add_argument("--signal-close-threshold", type=float, default=0.15)
    parser.add_argument("--max-win-rate-delta", type=float, default=0.10)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--fail-on-fail", action="store_true", help="Exit non-zero on FAIL gates; PENDING remains zero.")
    args = parser.parse_args()

    since = _parse_time(args.since) if args.since else None
    summary = build_summary(
        days=args.days,
        since=since,
        min_samples=max(1, int(args.min_samples)),
        coverage_threshold=max(0.0, min(1.0, float(args.coverage_threshold))),
        limit_threshold=max(0.0, min(1.0, float(args.limit_threshold))),
        max_slippage_bps=max(0.0, float(args.max_slippage_bps)),
        signal_close_threshold=max(0.0, min(1.0, float(args.signal_close_threshold))),
        max_win_rate_delta=max(0.0, min(1.0, float(args.max_win_rate_delta))),
    )
    report = render_markdown(summary)
    if args.output:
        output = args.output
        if not output.is_absolute():
            output = REPO_ROOT / output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(report, encoding="utf-8")
    print(report)
    if args.fail_on_fail and any(row.status == "FAIL" for row in summary.rows):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
