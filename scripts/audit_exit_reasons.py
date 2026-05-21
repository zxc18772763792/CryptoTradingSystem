"""Audit live/backtest exit reason coverage.

Examples:
    python scripts/audit_exit_reasons.py --days 30 --mode live
    python scripts/audit_exit_reasons.py --days 30 --mode live --output reports/exit_audit_baseline_2026-05-20.md
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
LIVE_JOURNAL_PATH = REPO_ROOT / "data/cache/live_review/strategy_trade_journal.jsonl"
RISK_HISTORY_PATHS = (
    REPO_ROOT / "data/cache/runtime_state/risk_trade_history_live.json",
    REPO_ROOT / "data/cache/runtime_state/risk_trade_history_paper.json",
)


@dataclass
class AuditSummary:
    mode: str
    days: int
    generated_at: datetime
    journal_rows: int
    opens: int
    closes: int
    losing_closes: int
    missing_close_reason: int
    open_exit_templates: Counter
    exit_reasons: Counter
    close_order_modes: Counter
    reason_pnl: Dict[str, List[float]]
    risk_history_rows: int
    risk_missing_close_reason: int
    risk_close_order_modes: Counter
    gross_win_net_loss: List[Dict[str, Any]]


def _parse_time(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            text = str(line or "").strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _load_risk_history(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = payload.get("trade_history") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _filter_by_days(rows: Iterable[Dict[str, Any]], days: int, now: datetime) -> List[Dict[str, Any]]:
    if int(days or 0) <= 0:
        return list(rows)
    cutoff = now - timedelta(days=int(days))
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        ts = _parse_time(row.get("timestamp") or row.get("time") or row.get("created_at"))
        if ts is None or ts >= cutoff:
            filtered.append(row)
    return filtered


def _nested_signal_metadata(row: Dict[str, Any]) -> Dict[str, Any]:
    signal = row.get("signal")
    if isinstance(signal, dict):
        metadata = signal.get("metadata")
        if isinstance(metadata, dict):
            return metadata
    return {}


def _close_reason(row: Dict[str, Any]) -> str:
    metadata = _nested_signal_metadata(row)
    reason = str(row.get("close_reason") or metadata.get("close_reason") or "").strip()
    if reason:
        return reason
    signal = row.get("signal")
    signal_type = str(
        row.get("signal_type")
        or (signal.get("signal_type") if isinstance(signal, dict) else "")
    ).lower()
    if signal_type in {"close_long", "close_short"}:
        return "signal_close"
    action = str(row.get("action") or "").strip().lower()
    if action == "manual_order":
        return "manual_order"
    return "unknown"


def audit_live(*, days: int, now: Optional[datetime] = None) -> AuditSummary:
    generated_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    journal_rows = _filter_by_days(_load_jsonl(LIVE_JOURNAL_PATH), days, generated_at)
    opens = [row for row in journal_rows if row.get("action") == "open_or_add"]
    closes = [row for row in journal_rows if row.get("action") == "close"]

    open_exit_templates = Counter(
        str(_nested_signal_metadata(row).get("exit_template") or "NONE")
        for row in opens
    )
    exit_reasons: Counter = Counter()
    close_order_modes: Counter = Counter()
    reason_pnl: Dict[str, List[float]] = defaultdict(list)
    gross_win_net_loss: List[Dict[str, Any]] = []

    for row in closes:
        reason = _close_reason(row)
        exit_reasons[reason] += 1
        close_order_modes[str(row.get("close_order_mode") or "unknown")] += 1
        pnl = float(row.get("net_pnl_usd") if row.get("net_pnl_usd") is not None else row.get("pnl") or 0.0)
        reason_pnl[reason].append(pnl)
        gross = float(row.get("gross_pnl_usd") if row.get("gross_pnl_usd") is not None else row.get("pnl") or 0.0)
        if gross > 0 and pnl < 0:
            gross_win_net_loss.append(
                {
                    "timestamp": row.get("timestamp"),
                    "strategy": row.get("strategy"),
                    "symbol": row.get("symbol"),
                    "gross_pnl_usd": gross,
                    "net_pnl_usd": pnl,
                    "slippage_bps": row.get("slippage_bps"),
                }
            )

    risk_rows: List[Dict[str, Any]] = []
    for path in RISK_HISTORY_PATHS:
        risk_rows.extend(_filter_by_days(_load_risk_history(path), days, generated_at))
    risk_close_like = [
        row
        for row in risk_rows
        if str(row.get("action") or "").strip().lower() in {"manual_order", "close", "protective_close"}
    ]

    return AuditSummary(
        mode="live",
        days=int(days or 0),
        generated_at=generated_at,
        journal_rows=len(journal_rows),
        opens=len(opens),
        closes=len(closes),
        losing_closes=sum(1 for row in closes if float(row.get("pnl") or 0.0) < 0),
        missing_close_reason=sum(1 for row in closes if _close_reason(row) in {"unknown", "signal_close"}),
        open_exit_templates=open_exit_templates,
        exit_reasons=exit_reasons,
        close_order_modes=close_order_modes,
        reason_pnl=dict(reason_pnl),
        risk_history_rows=len(risk_rows),
        risk_missing_close_reason=sum(1 for row in risk_close_like if not row.get("close_reason")),
        risk_close_order_modes=Counter(str(row.get("close_order_mode") or "unknown") for row in risk_close_like),
        gross_win_net_loss=gross_win_net_loss,
    )


def _pct(part: int, total: int) -> str:
    if total <= 0:
        return "0.0%"
    return f"{(part / total) * 100.0:.1f}%"


def render_markdown(summary: AuditSummary) -> str:
    lines: List[str] = [
        f"# Exit Reason Audit Baseline ({summary.generated_at.date().isoformat()})",
        "",
        f"- Mode: `{summary.mode}`",
        f"- Lookback days: `{summary.days}` (`0` means all local history)",
        f"- Generated at: `{summary.generated_at.isoformat()}`",
        "",
        "## Journal Summary",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Journal rows | {summary.journal_rows} |",
        f"| Opens | {summary.opens} |",
        f"| Closes | {summary.closes} |",
        f"| Losing closes | {summary.losing_closes} ({_pct(summary.losing_closes, summary.closes)}) |",
        f"| Close rows missing explicit reason | {summary.missing_close_reason} ({_pct(summary.missing_close_reason, summary.closes)}) |",
        f"| Risk history rows | {summary.risk_history_rows} |",
        f"| Risk manual/close rows missing close_reason | {summary.risk_missing_close_reason} |",
        "",
        "## Exit Reasons",
        "",
        "| Reason | Trades | Share | Avg PnL |",
        "|---|---:|---:|---:|",
    ]
    for reason, count in summary.exit_reasons.most_common():
        pnls = summary.reason_pnl.get(reason, [])
        avg = sum(pnls) / len(pnls) if pnls else 0.0
        lines.append(f"| `{reason}` | {count} | {_pct(count, summary.closes)} | {avg:.6f} |")
    if not summary.exit_reasons:
        lines.append("| `NONE` | 0 | 0.0% | 0.000000 |")

    lines.extend(
        [
            "",
            "## Close Order Modes",
            "",
            "| Mode | Journal Closes | Share | Risk Close Rows |",
            "|---|---:|---:|---:|",
        ]
    )
    modes = set(summary.close_order_modes) | set(summary.risk_close_order_modes)
    for mode in sorted(modes):
        journal_count = int(summary.close_order_modes.get(mode, 0))
        risk_count = int(summary.risk_close_order_modes.get(mode, 0))
        lines.append(f"| `{mode}` | {journal_count} | {_pct(journal_count, summary.closes)} | {risk_count} |")
    if not modes:
        lines.append("| `NONE` | 0 | 0.0% | 0 |")

    lines.extend(
        [
            "",
            "## Open Exit Template Coverage",
            "",
            "| Exit Template | Opens | Share |",
            "|---|---:|---:|",
        ]
    )
    for template, count in summary.open_exit_templates.most_common():
        lines.append(f"| `{template}` | {count} | {_pct(count, summary.opens)} |")
    if not summary.open_exit_templates:
        lines.append("| `NONE` | 0 | 0.0% |")

    lines.extend(
        [
            "",
            "## Cost Anomalies",
            "",
            "| Timestamp | Strategy | Symbol | Gross PnL | Net PnL | Slippage bps |",
            "|---|---|---|---:|---:|---:|",
        ]
    )
    for row in summary.gross_win_net_loss:
        lines.append(
            "| {timestamp} | `{strategy}` | `{symbol}` | {gross:.6f} | {net:.6f} | {slip} |".format(
                timestamp=row.get("timestamp") or "",
                strategy=row.get("strategy") or "",
                symbol=row.get("symbol") or "",
                gross=float(row.get("gross_pnl_usd") or 0.0),
                net=float(row.get("net_pnl_usd") or 0.0),
                slip="" if row.get("slippage_bps") is None else f"{float(row.get('slippage_bps')):.4f}",
            )
        )
    if not summary.gross_win_net_loss:
        lines.append("| - | - | - | 0.000000 | 0.000000 | - |")

    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `signal_close` means the journal only showed `close_long`/`close_short`; it did not preserve a more specific close reason.",
            "- `unknown` means no usable close reason could be inferred.",
            "- `unknown` under Close Order Modes means the row predates mode persistence or came from a path that did not record execution mode.",
            "- Risk history reason gaps should shrink after execution paths persist `close_reason`.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit exit reason distribution from local trading logs.")
    parser.add_argument("--days", type=int, default=30, help="Lookback days. Use 0 for all local history.")
    parser.add_argument("--mode", choices=["live", "backtest"], default="live")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    if args.mode != "live":
        raise SystemExit("backtest mode is not wired yet; run live mode for the current baseline")

    summary = audit_live(days=args.days)
    report = render_markdown(summary)
    if args.output:
        output = args.output
        if not output.is_absolute():
            output = REPO_ROOT / output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(report, encoding="utf-8")
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
