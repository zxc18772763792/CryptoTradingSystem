"""Compatibility audit for close_reason coverage.

The exit overhaul plan originally referenced this script name. The canonical
distribution report now lives in ``scripts/audit_exit_reasons.py``; this wrapper
keeps the original acceptance command available and focuses on coverage gates.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import audit_exit_reasons as exit_audit


@dataclass
class CoverageSummary:
    days: int
    generated_at: datetime
    since: Optional[datetime]
    journal_closes: int
    journal_missing_close_reason: int
    journal_coverage: float
    risk_close_rows: int
    risk_missing_close_reason: int
    risk_coverage: float
    vwap_close_rows: int
    vwap_reason_rows: int
    vwap_sell_misclassified_rows: int
    long_invalid_take_profit_rows: int


def _is_vwap_row(row: Dict[str, Any]) -> bool:
    strategy = str(row.get("strategy") or row.get("strategy_name") or "").lower()
    signal = row.get("signal")
    if isinstance(signal, dict):
        strategy = f"{strategy} {str(signal.get('strategy_name') or '').lower()}"
    return "vwap" in strategy and "reversion" in strategy


def _signal_type(row: Dict[str, Any]) -> str:
    signal = row.get("signal")
    return str(
        row.get("signal_type")
        or (signal.get("signal_type") if isinstance(signal, dict) else "")
        or ""
    ).strip().lower()


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return default


def _parse_time(value: Any) -> Optional[datetime]:
    return exit_audit._parse_time(value)


def _filter_since(rows: List[Dict[str, Any]], since: Optional[datetime]) -> List[Dict[str, Any]]:
    if since is None:
        return rows
    cutoff = since.astimezone(timezone.utc)
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        ts = _parse_time(row.get("timestamp") or row.get("time") or row.get("created_at"))
        if ts is None or ts >= cutoff:
            filtered.append(row)
    return filtered


def _count_invalid_long_take_profit(rows: List[Dict[str, Any]]) -> int:
    invalid = 0
    for row in rows:
        side = str(row.get("side") or row.get("position_side") or "").lower()
        if side not in {"long", "buy"}:
            continue
        entry = _safe_float(row.get("entry_price") or row.get("fill_price"))
        take_profit = _safe_float(row.get("take_profit"))
        if entry is not None and take_profit is not None and take_profit < entry:
            invalid += 1
    return invalid


def audit_close_reason_coverage(
    *,
    days: int,
    now: Optional[datetime] = None,
    since: Optional[datetime] = None,
) -> CoverageSummary:
    generated_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    resolved_since = since.astimezone(timezone.utc) if since is not None else None
    journal_rows = _filter_since(
        exit_audit._filter_by_days(
            exit_audit._load_jsonl(exit_audit.LIVE_JOURNAL_PATH),
            days,
            generated_at,
        ),
        resolved_since,
    )
    close_rows = [row for row in journal_rows if row.get("action") == "close"]
    journal_missing = sum(
        1
        for row in close_rows
        if exit_audit._close_reason(row) in {"unknown", "signal_close"}
    )
    vwap_rows = [row for row in close_rows if _is_vwap_row(row)]
    vwap_reason_rows = [
        row
        for row in vwap_rows
        if exit_audit._close_reason(row) == "vwap_mean_reversion_completed"
    ]
    vwap_sell_misclassified = [
        row
        for row in vwap_rows
        if _signal_type(row) == "sell"
    ]

    risk_rows: List[Dict[str, Any]] = []
    for path in exit_audit.RISK_HISTORY_PATHS:
        risk_rows.extend(
            exit_audit._filter_by_days(
                exit_audit._load_risk_history(path),
                days,
                generated_at,
            )
        )
    risk_rows = _filter_since(risk_rows, resolved_since)
    risk_close_like = [
        row
        for row in risk_rows
        if str(row.get("action") or "").strip().lower() in {"manual_order", "close", "protective_close"}
    ]
    risk_missing = sum(1 for row in risk_close_like if not row.get("close_reason"))
    journal_coverage = (
        (len(close_rows) - journal_missing) / len(close_rows)
        if close_rows
        else 0.0
    )
    risk_coverage = (
        (len(risk_close_like) - risk_missing) / len(risk_close_like)
        if risk_close_like
        else 0.0
    )

    return CoverageSummary(
        days=int(days or 0),
        generated_at=generated_at,
        since=resolved_since,
        journal_closes=len(close_rows),
        journal_missing_close_reason=journal_missing,
        journal_coverage=float(journal_coverage),
        risk_close_rows=len(risk_close_like),
        risk_missing_close_reason=risk_missing,
        risk_coverage=float(risk_coverage),
        vwap_close_rows=len(vwap_rows),
        vwap_reason_rows=len(vwap_reason_rows),
        vwap_sell_misclassified_rows=len(vwap_sell_misclassified),
        long_invalid_take_profit_rows=_count_invalid_long_take_profit(risk_rows),
    )


def _pct(value: float) -> str:
    return f"{float(value) * 100.0:.1f}%"


def render_markdown(summary: CoverageSummary, *, threshold: float = 0.95) -> str:
    journal_pass = summary.journal_closes > 0 and summary.journal_coverage >= threshold
    risk_pass = summary.risk_close_rows > 0 and summary.risk_coverage >= threshold
    vwap_pass = (
        summary.vwap_close_rows == 0
        or (
            summary.vwap_reason_rows == summary.vwap_close_rows
            and summary.vwap_sell_misclassified_rows == 0
        )
    )
    protection_pass = summary.long_invalid_take_profit_rows == 0
    lines = [
        f"# Close Reason Coverage Audit ({summary.generated_at.date().isoformat()})",
        "",
        f"- Lookback days: `{summary.days}` (`0` means all local history)",
        f"- Since: `{summary.since.isoformat() if summary.since else 'NONE'}`",
        f"- Coverage threshold: `{_pct(threshold)}`",
        "",
        "| Gate | Value | Status |",
        "|---|---:|---|",
        f"| Journal close_reason coverage | {_pct(summary.journal_coverage)} ({summary.journal_closes - summary.journal_missing_close_reason}/{summary.journal_closes}) | {'PASS' if journal_pass else 'PENDING'} |",
        f"| Risk close_reason coverage | {_pct(summary.risk_coverage)} ({summary.risk_close_rows - summary.risk_missing_close_reason}/{summary.risk_close_rows}) | {'PASS' if risk_pass else 'PENDING'} |",
        f"| VWAP close reason rows | {summary.vwap_reason_rows}/{summary.vwap_close_rows} | {'PASS' if vwap_pass else 'FAIL'} |",
        f"| VWAP misclassified SELL close rows | {summary.vwap_sell_misclassified_rows} | {'PASS' if summary.vwap_sell_misclassified_rows == 0 else 'FAIL'} |",
        f"| Invalid LONG take_profit below entry | {summary.long_invalid_take_profit_rows} | {'PASS' if protection_pass else 'FAIL'} |",
        "",
        "## Notes",
        "",
        "- `PENDING` means the local lookback window does not yet contain enough post-change close rows to prove the target.",
        "- The detailed exit reason and close order mode distribution is available from `scripts/audit_exit_reasons.py`.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit close_reason coverage from local trading logs.")
    parser.add_argument("--days", type=int, default=30, help="Lookback days. Use 0 for all local history.")
    parser.add_argument("--threshold", type=float, default=0.95, help="Coverage threshold as a ratio.")
    parser.add_argument("--since", default=None, help="Only count rows at or after this ISO timestamp.")
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--fail-under", action="store_true", help="Exit non-zero when coverage gates fail.")
    args = parser.parse_args()

    since = _parse_time(args.since) if args.since else None
    summary = audit_close_reason_coverage(days=args.days, since=since)
    report = render_markdown(summary, threshold=max(0.0, min(1.0, float(args.threshold))))
    if args.output:
        output = args.output
        if not output.is_absolute():
            output = exit_audit.REPO_ROOT / output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(report, encoding="utf-8")
    print(report)

    if args.fail_under:
        hard_fail = (
            summary.long_invalid_take_profit_rows > 0
            or summary.vwap_sell_misclassified_rows > 0
            or (
                summary.journal_closes > 0
                and summary.journal_coverage < float(args.threshold)
            )
            or (
                summary.risk_close_rows > 0
                and summary.risk_coverage < float(args.threshold)
            )
        )
        return 1 if hard_fail else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
