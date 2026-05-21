"""Backfill ``close_reason`` for historical close records.

Many historical close rows in ``strategy_trade_journal.jsonl`` and
``risk_trade_history_*.json`` were written before Phase 0前置 landed
(2026-05-21) and lack an explicit ``close_reason`` field. The Phase 0 audit
already reads with a read-time fallback (see ``scripts/audit_exit_reasons.py``
``_close_reason``), but downstream consumers — UI, performance snapshots,
LLM rationales — need a stable persisted field. This script does that
backfill once: read each file, infer a reason from available signal
metadata / order mode / signal_type, and rewrite atomically.

Inference rules (in priority order):

1. ``close_reason`` already present → keep as-is.
2. ``signal.metadata.close_reason`` present → copy to top-level.
3. ``close_order_mode`` indicates protective close (``limit_first`` or
   ``market_fallback`` with ``protective_close`` action) → infer based on
   close vs entry price direction (best-effort).
4. ``signal_type`` is ``close_long`` / ``close_short`` → ``signal_close``.
5. ``action`` is ``manual_order`` → ``manual_order``.
6. Otherwise → ``unknown_legacy`` (marker so we don't keep trying to backfill).

Each backfilled record gets:
- ``close_reason`` (string)
- ``close_reason_source`` (``"backfill_inferred"`` to distinguish from
  ``"protective"`` / ``"strategy_signal"`` which originate at write time)

Usage:
    python scripts/backfill_close_reasons.py --dry-run            # show what would change
    python scripts/backfill_close_reasons.py --apply              # write changes (backs up originals)
    python scripts/backfill_close_reasons.py --apply --no-backup  # skip .bak files
"""
from __future__ import annotations

import argparse
import json
import shutil
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


REPO_ROOT = Path(__file__).resolve().parents[1]
LIVE_JOURNAL_PATH = REPO_ROOT / "data/cache/live_review/strategy_trade_journal.jsonl"
RISK_HISTORY_PATHS = (
    REPO_ROOT / "data/cache/runtime_state/risk_trade_history_live.json",
    REPO_ROOT / "data/cache/runtime_state/risk_trade_history_paper.json",
)
BACKFILL_MARKER_SOURCE = "backfill_inferred"
UNKNOWN_TERMINAL = "unknown_legacy"


@dataclass
class BackfillStats:
    path: str
    total_close_rows: int = 0
    already_had_reason: int = 0
    inferred: Counter = field(default_factory=Counter)
    skipped_unknown: int = 0
    rewrote: bool = False


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def _read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError:
        return None


def _is_close_row(row: Dict[str, Any]) -> bool:
    action = str(row.get("action") or "").strip().lower()
    if action in {"close", "protective_close"}:
        return True
    if action == "manual_order":
        # Could be a close via close_only flag; check signal_type too.
        signal_type = str(
            row.get("signal_type")
            or ((row.get("signal") or {}).get("signal_type") if isinstance(row.get("signal"), dict) else "")
        ).lower()
        if signal_type in {"close_long", "close_short"}:
            return True
        # If reduce_only / close_only flag present, still treat as close.
        metadata = row.get("metadata") or {}
        if isinstance(row.get("signal"), dict):
            metadata = {**((row["signal"].get("metadata")) or {}), **metadata}
        if metadata.get("reduce_only") or metadata.get("close_only"):
            return True
    return False


def _signal_metadata(row: Dict[str, Any]) -> Dict[str, Any]:
    signal = row.get("signal")
    if isinstance(signal, dict):
        meta = signal.get("metadata")
        if isinstance(meta, dict):
            return meta
    return {}


def _signal_type(row: Dict[str, Any]) -> str:
    signal = row.get("signal")
    sig_type = row.get("signal_type") or (signal.get("signal_type") if isinstance(signal, dict) else "")
    return str(sig_type or "").lower()


def infer_close_reason(row: Dict[str, Any]) -> Optional[str]:
    """Return inferred close_reason or None when we cannot reliably infer.

    Rules ordered most-specific to least-specific. None means "leave alone";
    UNKNOWN_TERMINAL means "definitively unknown" and we tag with that so
    future passes don't retry.
    """
    # 1. Already set at top-level.
    existing = str(row.get("close_reason") or "").strip()
    if existing:
        return None  # nothing to do

    # 2. Nested in signal.metadata.
    sm = _signal_metadata(row)
    nested = str(sm.get("close_reason") or "").strip()
    if nested:
        return nested

    # 3. Protective close action explicit.
    action = str(row.get("action") or "").strip().lower()
    if action == "protective_close":
        # Try to refine: if metadata has trigger_reason / triggered_by, use that.
        for k in ("trigger_reason", "triggered_by", "protective_reason"):
            v = sm.get(k) or row.get(k)
            if v:
                return str(v).strip()
        # Fall through to generic protective tag.
        return "protective_close_legacy"

    # 4. close_order_mode hint.
    mode = str(row.get("close_order_mode") or sm.get("close_order_mode") or "").strip().lower()
    if mode in {"limit_first", "market_fallback", "limit_first_partial"}:
        # Could be from strategy signal or protective; default to signal-driven.
        st = _signal_type(row)
        if st in {"close_long", "close_short"}:
            return "signal_close_legacy"
        return "exit_template_legacy"

    # 5. Signal type strongly implies a strategy-driven close.
    st = _signal_type(row)
    if st in {"close_long", "close_short"}:
        return "signal_close_legacy"

    # 6. Manual order.
    if action == "manual_order":
        return "manual_order"

    # 7. Definitively unknown after exhausting hints.
    return UNKNOWN_TERMINAL


def _apply_backfill(rows: List[Dict[str, Any]], stats: BackfillStats) -> bool:
    """Mutates rows in place. Returns True if anything changed."""
    changed = False
    for row in rows:
        if not _is_close_row(row):
            continue
        stats.total_close_rows += 1
        if str(row.get("close_reason") or "").strip():
            stats.already_had_reason += 1
            continue
        inferred = infer_close_reason(row)
        if inferred is None:
            stats.already_had_reason += 1
            continue
        if inferred == UNKNOWN_TERMINAL:
            stats.skipped_unknown += 1
        row["close_reason"] = inferred
        row.setdefault("close_reason_source", BACKFILL_MARKER_SOURCE)
        row.setdefault("close_reason_backfilled_at", datetime.now(timezone.utc).isoformat())
        stats.inferred[inferred] += 1
        changed = True
    return changed


def _write_jsonl_atomic(path: Path, rows: List[Dict[str, Any]], *, backup: bool) -> None:
    if backup and path.exists():
        shutil.copy2(path, path.with_suffix(path.suffix + ".bak"))
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False))
            fh.write("\n")
    tmp.replace(path)


def _write_json_atomic(path: Path, payload: Any, *, backup: bool) -> None:
    if backup and path.exists():
        shutil.copy2(path, path.with_suffix(path.suffix + ".bak"))
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
    tmp.replace(path)


def backfill_jsonl(
    path: Path, *, apply: bool, backup: bool
) -> BackfillStats:
    stats = BackfillStats(path=str(path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path))
    rows = _read_jsonl(path)
    if not rows:
        return stats
    changed = _apply_backfill(rows, stats)
    if changed and apply:
        _write_jsonl_atomic(path, rows, backup=backup)
        stats.rewrote = True
    return stats


def backfill_json_history(
    path: Path, *, apply: bool, backup: bool
) -> BackfillStats:
    stats = BackfillStats(path=str(path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path))
    payload = _read_json(path)
    if payload is None:
        return stats

    # risk history is typically either a list of dicts OR a dict containing 'trades' / 'history'.
    rows: Optional[List[Dict[str, Any]]] = None
    list_owner: Optional[Dict[str, Any]] = None
    list_key: Optional[str] = None
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        for key in ("trade_history", "trades", "history", "records", "rows"):
            value = payload.get(key)
            if isinstance(value, list):
                rows = value
                list_owner = payload
                list_key = key
                break

    if rows is None:
        return stats

    changed = _apply_backfill(rows, stats)
    if changed and apply:
        if list_owner is not None and list_key is not None:
            list_owner[list_key] = rows
            _write_json_atomic(path, list_owner, backup=backup)
        else:
            _write_json_atomic(path, rows, backup=backup)
        stats.rewrote = True
    return stats


def _format_stats(stats: BackfillStats) -> str:
    lines = [f"## {stats.path}"]
    lines.append(f"- Close rows seen: {stats.total_close_rows}")
    lines.append(f"- Already had reason: {stats.already_had_reason}")
    lines.append(f"- Backfilled: {sum(stats.inferred.values())}")
    if stats.inferred:
        lines.append("- Inferred reasons:")
        for reason, count in stats.inferred.most_common():
            lines.append(f"    - `{reason}`: {count}")
    if stats.skipped_unknown:
        lines.append(f"- Tagged `{UNKNOWN_TERMINAL}` (no inference possible): {stats.skipped_unknown}")
    if stats.rewrote:
        lines.append("- [WROTE] File rewritten")
    else:
        lines.append("- [SKIP]  No changes written")
    return "\n".join(lines)


def run(*, apply: bool, backup: bool) -> List[BackfillStats]:
    results: List[BackfillStats] = []
    results.append(backfill_jsonl(LIVE_JOURNAL_PATH, apply=apply, backup=backup))
    for path in RISK_HISTORY_PATHS:
        results.append(backfill_json_history(path, apply=apply, backup=backup))
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill close_reason for legacy trade records.")
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--dry-run", action="store_true", help="Compute and print the plan without writing.")
    grp.add_argument("--apply", action="store_true", help="Write back the inferred close_reason values.")
    parser.add_argument("--no-backup", action="store_true", help="Skip creating .bak files when --apply is set.")
    args = parser.parse_args()

    results = run(apply=args.apply, backup=not args.no_backup)
    print(f"# Backfill {'(applied)' if args.apply else '(dry-run)'} at {datetime.now(timezone.utc).isoformat()}\n")
    for stats in results:
        print(_format_stats(stats))
        print()
    total_backfilled = sum(sum(s.inferred.values()) for s in results)
    print(f"**Total backfilled records: {total_backfilled}**")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
