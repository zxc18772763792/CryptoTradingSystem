"""Read-only, offline audit of public tracker statistics and legacy paper books.

Never calls a tracker tick, market API, collector, order API or journal writer.
Outputs counts, arithmetic discrepancies and hashes, not Telegram/X messages.
Usage: project-python scripts/audit_research_trackers.py --output NEW.json
       [--compare PREVIOUS.json]
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

STATE_FILES = [
    "listing_short/tracker.json", "unlock_short/tracker.json", "unlock_short_t7/tracker.json",
    "supply_factor/tracker.json", "xs_reversal/tracker.json", "coinglass_composite/tracker.json",
    "coinglass_composite_wide_eq/tracker.json", "announcement_short/monitor.json",
    "announcement_short/delist.json", "upbit_caution/tracker.json", "upbit_caution_hedged/tracker.json",
    "upbit_krw_listing/tracker.json", "bithumb_caution/tracker.json",
    "bithumb_caution_hedged/tracker.json", "korea_caution_hedged/tracker.json",
]
ROW_KEYS = ("id", "name", "n", "mean_pct", "ci90_pct", "ci_clusters", "open", "waiting", "late",
            "verdict", "tracker_status_label", "updated_at", "error", "metric_kind", "return_basis")
FINANCIAL_KEYS = ("status", "entry_price", "exit_price", "entry_at", "exit_at", "entry_ms", "exit_ms",
                  "funding", "return_pct", "hedged_return_pct", "short_return_pct", "funding_pct",
                  "exit_day_actual", "spread_pct", "long_return_pct", "short_return_pct", "long", "short",
                  "longs", "shorts", "net_per_position_pct", "main", "shadow", "liq_filter")
TERMINAL = {"closed", "stopped", "schedule_changed", "unresolved", "delisted"}


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()


def audit():
    from core.research.tracker_registry import tracker_rows

    started = datetime.now(timezone.utc).isoformat()
    loaded = tracker_rows()
    rows = [{k: row.get(k) for k in ROW_KEYS} for row in loaded]
    out = {"schema_version": 1, "started_at_utc": started, "read_only": True, "network_requests": 0,
           "rows": rows, "states": [], "issues": [], "terminal_financial_hashes": {},
           "limits": ["No historical raw archive completeness or executable fills are certified.",
                      "Frozen legacy rate-sum funding and daily-stop conventions are not changed.",
                      "Live writers may update operational timestamps during this read.",
                      "Pinned journals are read by their own validating summaries; raw private text is not exported."]}
    pooled = defaultdict(set)
    for relative in STATE_FILES:
        path = ROOT / "data/research" / relative
        if not path.exists():
            out["issues"].append({"path": relative, "kind": "missing_state"})
            continue
        raw = path.read_bytes()
        state = json.loads(raw)
        collection = next((k for k in ("trades", "days", "months", "books") if isinstance(state.get(k), dict)), None)
        records = state.get(collection, {})
        part = {"path": relative, "sha256": hashlib.sha256(raw).hexdigest(), "records": len(records),
                "statuses": dict(Counter(str(r.get("status", "nested_books")) for r in records.values())),
                "updated_at": state.get("updated_at"), "terminal_records": 0, "arithmetic_checked": 0}
        for key, r in records.items():
            record_id = f"{relative}:{key}"
            terminal = r.get("status") in TERMINAL or any(
                (r.get(v) or {}).get("status") in TERMINAL for v in ("main", "shadow"))
            if terminal:
                financial = {k: r.get(k) for k in FINANCIAL_KEYS if k in r}
                out["terminal_financial_hashes"][record_id] = digest(financial)
                part["terminal_records"] += 1
            for field in ("return_pct", "hedged_return_pct", "spread_pct", "net_per_position_pct"):
                if r.get(field) is not None and not math.isfinite(float(r[field])):
                    out["issues"].append({"record": record_id, "kind": "nonfinite_return", "field": field})
            if r.get("status") in {"closed", "stopped"} and r.get("exit_price") is not None and r.get("return_pct") is not None:
                cost = .003 if relative.startswith("listing_short/") else .001
                if relative.startswith(("listing_short/", "upbit_", "bithumb_", "korea_caution_")):
                    expected = (1 - float(r["exit_price"]) / float(r["entry_price"]) - cost + float(r["funding"])) * 100
                    part["arithmetic_checked"] += 1
                    if abs(expected - float(r["return_pct"])) > .001:
                        out["issues"].append({"record": record_id, "kind": "legacy_return_arithmetic_mismatch",
                                              "delta_pp": expected - float(r["return_pct"])})
            if relative == "supply_factor/tracker.json" and r.get("status") == "closed":
                missing = sum(not p.get("entry") or not p.get("exit") for leg in ("long", "short")
                              for p in r.get(leg, {}).values())
                if missing:
                    out["issues"].append({"record": record_id, "kind": "supply_closed_with_missing_members", "missing": missing})
            if ("caution" in relative and not r.get("backfilled") and not r.get("late") and r.get("entry_at")):
                pooled[f"{r.get('symbol')}:{r['entry_at']}"].add(relative)
        out["states"].append(part)
    pairs = next((r.get("paired_evaluation") for r in loaded if r["id"] == "xs_reversal"), None) or {}
    out["reversal_paired"] = {k: pairs.get(k) for k in ("n_pairs", "baseline", "shadow", "increment", "sensitivity", "execution_diagnostics")}
    # Per-day execution diagnostics contain public tickers/timing only, no messages.
    out["korea_shared_events"] = [{"symbol_entry": k, "books": sorted(v), "book_count": len(v)}
                                  for k, v in sorted(pooled.items()) if len(v) > 1]
    out["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--compare", type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Output exists; use a new path to preserve prior evidence")
    result = audit()
    if args.compare:
        before = json.loads(args.compare.read_text(encoding="utf-8"))["terminal_financial_hashes"]
        after = result["terminal_financial_hashes"]
        result["comparison"] = {"previous": str(args.compare), "prior_terminal_count": len(before),
            "changed_prior_records": [k for k, v in before.items() if after.get(k) != v],
            "new_terminal_count": len(set(after) - set(before))}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, allow_nan=False)
    print(json.dumps({"trackers": len(result["rows"]), "states": len(result["states"]),
                      "issues": result["issues"], "comparison": result.get("comparison")}, ensure_ascii=True))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
