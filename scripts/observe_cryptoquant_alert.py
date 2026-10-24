"""Manage the CryptoQuant Alert BTC-flow shadow ledger (records only; no orders, accounts or AI calls).

    status      journal counts, collector health, gate (no network)
    selftest    parser over the development sample against the pre-registered profile (no network)
    clock       bounded Binance server-time reads (public)
    activate    explicit registration; needs a healthy collector, a good clock and a matching parser profile
    tick        one pass of what the web task does every 15 seconds
    verify      full re-read of the hash chain
    report      the pre-registered statistics (descriptive until the 66-event review)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.news.alastar_observation import _utc  # noqa: E402
from core.research import cryptoquant_alert_shadow as cq  # noqa: E402


def block_bootstrap_lower(values_by_week, resamples=5000, seed=20261024, level=0.95):
    """One-sided lower bound of the mean from resampling whole calendar weeks."""
    import numpy as np

    weeks = [np.asarray(v, dtype=float) for v in values_by_week.values() if len(v)]
    if len(weeks) < 8:
        return None
    rng = np.random.default_rng(seed)
    means = []
    for _ in range(resamples):
        picked = [weeks[i] for i in rng.integers(0, len(weeks), len(weeks))]
        means.append(np.concatenate(picked).mean())
    return float(np.quantile(means, 1 - level))


def report(journal: cq.Journal) -> dict:
    records, events = journal._state()
    if not records:
        return {"activated": False}
    done = [e for e in events.values() if e["status"] == "completed" and e.get("primary_sample")
            and (e["null"] or {}).get("horizons", {}).get(str(cq.PRIMARY_HOURS), {}).get("status") == "ok"]
    out = {"activated_at_utc": records[0]["recorded_at_utc"], "gate": journal.gate(records, events),
           "events": len(events), "primary_sample_completed": len(done), "paper_only": True,
           "note": "Descriptive until 66 primary-sample completed events; promotion_allowed is always false."}
    by_week = {}
    for e in done:
        week = _utc(e["entry"]["entry_at"]).strftime("%G-W%V")
        by_week.setdefault(week, []).append(cq.excess(e, cq.PRIMARY_HOURS))
    values = [x for week in by_week.values() for x in week]
    out["primary"] = {"horizon_hours": cq.PRIMARY_HOURS, "n": len(values), "calendar_weeks": len(by_week),
                      "mean_excess_pp": 100 * sum(values) / len(values) if values else None,
                      "mean_net_stress_40bps_pct": (100 * sum(e["outcomes"][str(cq.PRIMARY_HOURS)]["net_price_only_40bps"] for e in done) / len(done))
                      if done else None,
                      "weekly_block_lower_95_pp": (lambda b: None if b is None else 100 * b)(block_bootstrap_lower(by_week))}
    rows = []
    for hours in cq.HOURS:
        mature = [e for e in events.values() if e.get("primary_sample") and str(hours) in e["outcomes"]]
        usable = [e for e in mature if e["null"] and e["null"]["horizons"][str(hours)]["status"] == "ok"]
        rows.append({"hours": hours, "n": len(mature), "mean_net_pct": 100 * sum(e["outcomes"][str(hours)]["net_price_only"] for e in mature) / len(mature) if mature else None,
                     "n_with_control": len(usable),
                     "mean_excess_pp": 100 * sum(cq.excess(e, hours) for e in usable) / len(usable) if usable else None})
    out["by_horizon_descriptive"] = rows
    templates = {}
    for e in events.values():
        templates[e["alert"]["template"]] = templates.get(e["alert"]["template"], 0) + 1
    out["events_by_template"] = templates
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=("status", "selftest", "clock", "activate", "tick", "verify", "report"))
    parser.add_argument("--output-dir", type=Path, default=cq.OUTPUT)
    parser.add_argument("--archive-dir", type=Path)
    parser.add_argument("--protocol", type=Path, default=cq.PROTOCOL)
    args = parser.parse_args()
    journal = cq.Journal(args.output_dir, args.archive_dir, args.protocol)
    if args.action == "selftest":
        profile = cq.development_sample_profile()
        expected = journal.protocol.get("development_sample") or {}
        result = {"profile": profile, "matches_protocol": profile == expected}
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if result["matches_protocol"] else 1
    if args.action == "status":
        records, events = journal._state()
        result = {"protocol_sha256": journal.protocol_sha, "implementation_sha256": journal.implementation_sha,
                  "journal_records": len(records), "activated": bool(records), "gate": journal.gate(records, events),
                  "collector_health": journal.health(), "summary": cq.summary(journal) if records else None}
    elif args.action == "verify":
        result = journal.verify()
    elif args.action == "report":
        result = report(journal)
    elif args.action in ("clock", "activate"):
        clock = cq.PublicMarketData(args.output_dir).clock_check()
        if args.action == "clock":
            result = clock
        else:
            if not clock["ok"]:
                raise SystemExit("Clock/round-trip check failed; shadow activation stopped")
            result = journal.activate(clock)
    else:
        result = cq.Tracker(journal, lambda: cq.PublicMarketData(args.output_dir)).tick()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
