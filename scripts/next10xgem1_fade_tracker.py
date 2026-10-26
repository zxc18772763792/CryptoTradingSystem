"""Manage the next10xgem1 24h fade paper ledger (records only; no orders, accounts or AI calls).

    status      journal counts, collector health, gate (no network)
    selftest    frozen labels over the study's 180-day history against the pre-registered profile (no network)
    clock       bounded Binance server-time reads (public)
    activate    explicit registration; needs a healthy collector, a good clock and a matching label profile
    tick        one pass of what the web task does every 15 seconds
    verify      full re-read of the hash chain
    report      the pre-registered pass-rule statistics (descriptive until 490 completed events)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.news.alastar_observation import _utc  # noqa: E402
from core.research import next10xgem1_fade_tracker as fade  # noqa: E402
from scripts.observe_cryptoquant_alert import block_bootstrap_lower  # noqa: E402


def report(journal: fade.Journal) -> dict:
    records, events = journal._state()
    if not records:
        return {"activated": False}
    done = fade.completed(events)
    by_week: dict = {}
    for e in done:
        by_week.setdefault(_utc(e["entry"]["entry_at"]).strftime("%G-W%V"), []).append(fade.excess(e))
    values = sorted(fade.excess(e) for e in done)
    funded = [e["settlement"]["books"]["plain"].get("net_with_funding") for e in done]
    funded = [v for v in funded if v is not None]
    stress = [e["settlement"]["books"]["plain"]["net_price_only_stress"] for e in done]
    lower = block_bootstrap_lower(by_week)
    checks = {
        "weekly_block_lower_95_excess_pp": None if lower is None else 100 * lower,
        "mean_funded_net_pct": 100 * sum(funded) / len(funded) if funded else None,
        "funded_coverage": f"{len(funded)}/{len(done)}",
        "mean_excess_without_best_pp": 100 * sum(values[:-1]) / (len(values) - 1) if len(values) > 1 else None,
        "mean_net_80bps_pct": 100 * sum(stress) / len(stress) if stress else None,
    }
    passed = (len(done) >= fade.TARGET_N and lower is not None and lower > 0 and bool(funded)
              and checks["mean_funded_net_pct"] > 0 and checks["mean_excess_without_best_pp"] > 0
              and checks["mean_net_80bps_pct"] > 0)
    return {"activated_at_utc": records[0]["recorded_at_utc"], "gate": journal.gate(records, events),
            "events": len(events), "primary_completed": len(done), "target": fade.TARGET_N, "calendar_weeks": len(by_week),
            "mean_excess_pp": 100 * sum(values) / len(values) if values else None, "pass_rule_checks": checks,
            "passes_pre_registered_rule": passed if len(done) >= fade.TARGET_N else None,
            "books": fade.summary(journal)["books"], "paper_only": True,
            "note": f"Descriptive until {fade.TARGET_N} completed primary events; promotion_allowed is always false."}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=("status", "selftest", "clock", "activate", "tick", "verify", "report"))
    parser.add_argument("--output-dir", type=Path, default=fade.OUTPUT)
    parser.add_argument("--archive-dir", type=Path)
    parser.add_argument("--protocol", type=Path, default=fade.PROTOCOL)
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")
    journal = fade.Journal(args.output_dir, args.archive_dir, args.protocol)
    if args.action == "status":
        result = {**fade.summary(journal), "health": journal.health(), "protocol_sha256": journal.protocol_sha,
                  "implementation_sha256": journal.implementation_sha}
    elif args.action == "selftest":
        profile = fade.development_profile()
        expected = journal.protocol["labels"]["development_sample"]
        result = {"profile": profile, "matches_protocol": profile["history_sha256"] == expected["history_sha256"]
                  and profile["label_counts"] == expected["label_counts"]}
    elif args.action == "clock":
        result = fade.PublicMarketData(args.output_dir, max_calls=8).clock_check()
    elif args.action == "activate":
        clock = fade.PublicMarketData(args.output_dir, max_calls=8).clock_check()
        result = journal.activate(clock)
    elif args.action == "tick":
        result = fade.Tracker(journal).tick()
    elif args.action == "verify":
        result = journal.verify()
    else:
        result = report(journal)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
