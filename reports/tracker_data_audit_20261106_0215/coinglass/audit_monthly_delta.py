"""Read-only Nov 6 overnight delta for the three CoinGlass paper trackers."""

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from core.research import coinglass_composite_tracker as tracker  # noqa: E402
from core.research import coinglass_daily_store as store  # noqa: E402


def read(path):
    return json.loads(path.read_text(encoding="utf-8"))


def replay_summary(original, replica):
    return {
        variant: {
            "scored": replica[variant].get("scored"),
            **{
                side: {
                    "saved": len(original[variant][side]),
                    "replica": len(replica[variant].get(side, {})),
                    "matches": len(original[variant][side].keys() & replica[variant].get(side, {}).keys()),
                    "same_scores": sum(
                        row["score"] == replica[variant].get(side, {}).get(symbol, {}).get("score")
                        for symbol, row in original[variant][side].items()
                    ),
                    "dropped": sorted(original[variant][side].keys() - replica[variant].get(side, {}).keys()),
                    "added": sorted(replica[variant].get(side, {}).keys() - original[variant][side].keys()),
                }
                for side in ("longs", "shorts")
            },
        }
        for variant in ("main", "shadow")
    }


def tracker_summary(state):
    days = sorted(state["books"].items())
    open_days = [(key, b) for key, b in days if b.get("main", {}).get("status") == "open"]
    day, last = days[-1]
    next_maturity = None
    if open_days:
        next_day, next_book = open_days[0]
        next_maturity = {"day": next_day, "eligible_from": (
            datetime.fromisoformat(next_book["formed_at"]) + timedelta(days=tracker.HOLD_DAYS)
        ).isoformat()}
    return {
        "updated_at": state["updated_at"],
        "days": len(days),
        "counts": Counter(b.get("main", {}).get("status") or b.get("status") for _, b in days),
        "stopped": state.get("stopped"),
        "latest": {
            "day": day, "formed_at": last.get("formed_at"), "coins": last.get("coins"),
            "main_scored": last.get("main", {}).get("scored"),
            "longs": len(last.get("main", {}).get("longs", {})),
            "shorts": len(last.get("main", {}).get("shorts", {})),
        },
        "next_maturity": next_maturity,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    now = datetime.now(timezone.utc)
    day = store.last_closed_day(now)
    day_key = day.date().isoformat()
    states = {v: tracker.load_state(spec["state_path"]) for v, spec in tracker.VARIANTS.items()}
    monthly = states["monthly"]
    book = monthly["books"][day_key]
    formed = datetime.fromisoformat(book["formed_at"])
    snapshot = read(tracker.MONTHLY_STATE_PATH.parent / f"universes/{book['universe']['month']}.json")
    universe = sorted(snapshot["members"].items())
    assert snapshot["id"] == book["universe"]["id"]
    coins = {perp: (store.load_coin(perp), store.coin_path(perp).stat().st_mtime) for _, perp in universe}
    ready = {perp: c for perp, (c, _) in coins.items() if store.complete(c, day)}
    timely = {perp: c for perp, c in ready.items() if coins[perp][1] <= formed.timestamp()}
    xec, xec_mtime = coins["1000XECUSDT"]
    delta = {
        "captured_at": now.isoformat(),
        "last_closed_utc_day": day_key,
        "collector_status": store.load_status(),
        "trackers": {v: tracker_summary(s) for v, s in states.items()},
        "monthly_universe": {"snapshot_id": snapshot["id"], "size": len(universe),
                             "saved_book_candidates": book["coins"], "partial": book["partial"]},
        "monthly_ready_now": len(ready),
        "monthly_ready_60_day_history": sum(
            not any(store.history_gaps(coin, dataset, day) for dataset in store.DATASETS) for coin in ready.values()
        ),
        "monthly_unchanged_mtime_at_formation": len(timely),
        "monthly_after_formation_mtime": sorted(ready.keys() - timely.keys()),
        "monthly_replay": {
            label: replay_summary(book, tracker.form(monthly, day, formed, dict.fromkeys(subset, 1.0), subset, False))
            for label, subset in (("unchanged_files_only", timely), ("later_full_cache", ready))
        },
        "xec": {
            "in_main": "1000XECUSDT" in {p for _, p in store.universe()},
            "eligible_now": "1000XECUSDT" in ready,
            "in_mtime_at_formation_subset": "1000XECUSDT" in timely,
            "selected_in_saved_book": {
                v: {side: "1000XECUSDT" in book[v][side] for side in ("longs", "shorts")} for v in ("main", "shadow")
            },
            "file_mtime": datetime.fromtimestamp(xec_mtime, timezone.utc).isoformat(),
            "latest_rows": {dataset: xec[dataset][-1] for dataset in store.DATASETS},
            "history_gap_counts": {dataset: len(store.history_gaps(xec, dataset, day)) for dataset in store.DATASETS},
            "next_utc_day_query_symbols_by_dataset": {
                dataset: [params["symbol"] for _, params in store.requests_for(xec, "XEC", "1000XECUSDT", dataset, day + timedelta(days=1))]
                for dataset in store.DATASETS
            },
        },
    }
    args.output.write_text(json.dumps(delta, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
