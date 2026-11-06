"""Read-only replay of the local signal cache. No collector or tracker ticks."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import json
import math
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from core.research import coinglass_composite_tracker as tracker  # noqa: E402
from core.research import coinglass_daily_store as store  # noqa: E402


def read_json(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as stream:
        return json.load(stream)


def compare_book(state, day, formed, saved, coins):
    reproduced = tracker.form(state, day, formed, dict.fromkeys(coins, 1.0), coins, False)["main"]
    out = {"input_coins": len(coins), "scored": reproduced.get("scored")}
    for side in ("longs", "shorts"):
        old = saved[side]
        new = reproduced.get(side, {})
        overlap = old.keys() & new.keys()
        errors = {p: round(new[p]["score"] - old[p]["score"], 5) for p in overlap}
        out[side] = {
            "saved": len(old), "replayed": len(new), "overlap": len(overlap),
            "exact_score": sum(v == 0 for v in errors.values()),
            "max_abs_score_delta": max(map(abs, errors.values()), default=None),
            "dropped": sorted(old.keys() - new.keys()), "added": sorted(new.keys() - old.keys()),
            "largest_score_deltas": sorted(errors.items(), key=lambda row: abs(row[1]), reverse=True)[:4],
        }
    return out


def analyze_book(name, state, universe, observations, count):
    reports = []
    for key, book in reversed(sorted(state["books"].items())):
        saved = book.get("main", {})
        if "longs" not in saved or len(reports) >= count:
            continue
        day = datetime.fromisoformat(key).replace(tzinfo=timezone.utc)
        formed = datetime.fromisoformat(book["formed_at"])
        members = {perp: observations.get(perp, ({}, math.inf)) for _, perp in universe}
        ready = {p: c for p, (c, _) in members.items() if store.complete(c, day)}
        timely = {p: c for p, c in ready.items() if members[p][1] <= formed.timestamp()}
        selected = saved["longs"].keys() | saved["shorts"].keys()
        coverage = {}
        for dataset in store.DATASETS:
            cutoff = store.needed_stamp(dataset, day)
            by_symbol = {p: {int(row["time"]): row for row in c.get(dataset, ())} for p, (c, _) in members.items()}
            exact = {p for p, rows in by_symbol.items() if cutoff in rows}
            valid = {p for p in exact if store.valid_history_row(dataset, by_symbol[p][cutoff])}
            clean_window = {p for p, rows in by_symbol.items() if all(
                t in rows and store.valid_history_row(dataset, rows[t])
                for t in range(cutoff - 30 * store.DAY_MS, cutoff + 1, store.DAY_MS)
            )}
            coverage[dataset] = {
                "exact": len(exact), "valid": len(valid), "valid_31_days": len(clean_window),
                "no_exact_selected": sorted(selected - exact),
                "invalid_exact": sorted(exact - valid),
                "latest_check_passes_but_exact_missing": sorted(ready.keys() - exact),
                "ready_missing_31_day_history": sorted(ready.keys() - clean_window),
            }
        reports.append({
            "tracker": name, "signal_day": key, "formed_at": formed.isoformat(), "universe": len(members),
            "saved_coins": book["coins"], "saved_scored": saved.get("scored"),
            "saved_selected_cache_mtime_after_formation": len(selected - timely.keys()),
            "coverage": coverage,
            "current_cache_replay": compare_book(state, day, formed, saved, ready),
            "mtime_at_formation_replay": compare_book(state, day, formed, saved, timely),
        })
    return reports


def profile(observations, bases):
    root = ROOT / "data/research"
    report = {}
    for source in ("coinglass_positioning", "coinglass_wide", "coinglass_history"):
        comparable = Counter()
        mismatches = Counter()
        examples = {}
        for perp in bases:
            suffix = ".json.gz" if source == "coinglass_history" else ".json"
            path = root / source / "raw" / f"{perp}{suffix}"
            if not path.is_file():
                continue
            raw = read_json(path)
            raw_data = raw.get("datasets", raw)
            for dataset, (_, _, fields) in store.DATASETS.items():
                observed = {r["time"]: r for r in observations[perp][0].get(dataset, ())}
                key = "top_position" if dataset == "top_ratio" and "top_position" in raw_data else dataset
                for row in raw_data.get(key, ()):
                    stamp = int(row["time"])
                    if stamp not in observed:
                        continue
                    comparable[dataset] += 1
                    same = all(str(observed[stamp].get(f)) == str(row.get(f)) for f in fields)
                    if same:
                        continue
                    mismatches[dataset] += 1
                    if len(examples.setdefault(dataset, [])) < 4:
                        examples[dataset].append((perp, stamp, observed[stamp], {f: row.get(f) for f in fields}))
        report[source] = {"same_stamp_rows": comparable, "mismatching_rows": mismatches, "examples": examples}
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    monthly = read_json(tracker.MONTHLY_STATE_PATH.parent / "universes/2026-11.json")
    universes = {"main": store.universe(), "monthly": sorted(monthly["members"].items())}
    observations = {}
    for perp in {p for u in universes.values() for _, p in u}:
        path = store.coin_path(perp)
        if path.is_file():
            observations[perp] = read_json(path), path.stat().st_mtime
    output = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "collector_status": {k: v for k, v in store.load_status().items() if not isinstance(v, (dict, list))},
        "books": [
            book for name, universe in universes.items()
            for book in analyze_book(name, tracker.load_state(tracker.VARIANTS[name]["state_path"]), universe, observations, 2)
        ],
        "overlapping_archive_comparison_main_universe": profile(observations, dict.fromkeys(p for _, p in universes["main"])),
    }
    args.output.write_text(json.dumps(output, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
