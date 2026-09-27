"""Unlock effect vs calendar- and age-matched control tokens.

Run scripts/unlock_event_study.py first (it saves closes.parquet, events.csv
and cliff_dates.json under data/research/unlock_study/).

Why a matched control (2026-09-27): the placebo in the event study was biased.
Dates far from any unlock are rare for tokens with monthly cliffs, so for young
tokens they landed right after launch (placebo +11%/30d). Young tokens also
drift down after listing regardless of unlocks, and alts fell vs BTC. Here each
unlock is compared with OTHER study tokens over the SAME calendar window that
are of similar age (+/-60 days) and have no cliff of their own within 45 days.
That removes market moves and the age effect at once; what remains is the
unlock.

Usage: python scripts/unlock_matched_control.py [--min-controls 3]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT = PROJECT_ROOT / "data" / "research" / "unlock_study"
WINDOWS = {"pre30": (-31, -1), "pre7": (-8, -1), "post7": (-1, 7), "post30": (-1, 30), "pre30_post30": (-31, 30)}


def month_boot(values: pd.Series, months: pd.Series, reps: int = 2000) -> tuple:
    frame = pd.DataFrame({"v": values.to_numpy(), "m": months.to_numpy()}).dropna()
    if len(frame) < 5:
        return (np.nan, np.nan)
    groups = [g["v"].to_numpy() for _, g in frame.groupby("m")]
    rng = np.random.default_rng(0)
    means = [np.concatenate([groups[i] for i in rng.integers(0, len(groups), len(groups))]).mean() for _ in range(reps)]
    return tuple(np.percentile(means, [5, 95]))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--min-controls", type=int, default=3)
    parser.add_argument("--age-tolerance", type=int, default=60)
    args = parser.parse_args(argv)

    closes = pd.read_parquet(OUT / "closes.parquet")
    closes.index = pd.to_datetime(closes.index, utc=True)
    first_seen = closes.apply(lambda s: s.first_valid_index())
    cliffs = {t: pd.to_datetime(v, utc=True) for t, v in json.loads((OUT / "cliff_dates.json").read_text(encoding="utf-8")).items()}
    events = pd.read_csv(OUT / "events.csv", parse_dates=["date"])
    events["date"] = pd.to_datetime(events["date"], utc=True)

    rows = []
    for ev in events.itertuples(index=False):
        t0 = ev.date
        if ev.token not in closes or pd.isna(first_seen.get(ev.token)) or t0 - pd.Timedelta(days=31) < first_seen[ev.token]:
            continue  # need a full pre-window of the token's own prices
        age = (t0 - first_seen[ev.token]).days
        controls = []
        for c in closes.columns:
            if c == ev.token or pd.isna(first_seen[c]):
                continue
            if abs((t0 - first_seen[c]).days - age) > args.age_tolerance or t0 - pd.Timedelta(days=31) < first_seen[c]:
                continue
            if any(abs((t0 - d).days) <= 45 for d in cliffs.get(c, [])):
                continue
            controls.append(c)
        if len(controls) < args.min_controls:
            continue
        rec = {"token": ev.token, "date": t0, "size_pct": ev.size_pct, "insider": ev.insider, "age_days": age, "n_controls": len(controls)}
        for name, (a, b) in WINDOWS.items():
            ta, tb = t0 + pd.Timedelta(days=a), t0 + pd.Timedelta(days=b)
            if ta not in closes.index or tb not in closes.index:
                rec[name] = np.nan
                continue
            own = closes.at[tb, ev.token] / closes.at[ta, ev.token] - 1
            ctrl = (closes.loc[tb, controls] / closes.loc[ta, controls] - 1).dropna()
            rec[name] = own - ctrl.mean() if len(ctrl) >= args.min_controls else np.nan
        rows.append(rec)

    R = pd.DataFrame(rows)
    R.to_csv(OUT / "matched_events.csv", index=False)
    print(f"events with >= {args.min_controls} calendar+age-matched controls: {len(R)} on {R['token'].nunique()} tokens "
          f"(median {R['n_controls'].median():.0f} controls each)")

    def report(sub: pd.DataFrame, label: str) -> None:
        if len(sub) < 15:
            return
        months = sub["date"].dt.strftime("%Y-%m")
        print(f"\n== {label}: n={len(sub)} ({sub['token'].nunique()} tokens), median size {sub['size_pct'].median():.1f}%")
        for w in WINDOWS:
            x = sub[w].dropna()
            lo, hi = month_boot(x, months.loc[x.index])
            print(f"  {w:13s} excess vs matched {x.mean() * 100:+6.2f}% (median {x.median() * 100:+6.2f}%, below control {(x < 0).mean():.0%})  90%CI [{lo * 100:+.2f}, {hi * 100:+.2f}]")

    report(R, "all cliff unlocks")
    for lo, hi in ((1, 3), (3, 10), (10, 1e9)):
        report(R[(R["size_pct"] >= lo) & (R["size_pct"] < hi)], f"size {lo}-{hi if hi < 1e9 else 'inf'}%")
    report(R[R["age_days"] <= 365], "token age <= 1 year")
    report(R[R["age_days"] > 365], "token age > 1 year")
    report(R[R["insider"]], "insider/VC blocks")
    report(R[~R["insider"]], "non-insider blocks")
    for year, grp in R.groupby(R["date"].dt.year):
        report(grp, f"year {year}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
