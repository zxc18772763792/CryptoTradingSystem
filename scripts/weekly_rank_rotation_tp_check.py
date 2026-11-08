"""Is "long rank #1, sell at +40%, no stop, re-rank every Monday" real, or a grid-search accident?

The rule came out of a 16-cell exit grid in weekly_rank_rotation_followup.py, picked on PRE, so PRE
no longer counts as a clean test. Bars fixed before these checks were run (2026-11-07):

  A. TRAIN+POST (not used to pick it): beats a random coin from the top decile of 30-day volatility
     under the same exit rule, one-sided p < 0.05. High-volatility coins reach +40% more often for
     no reason related to the model, so this is the fair control, not the whole universe.
  B. Other start days: mean net > 0 on at least 6 of the 7 weekdays in PRE. A real effect should
     not care which weekday the list is built on.
  C. Rank order: ranks 1-3 beat ranks 16-30 under the rule, in PRE and in TRAIN+POST.
  D. Harsher fills: stays positive in PRE and in TRAIN+POST when the take-profit must trade 1%
     through the limit and each round trip costs 0.5%.

Exit proxy: if the week's high reaches the limit the position sells at the limit, else at the next
Monday 01:00 open. Gap fills above the limit are ignored, which is conservative. Funding is charged
for the full week, also conservative for longs when funding is positive.

Usage: python scripts/weekly_rank_rotation_tp_check.py <the study's output dir>
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import weekly_rank_rotation_study as study  # noqa: E402

TP = 0.40


def rule_return(df: pd.DataFrame, tp: float = TP, through: float = 0.0) -> pd.Series:
    hit = df["maxup"] >= tp * (1 + through) + through
    return pd.Series(np.where(hit, tp, df["ret"]), index=df.index, dtype=float) - df["fund"]


def stats(values) -> dict:
    n = np.asarray(values, dtype=float)
    n = n[np.isfinite(n)]
    if len(n) < 3:
        return {"n": len(n)}
    eq = np.cumprod(1 + np.maximum(n, -1.0))
    return {"n": len(n), "mean%": round(100 * n.mean(), 2), "median%": round(100 * float(np.median(n)), 2), "win": round(float((n > 0).mean()), 2),
            "t": round(float(n.mean() / (n.std(ddof=1) / math.sqrt(len(n)))), 2), "worst%": round(100 * n.min(), 1),
            "cagr": round(float(eq[-1] ** (52 / len(n)) - 1), 2) if eq[-1] > 0 else -1.0,
            "maxdd": round(float((eq / np.maximum.accumulate(eq) - 1).min()), 2)}


def main() -> None:
    out = Path(sys.argv[1])
    model = study.load_model_weights()
    universe = pd.read_parquet(study.PANEL / "universe.parquet") > 0.5
    features = study.build_features()
    study.STORE_END = pd.Timestamp(universe.index[-1]) + pd.Timedelta(days=1)
    cw = pd.read_parquet(out / "coin_weeks.parquet")
    pd.set_option("display.width", 200, "display.max_columns", 30, "display.max_rows", 100)

    panel, dropped = {}, 0
    for m in pd.date_range(study.START, universe.index[-1], freq="W-MON"):
        day = m - pd.Timedelta(days=1)
        wk = study.score_week(features, model, day, universe.loc[day], "drop")
        joined = wk.join(cw[cw["monday"] == m].set_index("perp"), how="inner")
        if len(joined) >= 50:
            panel[m] = joined
            dropped += int(wk.index[0] not in joined.index)
    weeks = sorted(panel)
    period = {m: study.period_of(m) for m in weeks}
    pre = [m for m in weeks if period[m] == "PRE"]
    later = [m for m in weeks if period[m] != "PRE"]
    print(f"weeks {len(weeks)}; weeks whose true #1 had no complete 7-day window and was skipped: {dropped}")

    one = pd.Series({m: float(rule_return(panel[m].nsmallest(1, "rank")).iloc[0]) - study.COST_RT for m in weeks})
    uni_hold = pd.Series({m: float((panel[m]["ret"] - panel[m]["fund"]).mean()) for m in weeks})
    print("\n== The rule, Mondays, net of 0.16%")
    for name, wks in (("PRE", pre), ("TRAIN", [m for m in weeks if period[m] == "TRAIN"]), ("POST", [m for m in weeks if period[m] == "POST"]), ("TRAIN+POST", later), ("ALL", weeks)):
        print(f"{name:10s}", stats(one.loc[wks]), "universe hold mean% ", round(100 * uni_hold.loc[wks].mean(), 2))
    print("by year:", (one.groupby(one.index.year).mean() * 100).round(2).to_dict())
    hits = pd.Series({m: bool(panel[m].nsmallest(1, "rank")["maxup"].iloc[0] >= TP) for m in weeks})
    print("limit reached in", f"{hits.mean():.0%} of weeks;", "when missed, mean", f"{100 * one[~hits].mean():+.1f}%", "median", f"{100 * one[~hits].median():+.1f}%")

    print("\n== A. Against random picks under the same rule (one-sided p of the mean)")
    rng = np.random.default_rng(23)
    for label, wks in (("PRE", pre), ("TRAIN+POST", later)):
        for ctrl in ("universe", "top-decile 30d vol"):
            total = np.zeros(5000)
            for m in wks:
                df = panel[m]
                if ctrl != "universe":
                    df = df[df["vola_30d"] >= df["vola_30d"].quantile(0.9)]
                arr = rule_return(df).to_numpy(dtype=float)
                arr = arr[np.isfinite(arr)]
                total += arr[rng.integers(0, len(arr), size=5000)] - study.COST_RT
            obs = one.loc[wks].mean()
            print(f"{label:10s} vs {ctrl:20s}: rule {100 * obs:+.2f}% control {100 * (total / len(wks)).mean():+.2f}% p={(np.sum(total / len(wks) >= obs) + 1) / 5001:.4f}")

    print("\n== C. Rank order under the rule, mean net %/week")
    bands = [(1, 1), (2, 2), (3, 3), (4, 5), (6, 10), (11, 15), (16, 30), (31, 60), (61, 130)]
    rows = []
    for a, b in bands:
        rec = {"ranks": f"{a}-{b}"}
        for label, wks in (("PRE", pre), ("TRAIN+POST", later)):
            vals = [float(rule_return(panel[m][(panel[m]["rank"] >= a) & (panel[m]["rank"] <= b)]).mean()) - study.COST_RT for m in wks]
            rec[label] = round(100 * np.nanmean(vals), 2)
            rec[label + "_hit"] = round(float(np.nanmean([(panel[m][(panel[m]["rank"] >= a) & (panel[m]["rank"] <= b)]["maxup"] >= TP).mean() for m in wks])), 2)
        rows.append(rec)
    print(pd.DataFrame(rows).to_string(index=False))

    print("\n== Take-profit level for #1, mean net %/week (t)")
    for tp in (0.2, 0.3, 0.4, 0.5, 0.6, 0.8, 1.0, 2.0):
        vals = pd.Series({m: float(rule_return(panel[m].nsmallest(1, "rank"), tp).iloc[0]) - study.COST_RT for m in weeks})
        a, b = stats(vals.loc[pre]), stats(vals.loc[later])
        print(f"tp +{tp:.0%}: PRE {a['mean%']:+.2f} (t {a['t']:+.2f}) cagr {a['cagr']:+.0%} | TRAIN+POST {b['mean%']:+.2f} (t {b['t']:+.2f}) cagr {b['cagr']:+.0%}")

    print("\n== D. Harsher fills: must trade 1% through the limit, 0.5% round trip")
    harsh = pd.Series({m: float(rule_return(panel[m].nsmallest(1, "rank"), TP, 0.01).iloc[0]) - 0.005 for m in weeks})
    print("PRE", stats(harsh.loc[pre]), "\nTRAIN+POST", stats(harsh.loc[later]))

    print("\n== B. Other start days (list built on that weekday, enter 01:00 UTC, hold 7 days)")
    picks, needed = {}, {}
    for wd in range(7):
        for d in pd.date_range(study.START + pd.Timedelta(days=wd), universe.index[-1], freq="7D"):
            day = d - pd.Timedelta(days=1)
            if day not in universe.index:
                continue
            wk = study.score_week(features, model, day, universe.loc[day], "drop")
            if len(wk) >= 50:
                picks[d] = wk.index[0]
                needed.setdefault(wk.index[0], []).append(d)
    got = study.coin_weeks(needed)
    got = got.set_index(["perp", "monday"])
    rows = []
    for wd in range(7):
        vals = {d: float(rule_return(got.loc[[(p, d)]]).iloc[0]) - study.COST_RT for d, p in picks.items() if d.weekday() == wd and (p, d) in got.index}
        s = pd.Series(vals)
        a = stats(s[s.index <= study.PRE_END])
        b = stats(s[s.index > study.PRE_END])
        rows.append({"weekday": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][wd], "PRE_mean%": a.get("mean%"), "PRE_t": a.get("t"), "PRE_cagr": a.get("cagr"), "later_mean%": b.get("mean%"), "later_t": b.get("t"), "later_cagr": b.get("cagr")})
    print(pd.DataFrame(rows).to_string(index=False))

    # Monday came out best of seven, and nothing about Monday should matter. Spreading the same rule
    # over seven staggered tranches removes that luck and is the honest size of the effect.
    every = pd.Series({d: float(rule_return(got.loc[[(p, d)]]).iloc[0]) - study.COST_RT for d, p in picks.items() if (p, d) in got.index}).sort_index()
    blocks = every.groupby(every.index.to_period("W")).mean()
    blocks.index = blocks.index.start_time
    print("\n== Seven staggered tranches, one started each day, mean net % of deployed capital per week")
    for label, s in (("PRE", blocks[blocks.index <= study.PRE_END]), ("TRAIN+POST", blocks[blocks.index > study.PRE_END])):
        v = s.to_numpy()
        parts = []
        for f in (1.0, 0.5, 0.25, 0.1):
            eq = np.cumprod(1 + np.maximum(f * v, -1.0))
            parts.append(f"{f:.0%}: {eq[-1] ** (52 / len(v)) - 1:+.0%}/yr dd {(eq / np.maximum.accumulate(eq) - 1).min():.0%}")
        print(label, stats(v), "\n   size per week:", " | ".join(parts))
    print("by year:", (blocks.groupby(blocks.index.year).mean() * 100).round(2).to_dict())

    vols = [float(features["_volume"].loc[m - pd.Timedelta(days=1), panel[m].nsmallest(1, "rank").index[0]]) for m in weeks]
    print("\n== Size of the #1 coin: previous-day perp quote volume, $M: median", round(np.median(vols) / 1e6, 1),
          "p10", round(np.percentile(vols, 10) / 1e6, 1), "p90", round(np.percentile(vols, 90) / 1e6, 1))

    print("\n== Real archived lists, real #1, the same rule")
    rows = []
    for ds in study.ARCHIVE_DATES:
        real = json.load(open(study.LISTS / f"watchlist_{ds}.json", encoding="utf-8"))
        first = sorted(real["full_ranking"], key=lambda r: r["score"], reverse=True)[0]["base"]
        g = study.coin_weeks({f"{first}USDT": [pd.Timestamp(ds)]})
        if len(g):
            rows.append({"list": ds, "coin": first, "high%": round(100 * g["maxup"].iloc[0], 1), "low%": round(100 * g["maxdn"].iloc[0], 1), "hold%": round(100 * g["ret"].iloc[0], 1),
                         "rule_net%": round(100 * (float(rule_return(g).iloc[0]) - study.COST_RT), 1)})
    real_t = pd.DataFrame(rows)
    print(real_t.to_string(index=False))
    print("real lists", stats(real_t["rule_net%"] / 100))

    print("\n== Account growth at fixed size (ALL weeks, the rule, net): fraction of account per week")
    n = one.to_numpy()
    for label, vals in (("PRE", one.loc[pre].to_numpy()), ("TRAIN+POST", one.loc[later].to_numpy())):
        parts = []
        for f in (1.0, 0.5, 0.25, 0.1):
            eq = np.cumprod(1 + np.maximum(f * vals, -1.0))
            parts.append(f"{f:.0%}: {eq[-1] ** (52 / len(vals)) - 1:+.0%}/yr dd {(eq / np.maximum.accumulate(eq) - 1).min():.0%}")
        print(label, " | ".join(parts))
    print("worst 5 weeks:", [(str(k.date()), round(100 * v, 1)) for k, v in one.nsmallest(5).items()])


if __name__ == "__main__":
    main()
