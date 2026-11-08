"""Follow-up to radar_indicator_uses_study.py. All of this was decided AFTER seeing its tables, so it is
exploration: nothing here can confirm anything, only say what is worth watching forward.

A. Event shorts earned MORE on high-score coins, the opposite of the squeeze hypothesis. Is that stable
   across samples and streams, and is it the event or just how high-volatility coins behave anyway?
B. Limit-buying a -30% week looked strong outside the top ranks. Re-test it the way it could be run: one
   reference per coin per week, a resting limit that must trade 1% through, 0.3% round trip, on seven start
   days, as an account (1% of capital per fill), with crash weeks shown separately.
C. Volatility beat the model score as a tail meter. Which simple predictor is best, and how is it calibrated?
D. Radar real-time scores, if the weekly replay is available.

Usage (conda run needed): python scripts/radar_indicator_uses_followup.py <study output dir> [replay parquet]
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import radar_indicator_uses_study as base  # noqa: E402
import weekly_rank_rotation_study as rot  # noqa: E402

PANEL = base.PANEL
DEV_END = base.DEV_END
START = base.START


def ir(x) -> float:
    x = np.asarray(x, dtype=float)
    return float(x.mean() / x.std(ddof=1)) if len(x) > 2 and x.std(ddof=1) > 0 else float("nan")


def part_a(ev: pd.DataFrame, panel) -> None:
    close_fwd, fund, uni_mask, pct_all = panel
    print("\n=== A. Event shorts by score at entry")
    ev = ev[ev["score_pct"].notna()].copy()
    ev["band"] = pd.cut(ev["score_pct"], [0, 1 / 3, 2 / 3, 1.0], labels=["low", "mid", "high"])
    rows = []
    for keys, g in ev.groupby(["sample", "stream", "band"], observed=True):
        rows.append({"sample": keys[0], "stream": keys[1], "band": keys[2], "n": len(g), "mean%": 100 * g["hedged"].mean(), "median%": 100 * g["hedged"].median(), "win": (g["hedged"] > 0).mean(), "sd%": 100 * g["hedged"].std(), "worst%": 100 * g["hedged"].min()})
    print(pd.DataFrame(rows).round(2).to_string(index=False))
    print("\n--- by entry year, hedged mean %")
    ev["year"] = ev["entry_day"].dt.year
    print((ev.pivot_table(index="year", columns="band", values="hedged", aggfunc="mean", observed=True) * 100).round(2).join(ev.pivot_table(index="year", columns="band", values="hedged", aggfunc="count", observed=True), rsuffix="_n").to_string())

    print("\n--- Rule: skip the low-score third (hypothesis formed after seeing the pooled table)")
    rng = np.random.default_rng(7)
    for s, g in list(ev.groupby("sample")) + [("all", ev)]:
        keep = g[g["band"] != "low"]
        n_drop = len(g) - len(keep)
        arr = g["hedged"].to_numpy()
        sims_ir, sims_mean = [], []
        for _ in range(5000):
            sel = np.ones(len(arr), dtype=bool)
            sel[rng.choice(len(arr), size=n_drop, replace=False)] = False
            sims_ir.append(ir(arr[sel]))
            sims_mean.append(arr[sel].mean())
        print(f"{s:8s} n {len(g):3d} drop {n_drop:3d} | all {100 * g['hedged'].mean():+.2f}% IR {ir(arr):.3f} worst {100 * arr.min():+.1f}% | keep {100 * keep['hedged'].mean():+.2f}% IR {ir(keep['hedged']):.3f} worst {100 * keep['hedged'].min():+.1f}% | p(mean) {(np.sum(np.array(sims_mean) >= keep['hedged'].mean()) + 1) / 5001:.3f} p(IR) {(np.sum(np.array(sims_ir) >= ir(keep['hedged'])) + 1) / 5001:.3f}")
    for col in ("vol_pct", "ret30_pct"):
        sub = ev[ev[col].notna()]
        print(f"{col}: high third {100 * sub[sub[col] > 2 / 3]['hedged'].mean():+.2f}% / low third {100 * sub[sub[col] <= 1 / 3]['hedged'].mean():+.2f}%; holdout only {100 * sub[(sub[col] > 2 / 3) & (sub['sample'] == 'holdout')]['hedged'].mean():+.2f}% / {100 * sub[(sub[col] <= 1 / 3) & (sub['sample'] == 'holdout')]['hedged'].mean():+.2f}%")

    fwd = ev[ev["forward_era"].astype(str).str.lower() == "true"]
    print("forward era (entries since 2026-09-28):", {str(k): {"n": len(g), "mean%": round(100 * g["hedged"].mean(), 2), "median%": round(100 * g["hedged"].median(), 2)} for k, g in fwd.groupby("band", observed=True)})

    print("\n--- Same-score coins with no event: 7-day short minus equal-weight universe, 0.3% cost, same periods")
    rows = []
    event_days = {(r.perp, r.entry_day) for r in ev.itertuples()}
    uni_ret = (close_fwd - fund.fillna(0.0)).where(uni_mask).mean(axis=1)
    short_x = (-close_fwd + fund.fillna(0.0)).add(uni_ret, axis=0) - 0.003
    for s, wks in (("dev", (START, DEV_END)), ("holdout", (DEV_END + pd.Timedelta(days=1), pd.Timestamp("2026-11-03")))):
        sx = short_x.loc[wks[0]:wks[1]]
        pc = pct_all.reindex(index=sx.index, columns=sx.columns)
        for tag, lo, hi in (("low", 0, 1 / 3), ("mid", 1 / 3, 2 / 3), ("high", 2 / 3, 1.01)):
            vals = sx.where((pc > lo) & (pc <= hi) & uni_mask.reindex(index=sx.index, columns=sx.columns)).mean(axis=1)
            e = ev[(ev["sample"] == s) & (ev["band"] == tag)]["hedged"]
            rows.append({"sample": s, "band": tag, "no_event_mean%": 100 * vals.mean(), "event_mean%": 100 * e.mean(), "event_minus%": 100 * (e.mean() - vals.mean()), "n_events": len(e)})
    print(pd.DataFrame(rows).round(2).to_string(index=False))

    u = ev[ev["stream"] == "U"].copy()
    u["size_band"] = pd.cut(u["size_pct"], [0, 15, 30, 1e9], labels=["10-15", "15-30", ">30"])
    print("\n--- Unlock trades: score effect within unlock size (hedged mean %, n)")
    print((u.pivot_table(index="size_band", columns="band", values="hedged", aggfunc="mean", observed=True) * 100).round(2).join(u.pivot_table(index="size_band", columns="band", values="hedged", aggfunc="count", observed=True), rsuffix="_n").to_string())
    print("\n--- Within volatility: score effect (hedged mean %, n)")
    sub = ev[ev["vol_pct"].notna()].copy()
    sub["vol_band"] = pd.cut(sub["vol_pct"], [0, 0.5, 1.0], labels=["lower vol", "higher vol"])
    print((sub.pivot_table(index="vol_band", columns="band", values="hedged", aggfunc="mean", observed=True) * 100).round(2).join(sub.pivot_table(index="vol_band", columns="band", values="hedged", aggfunc="count", observed=True), rsuffix="_n").to_string())


def part_b(columns, universe, rank, vol) -> None:
    close, high, low, fwd7, fund7 = columns
    print("\n=== B. Resting limit buys: one reference per coin per week, must trade 1% through, 0.3% round trip, half the 7-day funding charged")
    idx = close.index
    dates = idx[(idx >= START) & (idx <= idx[-8])]
    dn7 = low.iloc[::-1].rolling(7, min_periods=1).min().iloc[::-1].shift(-1) / close - 1
    exit_ratio = fwd7 + 1
    valid = universe & fwd7.notna() & close.notna() & rank.notna()
    uni_week = fwd7.where(valid).median(axis=1)

    def events(day_list, level, mask):
        out = []
        for d in day_list:
            m = mask.loc[d] & (dn7.loc[d] <= -(level + 0.01))
            if not bool(m.any()):
                continue
            r = exit_ratio.loc[d][m] / (1 - level) - 1 - fund7.loc[d][m].fillna(0.0) / 2 - 0.003
            out.append(pd.DataFrame({"day": d, "perp": r.index, "r": r.to_numpy(), "rank": rank.loc[d][m].to_numpy()}))
        return pd.concat(out, ignore_index=True) if out else pd.DataFrame(columns=["day", "perp", "r", "rank"])

    def account(ev_df, day_list, size=0.01):
        by_day = ev_df.groupby("day")["r"].sum() * size
        return by_day.reindex(day_list).fillna(0.0)

    def line(ser, ev_df):
        v = ser.to_numpy()
        eq = np.cumprod(1 + np.maximum(v, -0.99))
        return {"events": len(ev_df), "ev_mean%": round(100 * ev_df["r"].mean(), 2) if len(ev_df) else None, "ev_median%": round(100 * ev_df["r"].median(), 2) if len(ev_df) else None, "win": round(float((ev_df["r"] > 0).mean()), 2) if len(ev_df) else None,
                "ev_worst%": round(100 * ev_df["r"].min(), 1) if len(ev_df) else None, "acct_wk%": round(100 * v.mean(), 3), "sharpe": round(float(v.mean() / v.std(ddof=1) * math.sqrt(52)), 2) if v.std(ddof=1) > 0 else None,
                "worst_wk%": round(100 * v.min(), 1), "maxdd%": round(100 * float((eq / np.maximum.accumulate(eq) - 1).min()), 1), "max_fills": int(ev_df.groupby("day").size().max()) if len(ev_df) else 0}

    mondays = [d for d in dates if d.weekday() == 6]  # Sunday close, i.e. the Monday reference
    dev_w = [d for d in mondays if d <= DEV_END]
    hold_w = [d for d in mondays if d > DEV_END]
    rows = []
    print("(level x rank band, Sunday-close reference, 1% of capital per fill)")
    for level in (0.2, 0.3, 0.4, 0.5):
        for a, b in ((1, 10), (11, 30), (31, 130), (1, 130)):
            mask = valid & (rank >= a) & (rank <= b)
            for label, wks in (("DEV", dev_w), ("HOLD", hold_w)):
                ev_df = events(wks, level, mask)
                rows.append({"level": f"-{level:.0%}", "rank": f"{a}-{b}", "period": label, **line(account(ev_df, wks), ev_df)})
    print(pd.DataFrame(rows).sort_values(["level", "rank", "period"]).to_string(index=False))

    # Stability: the same rule on all seven reference weekdays, and crash weeks vs the rest.
    print("\n--- -30%, all 130 names, by reference weekday (account % per week, Sharpe)")
    mask = valid
    rows = []
    for wd in range(7):
        ref = [d for d in dates if d.weekday() == wd]
        for label, wks in (("DEV", [d for d in ref if d <= DEV_END]), ("HOLD", [d for d in ref if d > DEV_END])):
            ev_df = events(wks, 0.3, mask)
            rows.append({"ref_weekday": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][wd], "period": label, **line(account(ev_df, wks), ev_df)})
    print(pd.DataFrame(rows).to_string(index=False))

    ev_all = events(mondays, 0.3, mask)
    ser = account(ev_all, mondays)
    print("\n--- -30%, all 130 names, Sunday reference: by year and without the best weeks")
    y = pd.DataFrame({"acct": ser, "year": [d.year for d in ser.index]})
    print((y.groupby("year")["acct"].agg(["mean", "sum", "min"]) * 100).round(2).to_string())
    print("events per year:", ev_all.groupby(ev_all["day"].dt.year).size().to_dict(), "median event by year %:", (ev_all.groupby(ev_all["day"].dt.year)["r"].median() * 100).round(1).to_dict())
    top = ser.nlargest(5)
    print("best 5 weeks:", [(str(k.date()), round(100 * v, 1)) for k, v in top.items()], "| share of total", round(float(top.sum() / ser.sum()), 2))
    print("worst 5 weeks:", [(str(k.date()), round(100 * v, 1)) for k, v in ser.nsmallest(5).items()])
    ev_all["crash_week"] = ev_all["day"].map(lambda d: bool(uni_week.get(d, 0.0) <= -0.15))
    for tag, g in ev_all.groupby("crash_week"):
        print(("market week median <= -15%" if tag else "other weeks"), {"events": len(g), "mean%": round(100 * g["r"].mean(), 2), "median%": round(100 * g["r"].median(), 2), "win": round(float((g["r"] > 0).mean()), 2), "worst%": round(100 * g["r"].min(), 1)})
    dist = ev_all.groupby("day").size()
    print("fills per week: median", int(dist.median()), "p90", int(dist.quantile(0.9)), "max", int(dist.max()))

    print("\n--- -30%, all weekdays pooled: non-crash weeks by year (crash = market median week <= -15%, known only afterwards)")
    ev_days = events([d for d in dates], 0.3, mask)
    ev_days["crash"] = ev_days["day"].map(lambda d: bool(uni_week.get(d, 0.0) <= -0.15))
    ev_days["year"] = ev_days["day"].dt.year
    print(ev_days.groupby(["crash", "year"])["r"].agg(events="count", mean=lambda x: round(100 * x.mean(), 2), median=lambda x: round(100 * x.median(), 2), win=lambda x: round(float((x > 0).mean()), 2)).to_string())

    print("\n--- Budget cap: at most 10 fills a week at 1% each (fills beyond 10 assumed not to execute, average return kept)")
    rows = []
    for wd in range(7):
        ref = [d for d in dates if d.weekday() == wd]
        for label, wks in (("DEV", [d for d in ref if d <= DEV_END]), ("HOLD", [d for d in ref if d > DEV_END])):
            ev_df = events(wks, 0.3, mask)
            g = ev_df.groupby("day")["r"]
            weekly = (g.mean() * g.size().clip(upper=10) * 0.01).reindex(wks).fillna(0.0)
            v = weekly.to_numpy()
            eq = np.cumprod(1 + np.maximum(v, -0.99))
            rows.append({"ref": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][wd], "period": label, "acct_wk%": round(100 * v.mean(), 3), "yr%": round(100 * (eq[-1] ** (52 / len(v)) - 1), 1), "sharpe": round(float(v.mean() / v.std(ddof=1) * math.sqrt(52)), 2), "worst_wk%": round(100 * v.min(), 1), "maxdd%": round(100 * float((eq / np.maximum.accumulate(eq) - 1).min()), 1)})
    print(pd.DataFrame(rows).to_string(index=False))

    # Control: market-order long the same universe on the same days, and the same coin one week later.
    long_all = (fwd7 - fund7.fillna(0.0)).where(valid).loc[mondays]
    print("control, plain 7-day long of the same universe, mean %:", {"DEV": round(100 * float(np.nanmean(long_all.loc[dev_w].values)), 2), "HOLD": round(100 * float(np.nanmean(long_all.loc[hold_w].values)), 2)})


def part_c(columns, universe, z, f) -> None:
    close, high, low, fwd7, _ = columns
    print("\n=== C. Tail meter: which simple predictor is best (date-averaged AUC)")
    idx, cols = close.index, close.columns
    dates = idx[(idx >= START) & (idx <= idx[-8])]
    dev, hold = dates[dates <= DEV_END], dates[dates > DEV_END]
    ret = close.pct_change(fill_method=None)
    mask = universe & fwd7.notna()
    park = np.sqrt((np.log(high / low) ** 2) / (4 * math.log(2)))
    preds = {"vol_7d": ret.rolling(7, min_periods=5).std(), "vol_14d": ret.rolling(14, min_periods=8).std(), "vol_30d": ret.rolling(30, min_periods=10).std(),
             "parkinson_7d": park.rolling(7, min_periods=5).mean(), "parkinson_14d": park.rolling(14, min_periods=8).mean(), "parkinson_30d": park.rolling(30, min_periods=10).mean(), "model_score": z}
    preds = {k: v.where(mask) for k, v in preds.items()}
    rk = {k: v.rank(axis=1, pct=True) for k, v in preds.items()}
    preds["park14+score"] = rk["parkinson_14d"] + rk["model_score"]
    preds["park14+vol30+score"] = rk["parkinson_14d"] + rk["vol_30d"] + rk["model_score"]
    up7 = high.iloc[::-1].rolling(7, min_periods=1).max().iloc[::-1].shift(-1) / close - 1
    dn7 = low.iloc[::-1].rolling(7, min_periods=1).min().iloc[::-1].shift(-1) / close - 1
    targets = {"touch +40%": (up7 >= .4).where(mask), "touch -30%": (dn7 <= -.3).where(mask), "either +40/-30": ((up7 >= .4) | (dn7 <= -.3)).where(mask)}
    rows = []
    sampled = {"DEV": dev[::3], "HOLD": hold[::3]}
    for tname, t in targets.items():
        for label, wks in sampled.items():
            rec = {"target": tname, "period": label}
            for k, p in preds.items():
                rec[k] = round(float(base.auc_by_date(p, t, wks).mean()), 3)
            rows.append(rec)
    print(pd.DataFrame(rows).to_string(index=False))

    best = rk["parkinson_14d"]
    print("\n--- Calibration: Parkinson 14-day decile -> chance within 7 days (all names in the universe)")
    rows = []
    for label, wks in (("DEV", dev), ("HOLD", hold)):
        for q in range(10):
            m = (best.loc[wks] > q / 10) & (best.loc[wks] <= (q + 1) / 10) & mask.loc[wks]
            n = float(m.sum().sum())
            rows.append({"period": label, "decile": q + 1, "up>=40": (up7.loc[wks].where(m) >= .4).sum().sum() / n, "dn<=-30": (dn7.loc[wks].where(m) <= -.3).sum().sum() / n, "median |7d|": float(np.nanmedian(fwd7.loc[wks].where(m).abs().values)),
                         "mean_ret": float(np.nanmean(fwd7.loc[wks].where(m).values)), "median_ret": float(np.nanmedian(fwd7.loc[wks].where(m).values))})
    print(pd.DataFrame(rows).round(3).to_string(index=False))

    print("\n--- Leverage reading: chance within 7 days of an adverse move of 1/L, ignoring maintenance margin and funding (HOLD, then DEV)")
    rows = []
    for label, wks in (("HOLD", hold), ("DEV", dev)):
        for lo, hi, tag in ((0, 0.3, "calm 30%"), (0.3, 0.7, "middle 40%"), (0.7, 0.9, "hot 20%"), (0.9, 1.01, "hottest 10%")):
            m = (best.loc[wks] > lo) & (best.loc[wks] <= hi) & mask.loc[wks]
            n = float(m.sum().sum())
            rec = {"period": label, "group": tag}
            for lev in (2, 3, 5, 10):
                rec[f"long {lev}x"] = round((dn7.loc[wks].where(m) <= -1 / lev).sum().sum() / n, 3)
                rec[f"short {lev}x"] = round((up7.loc[wks].where(m) >= 1 / lev).sum().sum() / n, 3)
            rows.append(rec)
    print(pd.DataFrame(rows).to_string(index=False))


def part_d(replay: Path, columns, universe, ev: pd.DataFrame) -> None:
    close, high, low, fwd7, _ = columns
    print("\n=== D. Radar real-time scores from the weekly replay")
    rp = pd.read_parquet(replay)
    rp["perp"] = rp["symbol"].str.replace("/USDT", "USDT", regex=False)
    rp["day"] = pd.to_datetime(rp["ts"]) - pd.Timedelta(days=1)
    for c in base.RADAR_SCORES:
        rp[c] = pd.to_numeric(rp[c], errors="coerce")
    idx, cols = close.index, close.columns
    wide = {c: rp.pivot_table(index="day", columns="perp", values=c).reindex(columns=cols) for c in base.RADAR_SCORES}
    days = wide[base.RADAR_SCORES[0]].index
    mask = fwd7.notna()
    up7 = high.iloc[::-1].rolling(7, min_periods=1).max().iloc[::-1].shift(-1) / close - 1
    dn7 = low.iloc[::-1].rolling(7, min_periods=1).min().iloc[::-1].shift(-1) / close - 1
    vol = close.pct_change(fill_method=None).rolling(30, min_periods=10).std()
    rows = []
    for tname, t in (("touch +40%", up7 >= .4), ("touch -30%", dn7 <= -.3)):
        for label, wks in (("DEV", days[days <= DEV_END]), ("HOLD", days[days > DEV_END])):
            wks = wks.intersection(idx[idx <= idx[-8]])
            common = wide[base.RADAR_SCORES[0]].reindex(index=wks).notna()
            rec = {"target": tname, "period": label, "weeks": len(wks), "vol_30d": round(float(base.auc_by_date(vol.reindex(index=wks).where(common & mask.reindex(index=wks)), t.reindex(index=wks).where(common & mask.reindex(index=wks)), wks).mean()), 3)}
            for c in base.RADAR_SCORES:
                rec[c.replace("_score", "")] = round(float(base.auc_by_date(wide[c].reindex(index=wks), t.reindex(index=wks).where(common & mask.reindex(index=wks)), wks).mean()), 3)
            rows.append(rec)
    print(pd.DataFrame(rows).to_string(index=False))

    print("\n--- Event shorts by radar score at the latest Monday before entry (coins in the replay universe only)")
    rows = []
    for c in ("accumulation_score", "continuation_score", "anomaly_score", "control_score", "narrative_heat_score", "upside_score", "risk_penalty"):
        pc = wide[c].rank(axis=1, pct=True)
        vals = []
        for r in ev.itertuples():
            pos = pc.index.searchsorted(r.entry_day - pd.Timedelta(days=1), side="right") - 1
            vals.append(float(pc.iat[pos, pc.columns.get_loc(r.perp)]) if pos >= 0 and r.perp in pc.columns else np.nan)
        s = pd.Series(vals, index=ev.index)
        g = ev[s.notna()]
        s = s[s.notna()]
        rec = {"score": c, "n": len(g)}
        for s_name, sub_idx in (("dev", g["sample"] == "dev"), ("holdout", g["sample"] == "holdout")):
            hi = g[sub_idx & (s > 2 / 3)]["hedged"]
            lo = g[sub_idx & (s <= 1 / 3)]["hedged"]
            rec[f"{s_name}_high%"] = round(100 * hi.mean(), 2)
            rec[f"{s_name}_low%"] = round(100 * lo.mean(), 2)
            rec[f"{s_name}_n"] = f"{len(hi)}/{len(lo)}"
        rows.append(rec)
    print(pd.DataFrame(rows).to_string(index=False))


def main() -> None:
    out = Path(sys.argv[1])
    replay = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    pd.set_option("display.width", 230, "display.max_columns", 40, "display.max_rows", 200)
    universe, f, z, z_all = base.build()
    idx, cols = universe.index, universe.columns
    read = lambda n: pd.read_parquet(PANEL / f"col_{n}.parquet").reindex(index=idx, columns=cols)
    close, high, low = read("close"), read("high"), read("low")
    fwd7 = np.expm1(pd.read_parquet(PANEL / "fwd7.parquet").reindex(index=idx, columns=cols))
    fund7 = pd.read_parquet(PANEL / "fund7.parquet").reindex(index=idx, columns=cols)
    rank = z.rank(axis=1, ascending=False, method="first")
    ev = pd.read_csv(out / "events_with_scores.csv", parse_dates=["entry_day", "exit_day"])
    columns = (close, high, low, fwd7, fund7)

    part_a(ev, (fwd7, fund7, universe, z_all.rank(axis=1, pct=True)))
    part_b(columns, universe, rank, None)
    part_c(columns, universe, z, f)
    if replay is not None and replay.exists():
        part_d(replay, columns, universe, ev)
    else:
        print("\n(radar replay not available yet)")


if __name__ == "__main__":
    main()
