"""How can the radar's indicators be used, given that they do not call direction?

The weekly model's rank predicts who reaches +40% in a week (45% for #1, 3% for the bottom) but its
mean return is flat. So the question is what a good tail-risk reading is worth. Five uses, with the
bars fixed before any of these tables was computed (2026-11-08):

U1 Tail meter. Does the score say more about next week's extremes than plain trailing volatility?
   Bar: date-averaged AUC beats vola_30d by at least 0.01 for BOTH touch +40% and touch -30%, in both
   DEV and HOLD. Otherwise the page can just show volatility.
U2 Filter on the event shorts (>=10% unlock t-7, Upbit KRW listing; development_reference.csv, read only).
   Hypothesis: shorting a top-quintile score coin is where squeezes live, so skipping them helps.
   Bar: skipping raises mean/sd per trade in both DEV and HOLD, one-sided p < 0.10 against skipping the
   same number of random trades, and the mean per trade does not fall.
U3 Filter on factor portfolios (rev21 reversal, cg_composite: the two running as trackers). The short leg
   is known to die on exploding coins, so drop top-score names before sorting.
   Bar: paired improvement in net return per 7-day cohort with Newey-West t >= 2 in both DEV and HOLD.
U4 Fade the spike / buy the dip by rank: short at the +40% touch or buy at the -30% touch within 7 days,
   exit at day 7's close. Bar: mean > 0 with t >= 2 in both periods AND median > 0, in the top-rank band.
U5 Market heat: share of the universe that touched +40% in the trailing week. Does it forecast next
   week's tail rates and event-short results? Bar: tail rate rises monotonically over heat terciles in
   both periods. Terciles use only the past (expanding, at least 52 weeks).

Periods: DEV 2022-01..2024-12, HOLD 2025-01.. . Costs 0.16% round trip per leg. Arithmetic returns;
shorts use (entry-exit)/entry. Features are from the close before entry; the score drops the two market
cap features as in weekly_rank_rotation_study.py. Numpy matmul is avoided on this machine.

Usage (conda run needed): python scripts/radar_indicator_uses_study.py <output dir> [radar replay parquet]
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

from core.research.pump_precursor import FEATURES, load_model_weights  # noqa: E402
import weekly_rank_rotation_study as rot  # noqa: E402

PANEL = ROOT / "data/research/xs_daily_panel"
START, DEV_END = pd.Timestamp("2022-01-03"), pd.Timestamp("2024-12-31")
COST = 0.0016
RADAR_SCORES = ["upside_score", "layout_score", "alert_score", "anomaly_score", "accumulation_score", "control_score",
                "ignition_score", "continuation_score", "risk_penalty", "narrative_heat_score"]


def nw_t(x, lag=6):
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    n = len(x)
    if n < 12:
        return float("nan"), float("nan")
    mu = x.mean()
    e = x - mu
    v = float((e * e).sum()) / n
    for k in range(1, lag + 1):
        v += 2 * (1 - k / (lag + 1)) * float((e[k:] * e[:-k]).sum()) / n
    return float(mu), float(mu / math.sqrt(max(v, 1e-18) / n))


def auc_by_date(score: pd.DataFrame, target: pd.DataFrame, dates) -> pd.Series:
    out = {}
    for d in dates:
        s, y = score.loc[d], target.loc[d]
        mask = s.notna() & y.notna()
        s, y = s[mask], y[mask].astype(bool)
        pos, neg = int(y.sum()), int((~y).sum())
        if pos < 3 or neg < 3:
            continue
        ranks = s.rank()
        out[d] = (float(ranks[y].sum()) - pos * (pos + 1) / 2) / (pos * neg)
    return pd.Series(out)


def build():
    print("building daily score panel", flush=True)
    model = load_model_weights()
    weights = dict(zip(model["features"], model["weights"]))
    universe = pd.read_parquet(PANEL / "universe.parquet") > 0.5
    majors = [c for c in universe.columns if rot.strip_1000(rot.base_of(c)) in rot.MAJORS | rot.NON_ALT]
    universe[majors] = False
    f = rot.build_features()
    names = [n for n in FEATURES if n not in ("log_mcap", "oi_mcap")]
    cols = universe.columns
    idx = universe.index

    def scored(mask: pd.DataFrame, partial: bool) -> pd.DataFrame:
        total = None
        complete = None
        for n in names:
            frame = f[n].reindex(index=idx, columns=cols).replace([np.inf, -np.inf], np.nan).where(mask)
            r = frame.rank(axis=1, pct=True) - 0.5
            complete = r.notna() if complete is None else (complete & r.notna())
            term = r.fillna(0.0) * weights[n]
            total = term if total is None else total + term
        return total.where(mask if partial else complete)

    z_uni = scored(universe, partial=False)
    present = f["_close"].reindex(index=idx, columns=cols).notna() & f["vola_30d"].reindex(index=idx, columns=cols).notna()
    present[majors] = False
    z_all = scored(present, partial=True)
    return universe, f, z_uni, z_all


def main() -> None:
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    replay_path = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    pd.set_option("display.width", 220, "display.max_columns", 40, "display.max_rows", 200)
    universe, f, z, z_all = build()
    idx, cols = universe.index, universe.columns
    read = lambda n: pd.read_parquet(PANEL / f"col_{n}.parquet").reindex(index=idx, columns=cols)
    close, high, low = read("close"), read("high"), read("low")
    fwd7 = np.expm1(pd.read_parquet(PANEL / "fwd7.parquet").reindex(index=idx, columns=cols))
    fund7 = pd.read_parquet(PANEL / "fund7.parquet").reindex(index=idx, columns=cols)
    pct = z.rank(axis=1, pct=True)
    rank = z.rank(axis=1, ascending=False, method="first")
    vol = f["vola_30d"].reindex(index=idx, columns=cols).where(universe)
    vol_pct = vol.rank(axis=1, pct=True)
    up7 = high.iloc[::-1].rolling(7, min_periods=1).max().iloc[::-1].shift(-1) / close - 1
    dn7 = low.iloc[::-1].rolling(7, min_periods=1).min().iloc[::-1].shift(-1) / close - 1
    last_full = idx[-8]
    dates = idx[(idx >= START) & (idx <= last_full)]
    dev = dates[dates <= DEV_END]
    hold = dates[dates > DEV_END]
    periods = {"DEV": dev, "HOLD": hold}
    valid = z.notna() & fwd7.notna()

    # ------------------------------------------------------------------ U1 tail meter
    print("\n=== U1 tail meter: chance within 7 days, by rank band")
    bands = [(1, 1), (2, 3), (4, 5), (6, 10), (11, 20), (21, 50), (51, 130)]
    rows = []
    for label, wks in periods.items():
        for a, b in bands:
            m = (rank.loc[wks] >= a) & (rank.loc[wks] <= b) & valid.loc[wks]
            u, d, r = up7.loc[wks].where(m), dn7.loc[wks].where(m), fwd7.loc[wks].where(m)
            cnt = float(m.sum().sum())
            rows.append({"period": label, "rank": f"{a}-{b}", "up>=20": (u >= .2).sum().sum() / cnt, "up>=40": (u >= .4).sum().sum() / cnt, "up>=100": (u >= 1).sum().sum() / cnt,
                         "dn<=-20": (d <= -.2).sum().sum() / cnt, "dn<=-30": (d <= -.3).sum().sum() / cnt, "dn<=-50": (d <= -.5).sum().sum() / cnt,
                         "mean_ret": np.nanmean(r.values), "median_ret": np.nanmedian(r.values)})
    print(pd.DataFrame(rows).round(3).to_string(index=False))

    print("\n--- AUC averaged over dates (0.5 = no information)")
    predictors = {"score": z, "vola_30d": vol, "ret_30d": f["ret_30d"].reindex(index=idx, columns=cols).where(universe),
                  "abs_ret_7d": f["ret_7d"].reindex(index=idx, columns=cols).where(universe).abs(), "range_20d": f["range_20d"].reindex(index=idx, columns=cols).where(universe)}
    targets = {"touch +40%": (up7 >= .4).where(valid), "touch -30%": (dn7 <= -.3).where(valid), "touch +100%": (up7 >= 1).where(valid),
               "both +20/-20": ((up7 >= .2) & (dn7 <= -.2)).where(valid)}
    auc_rows, u1_ok = [], True
    for tname, t in targets.items():
        for label, wks in periods.items():
            rec = {"target": tname, "period": label}
            for pname, p in predictors.items():
                rec[pname] = round(float(auc_by_date(p.where(valid), t, wks).mean()), 3)
            auc_rows.append(rec)
            if tname in ("touch +40%", "touch -30%") and rec["score"] - rec["vola_30d"] < 0.01:
                u1_ok = False
    print(pd.DataFrame(auc_rows).to_string(index=False))
    print("U1 bar passed:", u1_ok)

    print("\n--- Within the same volatility quintile: top vs bottom third of score")
    rows = []
    for label, wks in periods.items():
        for q in range(5):
            m = (vol_pct.loc[wks] > q / 5) & (vol_pct.loc[wks] <= (q + 1) / 5) & valid.loc[wks]
            within = z.loc[wks].where(m).rank(axis=1, pct=True)
            hi, lo = within >= 2 / 3, within <= 1 / 3
            rec = {"period": label, "vol_q": q + 1}
            for tag, sel in (("hi", hi), ("lo", lo)):
                cnt = float(sel.sum().sum())
                rec[f"{tag}_up40"] = (up7.loc[wks].where(sel) >= .4).sum().sum() / cnt
                rec[f"{tag}_dn30"] = (dn7.loc[wks].where(sel) <= -.3).sum().sum() / cnt
                rec[f"{tag}_mean"] = np.nanmean(fwd7.loc[wks].where(sel).values)
            rows.append(rec)
    print(pd.DataFrame(rows).round(3).to_string(index=False))

    # ------------------------------------------------------------------ radar replay scores
    if replay_path is not None and replay_path.exists():
        print("\n=== Radar real-time scores (weekly replay): AUC for the same targets, and vs volatility")
        rp = pd.read_parquet(replay_path)
        rp["perp"] = rp["symbol"].str.replace("/USDT", "USDT", regex=False)
        rp["day"] = pd.to_datetime(rp["ts"]) - pd.Timedelta(days=1)
        for c in RADAR_SCORES:
            rp[c] = pd.to_numeric(rp[c], errors="coerce")
        rows = []
        wide = {c: rp.pivot_table(index="day", columns="perp", values=c).reindex(columns=cols) for c in RADAR_SCORES}
        days = wide[RADAR_SCORES[0]].index
        for tname in ("touch +40%", "touch -30%"):
            for label, wks in periods.items():
                use = days.intersection(wks)
                rec = {"target": tname, "period": label, "weeks": len(use), "vola_30d": round(float(auc_by_date(vol, targets[tname], use).mean()), 3),
                       "model_score": round(float(auc_by_date(z, targets[tname], use).mean()), 3)}
                for c in RADAR_SCORES:
                    rec[c.replace("_score", "")] = round(float(auc_by_date(wide[c], targets[tname], use).mean()), 3)
                rows.append(rec)
        print(pd.DataFrame(rows).to_string(index=False))

    # ------------------------------------------------------------------ U2 event shorts
    print("\n=== U2 event shorts: does score at entry say anything? (read-only reference file)")
    ev = pd.read_csv(ROOT / "data/research/event_short_preclose_v1/development_reference.csv")
    ev["entry_day"] = pd.to_datetime(ev["entry_day"])
    ev["exit_day"] = pd.to_datetime(ev["exit_day"])
    ev = ev[ev["sample"].notna() & ev["hedged"].notna()].copy()
    print("samples:", ev["sample"].value_counts().to_dict(), "streams:", ev["stream"].value_counts().to_dict())
    pall = z_all.rank(axis=1, pct=True)
    vol_all = f["vola_30d"].reindex(index=idx, columns=cols).rank(axis=1, pct=True)
    ret30_all = f["ret_30d"].reindex(index=idx, columns=cols).rank(axis=1, pct=True)

    def at(frame, row, lag=1):
        day = row["entry_day"] - pd.Timedelta(days=lag)
        if day not in frame.index or row["perp"] not in frame.columns:
            return np.nan
        return float(frame.at[day, row["perp"]])

    ev["score_pct"] = ev.apply(lambda r: at(pall, r), axis=1)
    ev["score_pct_same"] = ev.apply(lambda r: at(pall, r, 0), axis=1)
    ev["vol_pct"] = ev.apply(lambda r: at(vol_all, r), axis=1)
    ev["ret30_pct"] = ev.apply(lambda r: at(ret30_all, r), axis=1)
    worst = []
    for _, r in ev.iterrows():
        if r["perp"] in high.columns and r["entry_day"] in high.index:
            seg = high.loc[r["entry_day"] + pd.Timedelta(days=1): r["exit_day"], r["perp"]]
            worst.append(float(seg.max() / r["entry_px"] - 1) if seg.notna().any() and r["entry_px"] > 0 else np.nan)
        else:
            worst.append(np.nan)
    ev["worst_up"] = worst
    ev.to_csv(out / "events_with_scores.csv", index=False)
    print("coverage: score", ev["score_pct"].notna().mean().round(3), "vol", ev["vol_pct"].notna().mean().round(3))

    def bucket_table(col, data):
        rows = []
        for lo, hi, tag in ((0, 1 / 3, "low third"), (1 / 3, 2 / 3, "mid third"), (2 / 3, 0.8, "upper"), (0.8, 1.01, "top fifth")):
            g = data[(data[col] > lo) & (data[col] <= hi)]
            if len(g):
                rows.append({"bucket": tag, "n": len(g), "mean%": 100 * g["hedged"].mean(), "median%": 100 * g["hedged"].median(), "win": (g["hedged"] > 0).mean(),
                             "sd%": 100 * g["hedged"].std(), "worst%": 100 * g["hedged"].min(), "stopped": (g["reason"] != "time").mean(), "worst_up_med%": 100 * g["worst_up"].median()})
        return pd.DataFrame(rows).round(2)

    for col in ("score_pct", "vol_pct", "ret30_pct", "score_pct_same"):
        print(f"\n--- by {col}, all trades")
        print(bucket_table(col, ev).to_string(index=False))
    print("\n--- by score_pct, each stream")
    for s, g in ev.groupby("stream"):
        print(s)
        print(bucket_table("score_pct", g).to_string(index=False))

    print("\n--- Rule test: skip the top fifth (the bar), each sample")
    rng = np.random.default_rng(13)
    for col in ("score_pct", "vol_pct"):
        for s, g in list(ev.groupby("sample")) + [("all", ev)]:
            g = g[g[col].notna()]
            keep = g[g[col] <= 0.8]
            base_ir = g["hedged"].mean() / g["hedged"].std()
            kept_ir = keep["hedged"].mean() / keep["hedged"].std()
            n_drop = len(g) - len(keep)
            sims = []
            arr = g["hedged"].to_numpy()
            for _ in range(5000):
                sel = np.ones(len(arr), dtype=bool)
                sel[rng.choice(len(arr), size=n_drop, replace=False)] = False
                sims.append(arr[sel].mean() / arr[sel].std(ddof=1))
            p = (np.sum(np.array(sims) >= kept_ir) + 1) / 5001
            half = g["hedged"].where(g[col] <= 0.8, g["hedged"] * 0.5)
            print(f"{col:10s} {s:8s} n {len(g):3d} drop {n_drop:3d} | all mean {100 * g['hedged'].mean():+.2f}% IR {base_ir:.3f} worst {100 * g['hedged'].min():+.1f}% | keep mean {100 * keep['hedged'].mean():+.2f}% IR {kept_ir:.3f} worst {100 * keep['hedged'].min():+.1f}% p {p:.3f} | half-size mean {100 * half.mean():+.2f}% IR {half.mean() / half.std():.3f}")

    # ------------------------------------------------------------------ U3 factor portfolios
    print("\n=== U3 factor portfolios: 7-day cohorts, long top fifth / short bottom fifth, net of costs and funding")
    factors = {"rev21 (low is long)": -pd.read_parquet(PANEL / "factor_rev21.parquet").reindex(index=idx, columns=cols),
               "cg_composite": pd.read_parquet(PANEL / "factor_cg_composite.parquet").reindex(index=idx, columns=cols)}
    long_r = (fwd7 - fund7.fillna(0.0))
    short_r = (-fwd7 + fund7.fillna(0.0))

    def ls_series(sig, mask, drop_from=None):
        s = sig.where(mask & universe & fwd7.notna())
        q = s.rank(axis=1, pct=True)
        top, bot = q >= 0.8, q <= 0.2
        if drop_from is not None:
            top = top & drop_from
            bot = bot & drop_from
        lr = long_r.where(top).mean(axis=1)
        sr = short_r.where(bot).mean(axis=1)
        return (lr + sr - 2 * COST).loc[dates], lr.loc[dates], sr.loc[dates]

    rows = []
    for fname, sig in factors.items():
        full = universe
        base, bl, bs = ls_series(sig, full)
        variants = {"drop top-20% score before sort": pct.le(0.8) | pct.isna(), "drop top-10% score before sort": pct.le(0.9) | pct.isna(),
                    "drop top-20% vol before sort": vol_pct.le(0.8) | vol_pct.isna()}
        for label, wks in periods.items():
            m, t = nw_t(base.loc[wks])
            rows.append({"factor": fname, "variant": "no filter", "period": label, "net%": 100 * m, "t": t, "long%": 100 * bl.loc[wks].mean(), "short%": 100 * bs.loc[wks].mean(), "diff%": np.nan, "diff_t": np.nan})
        for vname, keep in variants.items():
            series, sl, ss = ls_series(sig, keep)
            for label, wks in periods.items():
                m, t = nw_t(series.loc[wks])
                dm, dt = nw_t((series - base).loc[wks])
                rows.append({"factor": fname, "variant": vname, "period": label, "net%": 100 * m, "t": t, "long%": 100 * sl.loc[wks].mean(), "short%": 100 * ss.loc[wks].mean(), "diff%": 100 * dm, "diff_t": dt})
        # drop only from the short leg, where the squeezes are
        keep = pct.le(0.8) | pct.isna()
        series, sl, ss = ls_series(sig, full, None)
        q = sig.where(universe & fwd7.notna()).rank(axis=1, pct=True)
        top = q >= 0.8
        bot = (q <= 0.2) & keep
        series = long_r.where(top).mean(axis=1) + short_r.where(bot).mean(axis=1) - 2 * COST
        for label, wks in periods.items():
            m, t = nw_t(series.loc[wks])
            dm, dt = nw_t((series - base).loc[wks])
            rows.append({"factor": fname, "variant": "drop top-20% score, short leg only", "period": label, "net%": 100 * m, "t": t, "long%": 100 * long_r.where(top).mean(axis=1).loc[wks].mean(), "short%": 100 * short_r.where(bot).mean(axis=1).loc[wks].mean(), "diff%": 100 * dm, "diff_t": dt})
    print(pd.DataFrame(rows).round(3).to_string(index=False))

    # ------------------------------------------------------------------ U4 fade the spike / buy the dip
    print("\n=== U4 limit entries by rank band: short at +40% touch, or buy at -30% touch, exit at day 7's close")
    rows = []
    exit_ratio = fwd7 + 1
    for label, wks in periods.items():
        for a, b in [(1, 3), (4, 10), (11, 30), (31, 130)]:
            band = (rank.loc[wks] >= a) & (rank.loc[wks] <= b) & valid.loc[wks]
            touched = band & (up7.loc[wks] >= .4)
            fade = (-(exit_ratio.loc[wks] / 1.4 - 1) + fund7.loc[wks].fillna(0.0) / 2 - COST).where(touched)
            dipped = band & (dn7.loc[wks] <= -.3)
            dip = ((exit_ratio.loc[wks] / 0.7 - 1) - fund7.loc[wks].fillna(0.0) / 2 - COST).where(dipped)
            for tag, frame, n_events in (("fade +40", fade, touched), ("buy -30", dip, dipped)):
                per_day = frame.mean(axis=1)
                m, t = nw_t(per_day)
                vals = frame.values[np.isfinite(frame.values)]
                rows.append({"period": label, "rank": f"{a}-{b}", "rule": tag, "events": int(n_events.sum().sum()), "mean%": 100 * vals.mean() if len(vals) else np.nan, "median%": 100 * np.median(vals) if len(vals) else np.nan,
                             "win": (vals > 0).mean() if len(vals) else np.nan, "worst%": 100 * vals.min() if len(vals) else np.nan, "day_mean%": 100 * m, "t": t})
    print(pd.DataFrame(rows).round(2).sort_values(["rule", "rank", "period"]).to_string(index=False))

    # ------------------------------------------------------------------ U5 market heat
    print("\n=== U5 market heat: share of the universe that touched +40% in the trailing 7 days")
    back_high = high.rolling(7, min_periods=1).max()
    trail_up = back_high / close.shift(7) - 1
    heat = (trail_up.where(universe) >= .4).sum(axis=1) / universe.sum(axis=1).replace(0, np.nan)
    weekly = heat.loc[dates]
    q1 = weekly.expanding(min_periods=364).quantile(1 / 3).shift(1)
    q2 = weekly.expanding(min_periods=364).quantile(2 / 3).shift(1)
    state = pd.Series(np.where(weekly <= q1, "cool", np.where(weekly <= q2, "warm", "hot")), index=weekly.index).where(q1.notna())
    top20 = (rank <= 20) & valid
    rows = []
    next_up = (up7.where(valid) >= .4).sum(axis=1) / valid.sum(axis=1).replace(0, np.nan)
    next_dn = (dn7.where(valid) <= -.3).sum(axis=1) / valid.sum(axis=1).replace(0, np.nan)
    top_up = (up7.where(top20) >= .4).sum(axis=1) / top20.sum(axis=1).replace(0, np.nan)
    top_dn = (dn7.where(top20) <= -.3).sum(axis=1) / top20.sum(axis=1).replace(0, np.nan)
    uni_ret = fwd7.where(valid).mean(axis=1)
    disp = fwd7.where(valid).std(axis=1)
    tp_one = pd.Series(np.nan, index=dates, dtype=float)
    first = (rank == 1) & valid
    tp_frame = (fwd7.where(up7 < .4, .4) - fund7.fillna(0.0)).where(first)
    tp_one = tp_frame.mean(axis=1).loc[dates] - COST
    for label, wks in periods.items():
        for tag in ("cool", "warm", "hot"):
            days = state.loc[wks][state.loc[wks] == tag].index
            if len(days):
                rows.append({"period": label, "state": tag, "days": len(days), "heat_now%": 100 * weekly.loc[days].mean(), "next_up40%": 100 * next_up.loc[days].mean(), "next_dn30%": 100 * next_dn.loc[days].mean(),
                             "top20_up40%": 100 * top_up.loc[days].mean(), "top20_dn30%": 100 * top_dn.loc[days].mean(), "uni_ret%": 100 * uni_ret.loc[days].mean(), "dispersion%": 100 * disp.loc[days].mean(), "#1 tp40 net%": 100 * tp_one.loc[days].mean()})
    print(pd.DataFrame(rows).round(2).to_string(index=False))
    ev["heat"] = ev["entry_day"].map(lambda d: state.get(d - pd.Timedelta(days=1), None))
    rows = []
    for (s, tag), g in ev[ev["heat"].notna()].groupby(["sample", "heat"]):
        rows.append({"sample": s, "heat": tag, "n": len(g), "mean%": 100 * g["hedged"].mean(), "median%": 100 * g["hedged"].median(), "win": (g["hedged"] > 0).mean(), "worst%": 100 * g["hedged"].min()})
    print("\nevent shorts by heat at entry")
    print(pd.DataFrame(rows).round(2).to_string(index=False))


if __name__ == "__main__":
    main()
