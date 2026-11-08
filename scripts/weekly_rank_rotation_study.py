"""Weekly rotation on the radar's pump-list ranking: hold rank #1 / top-K for a week, re-rank, repeat.

Rules fixed before any return in this script was computed (2026-11-07):

Signal      Frozen weights in data/research/pump_watchlist/model_weights.json (trained on
            2025-07-21..2026-02-15 only), re-scored point-in-time every Monday from the Sunday
            close on the local panel (data/research/xs_daily_panel), which includes delisted perps.
            There is no history of market cap, so the two mcap features use the proxy that best
            reproduces the real archived lists (chosen in stage 1, before returns are looked at).
Trade       Enter at the Monday 01:00 UTC 1h open (the live list is ready around then), exit at the
            next Monday 01:00 open. A perp that stops trading mid-week exits at its last close.
            Shorts use (entry-exit)/entry. Real funding: longs pay, shorts earn.
Costs       0.16% round trip per leg (taker 5bp x2 + slippage 3bp x2), charged only on names that
            actually enter or leave; 0.5% is the sensitivity case.
Variants    side: long, short, long minus universe, short against universe
            K: 1, 3, 5, 10, 15
            stop: none, or -30% for longs / +40% for shorts (first touch on 1h bars, gaps fill at open)
            gated: long or short decided by BTC against its 50-day mean, or by last week's excess
Periods     PRE   2022-01-03..2025-07-14  never seen by the weights, the clean test
            TRAIN 2025-07-21..2026-02-09  in-sample for the weights
            POST  2026-02-16..latest      after the training cut (2026-07 onward was already seen by
                                          the 10/24 review, which is what suggested the short side)
            REAL  the archived lists, with the real scores
A variant passes only if ALL hold:
  1. mean net weekly return > 0 in PRE and in POST
  2. in PRE, gross mean beats same-size random picks from the same weekly universe at p < 0.001
     (one-sided; 0.05 / about 50 variants)
  3. dropping the 3 best-contributing coins leaves PRE and POST means > 0
  4. compounded at 1x, equity never reaches zero in any period

Usage (conda run is needed for the native DLL path):
  python scripts/weekly_rank_rotation_study.py <output dir>
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core.research.pump_precursor import FEATURES, load_model_weights  # noqa: E402

PANEL = ROOT / "data/research/xs_daily_panel"
FUT = ROOT / "data/historical/binance_futures"
LISTS = ROOT / "data/research/pump_watchlist"
MAJORS = {"BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "TRX", "TON"}
NON_ALT = {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USDE", "USDS", "DAI", "PAXG", "XAUT"}
UNIVERSE_N = 130
COST_RT = 0.0016
STOP_LONG, STOP_SHORT = 0.30, 0.40
START = pd.Timestamp("2022-01-03")
PRE_END, TRAIN_END = pd.Timestamp("2025-07-14"), pd.Timestamp("2026-02-09")
ARCHIVE_DATES = ["2026-07-20", "2026-08-03", "2026-08-10", "2026-09-07", "2026-09-14", "2026-09-21",
                 "2026-09-28", "2026-10-05", "2026-10-12", "2026-10-19", "2026-10-26"]
KS = (1, 3, 5, 10, 15)


def base_of(perp: str) -> str:
    return perp[:-4]


def strip_1000(base: str) -> str:
    for prefix in ("1000000", "1000"):
        if base.startswith(prefix) and len(base) > len(prefix):
            return base[len(prefix):]
    return base


def build_features() -> dict[str, pd.DataFrame]:
    read = lambda name: pd.read_parquet(PANEL / f"col_{name}.parquet")
    c, v, oi, fund = read("close"), read("volume"), read("oi"), read("funding")
    f: dict[str, pd.DataFrame] = {}
    # Live generation only ever sees the last 365 days, so ATH, "ever" and age are capped to that.
    f["age_days"] = c.notna().cumsum().where(c.notna()).clip(upper=365)
    f["oi_chg_7d"] = oi / oi.shift(7) - 1
    f["oi_chg_30d"] = oi / oi.shift(30) - 1
    f["funding_7d"] = fund.rolling(7, min_periods=3).mean()
    f["ret_7d"] = c / c.shift(7) - 1
    f["ret_30d"] = c / c.shift(30) - 1
    f["dd_from_ath"] = c / c.rolling(365, min_periods=1).max() - 1
    f["vola_30d"] = c.pct_change(fill_method=None).rolling(30, min_periods=10).std()
    vol_avg = v.rolling(30, min_periods=10).mean()
    f["vol_trend"] = v.rolling(7, min_periods=3).mean() / vol_avg
    f["vol_pctile_90d"] = v.rolling(90, min_periods=20).apply(lambda x: (x[-1] >= x).mean(), raw=True)
    f["range_20d"] = (c.rolling(20).max() - c.rolling(20).min()) / c
    past = (c / c.shift(30) >= 2.0).astype(float).where(c.notna())
    f["pumped_before_120d"] = past.rolling(120, min_periods=1).max().shift(1)
    f["pumped_ever"] = past.rolling(365, min_periods=1).max().shift(1)
    f["log_vol30"] = np.log10(vol_avg.clip(lower=1e2))
    f["oi_over_vol"] = oi / vol_avg  # proxy candidate for oi/mcap
    f["_volume"] = v
    f["_close"] = c
    return f


def score_week(features, model, day, universe_row, proxy: str) -> pd.DataFrame:
    members = [c for c in universe_row.index[universe_row.to_numpy()] if strip_1000(base_of(c)) not in MAJORS | NON_ALT]
    volume = features["_volume"].loc[day, members].dropna().sort_values(ascending=False)
    members = list(volume.index[:UNIVERSE_N])
    names = [n for n in FEATURES if n not in ("log_mcap", "oi_mcap")]
    frame = pd.DataFrame({n: features[n].loc[day, members] for n in names})
    if proxy == "volume":
        frame["log_mcap"] = frame["log_vol30"]
        frame["oi_mcap"] = features["oi_over_vol"].loc[day, members]
    frame = frame.replace([np.inf, -np.inf], np.nan).dropna()
    if frame.empty:
        return frame
    ranks = frame.rank(pct=True)
    weights = dict(zip(model["features"], model["weights"]))
    z = sum(weights[n] * (ranks[n] - 0.5) for n in ranks.columns if n in weights)
    out = pd.DataFrame({"score": z, "ret_30d": frame["ret_30d"], "vola_30d": frame["vola_30d"]})
    out = out.sort_values("score", ascending=False)
    out["rank"] = np.arange(1, len(out) + 1)
    return out


def validate(features, model, universe) -> str:
    print("\n== Stage 1: does the rebuild match the real archived lists? (no returns used)")
    best, table = None, {}
    for proxy in ("drop", "volume"):
        rows = []
        for ds in ARCHIVE_DATES:
            real = json.load(open(LISTS / f"watchlist_{ds}.json", encoding="utf-8"))
            real_rank = {r["base"]: r["score"] for r in real["full_ranking"]}
            day = pd.Timestamp(ds) - pd.Timedelta(days=1)
            mine = score_week(features, model, day, universe.loc[day], proxy)
            mine.index = [strip_1000(base_of(c)) for c in mine.index]
            mine = mine[~mine.index.duplicated()]
            both = [b for b in mine.index if b in real_rank]
            real_s = pd.Series({b: real_rank[b] for b in both})
            rho = mine.loc[both, "score"].rank().corr(real_s.rank())
            top_real = set(sorted(real_rank, key=real_rank.get, reverse=True)[:15])
            top_mine = set(mine.index[:15])
            first = sorted(real_rank, key=real_rank.get, reverse=True)[0]
            rows.append({"list": ds, "common": len(both), "spearman": rho, "top15_overlap": len(top_real & top_mine),
                         "real_1": first, "mine_1": mine.index[0], "real_1_my_rank": int(mine.loc[first, "rank"]) if first in mine.index else -1})
        t = pd.DataFrame(rows)
        table[proxy] = t
        print(f"proxy={proxy}: spearman {t.spearman.mean():.3f}, top-15 overlap {t.top15_overlap.mean():.1f}/15, same #1 {int((t.real_1 == t.mine_1).sum())}/{len(t)}")
        if best is None or t.spearman.mean() > table[best].spearman.mean():
            best = proxy
    print(table[best].round(3).to_string(index=False))
    print("chosen proxy:", best)
    return best


def coin_weeks(needed: dict[str, list[pd.Timestamp]]) -> pd.DataFrame:
    """Per (coin, Monday): entry, exit, funding and first-touch stop exits, from 1h bars."""
    rows = []
    for k, (perp, mondays) in enumerate(sorted(needed.items())):
        folder = FUT / f"{base_of(perp)}_USDT"
        path = folder / "1h.parquet"
        if not path.exists():
            continue
        bars = pd.read_parquet(path, columns=["open", "high", "low", "close"])
        idx = pd.DatetimeIndex(bars.index)
        if idx.tz is not None:
            idx = idx.tz_convert("UTC").tz_localize(None)
        order = np.argsort(idx.asi8, kind="stable")
        idx = idx[order]
        o, h, l, cl = (bars[n].to_numpy(dtype=float)[order] for n in ("open", "high", "low", "close"))
        keep = np.r_[idx.asi8[1:] != idx.asi8[:-1], True]
        idx, o, h, l, cl = idx[keep], o[keep], h[keep], l[keep], cl[keep]
        fpath = folder / "funding_rate.parquet"
        f_t, f_c = np.array([], dtype="datetime64[ns]"), np.array([0.0])
        if fpath.exists():
            fr = pd.read_parquet(fpath, columns=["funding_rate"]).sort_index()
            fi = pd.DatetimeIndex(fr.index)
            if fi.tz is not None:
                fi = fi.tz_convert("UTC").tz_localize(None)
            f_t = fi.values
            f_c = np.r_[0.0, np.cumsum(fr["funding_rate"].to_numpy(dtype=float))]

        def funding(a, b):
            if not len(f_t):
                return 0.0
            return float(f_c[np.searchsorted(f_t, np.datetime64(b), side="right")] - f_c[np.searchsorted(f_t, np.datetime64(a), side="right")])

        tail = idx[-1] if len(idx) else None
        for m in mondays:
            t0 = m + pd.Timedelta(hours=1)
            t1 = t0 + pd.Timedelta(days=7)
            i0 = idx.searchsorted(t0)
            if i0 >= len(idx) or idx[i0] - t0 > pd.Timedelta(hours=3) or not o[i0] > 0:
                continue
            i1 = idx.searchsorted(t1)
            delisted = False
            if i1 < len(idx) and idx[i1] - t1 <= pd.Timedelta(hours=3):
                ex, seg_end = o[i1], i1
            elif tail is not None and tail < STORE_END - pd.Timedelta(days=3):
                if i1 <= i0:
                    continue
                ex, seg_end = cl[i1 - 1], i1
                delisted = True
            else:
                continue  # window not finished in the local store, or a gap
            e = o[i0]
            hi, lo = h[i0:seg_end], l[i0:seg_end]
            rec = {"perp": perp, "monday": m, "ret": ex / e - 1, "fund": funding(t0, t1), "maxup": hi.max() / e - 1,
                   "maxdn": lo.min() / e - 1, "delisted": delisted}
            hit = np.flatnonzero(hi >= e * (1 + STOP_SHORT))
            if len(hit):
                j = i0 + int(hit[0])
                rec["ret_short_stop"] = max(e * (1 + STOP_SHORT), o[j]) / e - 1
                rec["fund_short_stop"] = funding(t0, idx[j])
            else:
                rec["ret_short_stop"], rec["fund_short_stop"] = rec["ret"], rec["fund"]
            hit = np.flatnonzero(lo <= e * (1 - STOP_LONG))
            if len(hit):
                j = i0 + int(hit[0])
                rec["ret_long_stop"] = min(e * (1 - STOP_LONG), o[j]) / e - 1
                rec["fund_long_stop"] = funding(t0, idx[j])
            else:
                rec["ret_long_stop"], rec["fund_long_stop"] = rec["ret"], rec["fund"]
            rows.append(rec)
        if k % 100 == 0:
            print(f"  1h pass {k}/{len(needed)}", flush=True)
    return pd.DataFrame(rows)


def period_of(m: pd.Timestamp) -> str:
    return "PRE" if m <= PRE_END else "TRAIN" if m <= TRAIN_END else "POST"


def gross_per_coin(week: pd.DataFrame, side: str, stop: bool) -> pd.Series:
    """Net-of-funding return of each coin that week for one side, before trading costs."""
    if side == "long":
        return (week["ret_long_stop"] - week["fund_long_stop"]) if stop else (week["ret"] - week["fund"])
    return (-week["ret_short_stop"] + week["fund_short_stop"]) if stop else (-week["ret"] + week["fund"])


def run_variant(panel, weeks, side, k, stop, cost_rt=COST_RT, gate=None):
    """side: long | short | long_x | short_x (x = against the equal-weight universe)."""
    leg = "long" if side.startswith("long") else "short"
    out, prev = [], set()
    for i, m in enumerate(weeks):
        wk = panel[m]
        this_leg = leg
        if gate is not None:
            this_leg = gate.get(m)
            if this_leg is None:
                continue
        picks = list(wk.nsmallest(k, "rank").index)
        net = gross_per_coin(wk.loc[picks], this_leg, stop)
        gross = float(net.mean())
        uni = float((wk["ret"] - wk["fund"]).mean())
        nxt = set(panel[weeks[i + 1]].nsmallest(k, "rank").index) if i + 1 < len(weeks) else set()
        turnover = (len(set(picks) - prev) + len(set(picks) - nxt)) / k
        cost = turnover * cost_rt / 2
        hedged = side.endswith("_x")
        value = gross + (uni if side == "short_x" else -uni if side == "long_x" else 0.0)
        out.append({"monday": m, "gross": value, "net": value - cost - (cost_rt if hedged else 0.0), "turnover": turnover,
                    "contrib": dict(zip(picks, net / k))})
        prev = set(picks)
    return pd.DataFrame(out)


def describe(res: pd.DataFrame) -> dict:
    if res.empty:
        return {}
    n = res["net"].to_numpy()
    eq = np.cumprod(1 + np.maximum(n, -1.0))
    peak = np.maximum.accumulate(eq)
    ruined = bool((n <= -1).any())
    std = n.std(ddof=1) if len(n) > 1 else float("nan")
    return {"weeks": len(n), "mean": n.mean(), "median": np.median(n), "win": (n > 0).mean(), "worst": n.min(), "best": n.max(),
            "t": n.mean() / (std / math.sqrt(len(n))) if std > 0 else float("nan"),
            "cagr": float(eq[-1] ** (52 / len(n)) - 1) if eq[-1] > 0 else -1.0,
            "maxdd": float((eq / peak - 1).min()), "ruined": ruined, "turnover": res["turnover"].mean()}


def ex_top3_mean(res: pd.DataFrame, cost_col="net") -> float:
    if res.empty:
        return float("nan")
    totals: dict[str, float] = {}
    for c in res["contrib"]:
        for coin, value in c.items():
            totals[coin] = totals.get(coin, 0.0) + float(value)
    top = sorted(totals, key=totals.get, reverse=True)[:3]
    removed = sum(totals[c] for c in top)
    return float((res[cost_col].sum() - removed) / len(res))


def random_p(panel, weeks, side, k, stop, observed_gross, sims=2000, seed=11) -> float:
    rng = np.random.default_rng(seed)
    leg = "long" if side.startswith("long") else "short"
    total = np.zeros(sims)
    for m in weeks:
        wk = panel[m]
        arr = gross_per_coin(wk, leg, stop).to_numpy(dtype=float)
        arr = arr[np.isfinite(arr)]
        idx = rng.integers(0, len(arr), size=(sims, k))
        draw = arr[idx].mean(axis=1)
        if side == "short_x":
            draw = draw + float((wk["ret"] - wk["fund"]).mean())
        elif side == "long_x":
            draw = draw - float((wk["ret"] - wk["fund"]).mean())
        total += draw
    return float((np.sum(total / len(weeks) >= observed_gross) + 1) / (sims + 1))


def main() -> None:
    global STORE_END
    out_dir = Path(sys.argv[1])
    out_dir.mkdir(parents=True, exist_ok=True)
    model = load_model_weights()
    universe = pd.read_parquet(PANEL / "universe.parquet") > 0.5
    print("panel to", universe.index[-1].date())
    features = build_features()
    STORE_END = pd.Timestamp(universe.index[-1]) + pd.Timedelta(days=1)
    proxy = validate(features, model, universe)

    mondays = [m for m in pd.date_range(START, universe.index[-1], freq="W-MON")]
    scored = {}
    for m in mondays:
        day = m - pd.Timedelta(days=1)
        wk = score_week(features, model, day, universe.loc[day], proxy)
        if len(wk) >= 50:
            scored[m] = wk
    print(f"\nweeks scored {len(scored)}; names per week median {int(np.median([len(v) for v in scored.values()]))}")

    cache = out_dir / "coin_weeks.parquet"
    if cache.exists():
        cw = pd.read_parquet(cache)
    else:
        needed: dict[str, list[pd.Timestamp]] = {}
        for m, wk in scored.items():
            for perp in wk.index:
                needed.setdefault(perp, []).append(m)
        print(f"== Stage 2: weekly returns from 1h bars for {len(needed)} perps")
        cw = coin_weeks(needed)
        cw.to_parquet(cache)
    print("coin-weeks", len(cw), "delisting exits", int(cw["delisted"].sum()))

    panel = {}
    for m, wk in scored.items():
        g = cw[cw["monday"] == m].set_index("perp")
        joined = wk.join(g, how="inner")
        if len(joined) >= 50:
            panel[m] = joined
    weeks = sorted(panel)
    by_period = {p: [m for m in weeks if period_of(m) == p] for p in ("PRE", "TRAIN", "POST")}
    print({p: (len(v), str(v[0].date()), str(v[-1].date())) for p, v in by_period.items()})

    # What the #1 actually looks like and how often it changes.
    tops = pd.DataFrame({m: panel[m].nsmallest(1, "rank").iloc[0][["ret_30d", "vola_30d", "maxup", "maxdn", "ret"]] for m in weeks}).T
    names = [panel[m].nsmallest(1, "rank").index[0] for m in weeks]
    print("\n== Rank #1 profile")
    print(f"median 30d run-up before entry {tops.ret_30d.astype(float).median():+.0%}, daily vol {tops.vola_30d.astype(float).median():.1%}")
    print(f"same #1 as the week before: {np.mean([a == b for a, b in zip(names[1:], names[:-1])]):.0%}; distinct coins {len(set(names))} of {len(names)} weeks")
    print(f"in the 7-day hold: median high {tops.maxup.astype(float).median():+.0%}, median low {tops.maxdn.astype(float).median():+.0%}; "
          f"weeks up >=40% intraweek {np.mean(tops.maxup.astype(float) >= 0.4):.0%}, >=100% {np.mean(tops.maxup.astype(float) >= 1.0):.0%}; "
          f"down <=-30% {np.mean(tops.maxdn.astype(float) <= -0.3):.0%}")

    # Gates: decided only from data available before the Monday entry.
    btc = pd.read_parquet(FUT / "BTC_USDT" / "1d.parquet", columns=["close"])["close"]
    btc.index = pd.DatetimeIndex(btc.index).tz_localize(None).normalize() if pd.DatetimeIndex(btc.index).tz is None else pd.DatetimeIndex(btc.index).tz_convert("UTC").tz_localize(None).normalize()
    btc = btc[~btc.index.duplicated(keep="last")]
    ma50 = btc.rolling(50).mean()
    gate_btc = {m: ("long" if btc.asof(m - pd.Timedelta(days=1)) > ma50.asof(m - pd.Timedelta(days=1)) else "short") for m in weeks}

    rows = []
    variants = [(s, k, st, None) for s in ("long", "short", "long_x", "short_x") for k in KS for st in (False, True)]
    variants += [("gate_btc", k, False, gate_btc) for k in KS] + [("gate_self", k, False, "self") for k in KS]
    keep_results = {}
    for side, k, stop, gate in variants:
        if gate == "self":
            base = run_variant(panel, weeks, "long_x", k, False).set_index("monday")["gross"]
            g = {m: ("long" if base.get(prev, 0.0) > 0 else "short") for prev, m in zip(weeks[:-1], weeks[1:])}
        else:
            g = gate
        run_side = "long" if gate is not None else side
        res = run_variant(panel, weeks, run_side, k, stop, gate=g).set_index("monday", drop=False)
        keep_results[(side, k, stop)] = res
        rec = {"side": side, "k": k, "stop": stop}
        for p, wks in by_period.items():
            sub = res.loc[[m for m in wks if m in res.index]]
            d = describe(sub)
            for key in ("mean", "median", "win", "t", "cagr", "maxdd", "worst"):
                rec[f"{p}_{key}"] = d.get(key)
            rec[f"{p}_ruined"] = d.get("ruined")
            rec[f"{p}_ex3"] = ex_top3_mean(sub)
        pre = res.loc[[m for m in by_period["PRE"] if m in res.index]]
        if gate is None:
            rec["pre_p"] = random_p(panel, by_period["PRE"], side, k, stop, pre["gross"].mean())
        else:
            rec["pre_p"] = float("nan")
        rec["passes"] = bool(
            rec["PRE_mean"] > 0 and rec["POST_mean"] > 0 and rec["pre_p"] < 0.001 and rec["PRE_ex3"] > 0 and rec["POST_ex3"] > 0
            and not rec["PRE_ruined"] and not rec["TRAIN_ruined"] and not rec["POST_ruined"]
        )
        rows.append(rec)
    T = pd.DataFrame(rows)
    T.drop(columns=[]).to_csv(out_dir / "variants.csv", index=False)
    pd.set_option("display.width", 240, "display.max_columns", 60, "display.max_rows", 200)
    show = T.copy()
    for c in show.columns:
        if c.endswith(("_mean", "_median", "_ex3")):
            show[c] = (show[c] * 100).round(2)
    print("\n== Stage 3: every variant, net weekly return in %, t-stat, compounded result at 1x")
    cols = ["side", "k", "stop", "PRE_mean", "PRE_t", "PRE_win", "PRE_ex3", "pre_p", "TRAIN_mean", "POST_mean", "POST_t", "POST_ex3", "PRE_ruined", "POST_ruined", "PRE_maxdd", "POST_maxdd", "passes"]
    print(show[cols].round(3).to_string(index=False))
    print("\npasses:", int(T["passes"].sum()), "of", len(T))

    # Year-by-year for the variants people would actually trade.
    print("\n== Calendar years, mean net weekly % (stop variants)")
    yr = {}
    for key in (("long", 1, True), ("short", 1, True), ("long", 5, True), ("short", 5, True), ("short_x", 5, True), ("short_x", 15, True), ("long_x", 5, True)):
        r = keep_results[key]
        yr[f"{key[0]}#{key[1]}"] = (r.groupby(r["monday"].dt.year)["net"].mean() * 100).round(2)
    print(pd.DataFrame(yr).to_string())

    # Cost sensitivity for the concentrated versions.
    print("\n== Cost sensitivity: round trip 0.5% per leg (rank #1 names are thin)")
    for key in (("long", 1, True), ("short", 1, True), ("short", 5, True), ("short_x", 5, True)):
        res = run_variant(panel, weeks, key[0], key[1], key[2], cost_rt=0.005).set_index("monday", drop=False)
        vals = {p: describe(res.loc[[m for m in w if m in res.index]])["mean"] * 100 for p, w in by_period.items()}
        print(key, {p: round(v, 2) for p, v in vals.items()})

    # The real archived lists, real scores.
    print("\n== Real archived lists (true scores, frozen model)")
    real_rows = []
    for ds in ARCHIVE_DATES:
        m = pd.Timestamp(ds)
        real = json.load(open(LISTS / f"watchlist_{ds}.json", encoding="utf-8"))
        ranked = [r["base"] for r in sorted(real["full_ranking"], key=lambda r: r["score"], reverse=True)]
        g = cw[cw["monday"] == m].copy()
        g["base"] = [strip_1000(base_of(p)) for p in g["perp"]]
        g = g.drop_duplicates("base").set_index("base")
        order = [b for b in ranked if b in g.index]
        if len(order) < 50:
            continue
        rec = {"list": ds, "n": len(order), "uni": (g.loc[order, "ret"] - g.loc[order, "fund"]).mean()}
        for k in KS:
            pick = order[:k]
            rec[f"long{k}"] = float((g.loc[pick, "ret_long_stop"] - g.loc[pick, "fund_long_stop"]).mean() - COST_RT)
            rec[f"short{k}"] = float((-g.loc[pick, "ret_short_stop"] + g.loc[pick, "fund_short_stop"]).mean() - COST_RT)
        real_rows.append(rec)
    R = pd.DataFrame(real_rows)
    print((R.set_index("list") * 100).drop(columns=["n"]).round(1).to_string() if len(R) else "none")
    if len(R):
        print("mean %:", (R.drop(columns=["list", "n"]).mean() * 100).round(2).to_dict())
    R.to_csv(out_dir / "real_lists.csv", index=False)


if __name__ == "__main__":
    main()
