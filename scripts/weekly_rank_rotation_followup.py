"""Follow-up to weekly_rank_rotation_study.py: 0 of 50 variants passed, so look at why.

1. Gated variants against random picks (the main run left their p-value blank).
2. The real archived lists with every listed coin priced, not only those in the rebuilt universe.
3. How fragile rank #1 is: rebuilt #1 against real #1 on the same dates.
4. POST split at 2026-07-20 (what the weights' own research saw vs after), best-coin dependence.
5. Exit rules for long #1 / top 3 (take-profit, stop): picked on PRE, checked on POST.
6. Position size: the arithmetic mean only matters if the size lets you survive the path.

Usage: python scripts/weekly_rank_rotation_followup.py <the study's output dir>
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

COST = study.COST_RT
RULES = [(None, None), (0.2, None), (0.4, None), (1.0, None), (None, 0.3), (0.2, 0.3), (0.4, 0.3), (1.0, 0.3)]


def load_bars(perp: str):
    folder = study.FUT / f"{study.base_of(perp)}_USDT"
    if not (folder / "1h.parquet").exists():
        return None
    bars = pd.read_parquet(folder / "1h.parquet", columns=["open", "high", "low", "close"])
    idx = pd.DatetimeIndex(bars.index)
    if idx.tz is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    bars.index = idx
    bars = bars[~bars.index.duplicated(keep="last")].sort_index()
    fund = None
    if (folder / "funding_rate.parquet").exists():
        fr = pd.read_parquet(folder / "funding_rate.parquet", columns=["funding_rate"]).sort_index()
        fi = pd.DatetimeIndex(fr.index)
        if fi.tz is not None:
            fi = fi.tz_convert("UTC").tz_localize(None)
        fund = pd.Series(fr["funding_rate"].to_numpy(dtype=float), index=fi)
    return bars, fund


def long_exit(seg: pd.DataFrame, entry: float, last_price: float, tp, sl):
    """Return (exit price, exit time). Same-bar TP and stop: the stop is assumed first."""
    tp_px = entry * (1 + tp) if tp is not None else None
    sl_px = entry * (1 - sl) if sl is not None else None
    for t, o, h, l in zip(seg.index, seg["open"].to_numpy(), seg["high"].to_numpy(), seg["low"].to_numpy()):
        if sl_px is not None and l <= sl_px:
            return min(sl_px, o), t
        if tp_px is not None and h >= tp_px:
            return max(tp_px, o), t
    return last_price, seg.index[-1] if len(seg) else None


def summary(values) -> dict:
    n = np.asarray(values, dtype=float)
    if not len(n):
        return {}
    eq = np.cumprod(1 + np.maximum(n, -1.0))
    peak = np.maximum.accumulate(eq)
    return {"mean%": 100 * n.mean(), "median%": 100 * np.median(n), "win": (n > 0).mean(), "t": n.mean() / (n.std(ddof=1) / math.sqrt(len(n))),
            "cagr": eq[-1] ** (52 / len(n)) - 1 if eq[-1] > 0 else -1.0, "maxdd": (eq / np.where(peak > 0, peak, 1) - 1).min()}


def main() -> None:
    out = Path(sys.argv[1])
    model = study.load_model_weights()
    universe = pd.read_parquet(study.PANEL / "universe.parquet") > 0.5
    features = study.build_features()
    study.STORE_END = pd.Timestamp(universe.index[-1]) + pd.Timedelta(days=1)
    cw = pd.read_parquet(out / "coin_weeks.parquet")
    panel = {}
    for m in pd.date_range(study.START, universe.index[-1], freq="W-MON"):
        day = m - pd.Timedelta(days=1)
        wk = study.score_week(features, model, day, universe.loc[day], "drop")
        joined = wk.join(cw[cw["monday"] == m].set_index("perp"), how="inner")
        if len(joined) >= 50:
            panel[m] = joined
    weeks = sorted(panel)
    periods = {p: [m for m in weeks if study.period_of(m) == p] for p in ("PRE", "TRAIN", "POST")}
    periods["POST_a"] = [m for m in periods["POST"] if m < pd.Timestamp("2026-07-20")]
    periods["POST_b"] = [m for m in periods["POST"] if m >= pd.Timestamp("2026-07-20")]
    pd.set_option("display.width", 220, "display.max_columns", 40, "display.max_rows", 200)

    print("== 1. Gated variants against random picks on the same long/short schedule (no stop, gross)")
    btc = pd.read_parquet(study.FUT / "BTC_USDT" / "1d.parquet", columns=["close"])["close"]
    btc.index = pd.DatetimeIndex(btc.index).normalize()
    btc = btc[~btc.index.duplicated(keep="last")]
    ma = btc.rolling(50).mean()
    rng = np.random.default_rng(5)
    for name in ("gate_self", "gate_btc"):
        for k in study.KS:
            if name == "gate_btc":
                gate = {m: ("long" if btc.asof(m - pd.Timedelta(days=1)) > ma.asof(m - pd.Timedelta(days=1)) else "short") for m in weeks}
            else:
                base = study.run_variant(panel, weeks, "long_x", k, False).set_index("monday")["gross"]
                gate = {m: ("long" if base.get(a, 0.0) > 0 else "short") for a, m in zip(weeks[:-1], weeks[1:])}
            res = study.run_variant(panel, weeks, "long", k, False, gate=gate).set_index("monday")
            line = f"{name} k={k:2d}"
            for p in ("PRE", "TRAIN", "POST"):
                wks = [m for m in periods[p] if m in res.index]
                total = np.zeros(2000)
                for m in wks:
                    arr = study.gross_per_coin(panel[m], gate[m], False).to_numpy(dtype=float)
                    arr = arr[np.isfinite(arr)]
                    total += arr[rng.integers(0, len(arr), size=(2000, k))].mean(axis=1)
                obs = res.loc[wks, "gross"].mean()
                pv = (np.sum(total / len(wks) >= obs) + 1) / 2001
                s = summary(res.loc[wks, "net"])
                line += f" | {p} net {s['mean%']:+.2f}% t {s['t']:+.2f} p {pv:.3f} cagr {s['cagr']:+.0%} dd {s['maxdd']:.0%}"
            print(line)

    print("\n== 2. Real archived lists, every listed coin priced (stop -30% long / +40% short, full turnover cost)")
    rows, firsts = [], []
    for ds in study.ARCHIVE_DATES:
        m = pd.Timestamp(ds)
        real = json.load(open(study.LISTS / f"watchlist_{ds}.json", encoding="utf-8"))
        ranked = [r["base"] for r in sorted(real["full_ranking"], key=lambda r: r["score"], reverse=True)]
        table = {}
        for base in ranked:
            got = study.coin_weeks({f"{base}USDT": [m]})
            if len(got):
                table[base] = got.iloc[0]
        g = pd.DataFrame(table).T
        for col in ("ret", "fund", "ret_long_stop", "fund_long_stop", "ret_short_stop", "fund_short_stop", "maxup", "maxdn"):
            g[col] = pd.to_numeric(g[col])
        order = [b for b in ranked if b in g.index]
        rec = {"list": ds, "priced": len(order), "uni%": 100 * (g["ret"] - g["fund"]).mean()}
        for k in study.KS:
            pick = order[:k]
            rec[f"L{k}"] = 100 * ((g.loc[pick, "ret"] - g.loc[pick, "fund"]).mean() - COST)
            rec[f"S{k}"] = 100 * ((-g.loc[pick, "ret"] + g.loc[pick, "fund"]).mean() - COST)
        rows.append(rec)
        one = g.loc[order[0]]
        mine = panel[m].nsmallest(1, "rank").iloc[0] if m in panel else None
        firsts.append({"list": ds, "real#1": order[0], "ret%": 100 * one["ret"], "high%": 100 * one["maxup"], "low%": 100 * one["maxdn"],
                       "rebuilt#1": panel[m].nsmallest(1, "rank").index[0], "rebuilt_ret%": 100 * float(mine["ret"]) if mine is not None else float("nan")})
    R = pd.DataFrame(rows)
    print(R.round(1).to_string(index=False))
    print("mean:", R.drop(columns=["list", "priced"]).mean().round(2).to_dict())
    print("(no stop here; the main run's table used the stop variants)")
    F = pd.DataFrame(firsts)
    print("\n== 3. Rank #1 is fragile: same model, same date, slightly different inputs")
    print(F.round(1).to_string(index=False))
    print(f"same coin {int((F['real#1'] == F['rebuilt#1'].str.replace('USDT', '')).sum())}/{len(F)}; "
          f"mean 7d return real #1 {F['ret%'].mean():+.1f}%, rebuilt #1 {F['rebuilt_ret%'].mean():+.1f}%")

    print("\n== 4. POST split and best-coin dependence (long, no stop)")
    for k in (1, 3, 5, 15):
        res = study.run_variant(panel, weeks, "long", k, False).set_index("monday", drop=False)
        parts = []
        for p in ("PRE", "TRAIN", "POST_a", "POST_b"):
            sub = res.loc[periods[p]]
            s = summary(sub["net"])
            parts.append(f"{p} {s['mean%']:+.2f}% med {s['median%']:+.1f}% cagr {s['cagr']:+.0%}")
        totals: dict[str, float] = {}
        for c in res["contrib"]:
            for coin, value in c.items():
                totals[coin] = totals.get(coin, 0.0) + float(value)
        top = sorted(totals, key=totals.get, reverse=True)[:5]
        print(f"long#{k}: " + " | ".join(parts))
        print(f"   total {res['net'].sum():+.1f}, best five {[(c[:-4], round(totals[c], 1)) for c in top]}")

    print("\n== 5. Exit rules, long, hold up to 7 days (rule = take-profit/stop)")
    picks: dict[str, list] = {}
    for m in weeks:
        for perp in panel[m].nsmallest(3, "rank").index:
            picks.setdefault(perp, []).append(m)
    records = []
    for perp, mondays in sorted(picks.items()):
        loaded = load_bars(perp)
        if loaded is None:
            continue
        bars, fund = loaded
        for m in mondays:
            t0, t1 = m + pd.Timedelta(hours=1), m + pd.Timedelta(hours=1, days=7)
            seg = bars.loc[t0:t1 - pd.Timedelta(hours=1)]
            if seg.empty:
                continue
            entry = float(seg["open"].iloc[0])
            after = bars.loc[t1:t1 + pd.Timedelta(hours=3)]
            last = float(after["open"].iloc[0]) if len(after) else float(seg["close"].iloc[-1])
            for tp, sl in RULES:
                px, when = long_exit(seg, entry, last, tp, sl)
                paid = float(fund.loc[(fund.index > t0) & (fund.index <= (when if when is not None else t1))].sum()) if fund is not None else 0.0
                records.append({"monday": m, "perp": perp, "rank": int(panel[m].loc[perp, "rank"]), "tp": tp, "sl": sl, "r": px / entry - 1 - paid})
    X = pd.DataFrame(records)
    X["tp"] = X["tp"].fillna(0.0)
    X["sl"] = X["sl"].fillna(0.0)
    table = []
    for k in (1, 3):
        for (tp, sl), g in X[X["rank"] <= k].groupby(["tp", "sl"]):
            wk = g.groupby("monday")["r"].mean() - COST
            rec = {"k": k, "tp": tp, "sl": sl}
            for p in ("PRE", "TRAIN", "POST"):
                s = summary(wk.reindex([m for m in periods[p] if m in wk.index]))
                rec[f"{p}_mean%"] = s["mean%"]
                rec[f"{p}_t"] = s["t"]
                rec[f"{p}_cagr"] = s["cagr"]
            table.append(rec)
    E = pd.DataFrame(table)
    print(E.round(2).to_string(index=False))
    best = E.sort_values("PRE_t", ascending=False).head(3)
    print("picked on PRE t:", best[["k", "tp", "sl", "PRE_t", "POST_mean%", "POST_t"]].round(2).to_dict("records"))

    print("\n== 6. Position size: compound growth per year if only this fraction of the account goes in each week (net, no stop)")
    for k in (1, 3):
        res = study.run_variant(panel, weeks, "long", k, False).set_index("monday", drop=False)
        for p in ("PRE", "POST"):
            n = res.loc[periods[p], "net"].to_numpy()
            line = []
            for f in (1.0, 0.5, 0.25, 0.1, 0.05):
                eq = np.cumprod(1 + np.maximum(f * n, -1.0))
                line.append(f"{f:.0%}: {eq[-1] ** (52 / len(n)) - 1:+.1%}/yr dd {(eq / np.maximum.accumulate(eq) - 1).min():.0%}")
            print(f"long#{k} {p}: " + " | ".join(line))


if __name__ == "__main__":
    main()
