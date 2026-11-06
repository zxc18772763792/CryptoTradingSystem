"""Recompute saved tracker returns and tie selected trades to the local market archive."""
import json
from pathlib import Path
from statistics import mean

import pandas as pd

R = Path("data/research")
H = Path("data/historical")
O = Path("reports/tracker_data_audit_20261105/others")
M = {}
def state(name, key):
    return json.loads((R / name / "tracker.json").read_text(encoding="utf-8"))[key]
def pct(trade):
    return 100 * (1 - trade["exit_price"] / trade["entry_price"])
def archive(trade, entry, exit_, include_start=False):
    folder = H / "binance_futures" / (trade["symbol"][:-4] + "_USDT")
    dates = pd.read_parquet(folder / "1d.parquet")
    funds = pd.read_parquet(folder / "funding_rate.parquet")
    after_entry = funds.index >= entry if include_start else funds.index > entry
    selected = funds[after_entry & (funds.index <= exit_)]
    return {
        "entry_utc": entry.isoformat(), "exit_utc": exit_.isoformat(),
        "archived_entry": float(dates.loc[entry - pd.Timedelta(days=1), "close"]),
        "archived_exit_close": float(dates.loc[exit_ - pd.Timedelta(days=1), "close"]),
        "funding_count": len(selected),
        "funding_pct": float(selected.funding_rate.sum() * 100),
        "cash_funding_pct": float((selected.funding_rate * selected.mark_price).sum() * 100 / trade["entry_price"]),
    }
def legs(xs, sign):
    return mean(sign * (p["exit"] / p["entry"] - 1 - p["funding"]) for p in xs.values())
def timing(xs, sign):
    return mean(sign * (p["exit"] / p["mark_at_formation"] - 1 - p["funding"]) for p in xs.values())
def funded_trade(trade):
    return trade["status"] in ("closed", "stopped") and trade.get("return_pct") is not None
xs = {}
for day, t in sorted(state("xs_reversal", "days").items()):
    if t["status"] != "closed" or t.get("late") or t.get("missing_positions"):
        continue
    long, short = t["longs"], t["shorts"]
    base = (legs(long, 1) + legs(short, -1)) * 50 - .15
    row = {"recomputed": base, "saved": t["net_per_position_pct"], "n_legs": len(long) + len(short)}
    if all(p.get("mark_at_formation") for p in list(long.values()) + list(short.values())):
        row["formation_mark_proxy"] = (timing(long, 1) + timing(short, -1)) * 50 - .15
    sh = t.get("liq_filter", {})
    if sh.get("status") == "closed":
        row["shadow_diff"] = (legs(sh["longs"], 1) - legs(long, 1)) * 50
        row["saved_shadow_diff"] = sh["diff_pct"]
    xs[day] = row
paired = [r["shadow_diff"] for r in xs.values() if "shadow_diff" in r]
formation = [r for r in xs.values() if "formation_mark_proxy" in r]
M["xs"] = {
    "days": len(xs), "legs": sum(r["n_legs"] for r in xs.values()),
    "net_pct": mean(r["recomputed"] for r in xs.values()),
    "max_abs_error_pp": max(abs(r["recomputed"] - r["saved"]) for r in xs.values()),
    "mark_days": len(formation), "mark_net_proxy": mean(r["formation_mark_proxy"] for r in formation),
    "mark_days_base_net": mean(r["recomputed"] for r in formation),
    "shadow_n": len(paired), "shadow_diff": mean(paired),
    "shadow_first7_diff": mean(paired[:7]), "shadow_after7_diff": mean(paired[7:]),
    "shadow_max_abs_error_pp": max(abs(r["shadow_diff"] - r["saved_shadow_diff"]) for r in xs.values() if "shadow_diff" in r),
    "daily": xs,
}

for name, hedge in [("upbit_krw_listing", False), ("upbit_caution", False),
                    ("upbit_caution_hedged", True), ("bithumb_caution", False),
                    ("bithumb_caution_hedged", True), ("korea_caution_hedged", True)]:
    rows = {}
    trades = state(name, "trades")
    for k, t in trades.items():
        if not funded_trade(t) or t.get("backfilled") or t.get("late"):
            continue
        if hedge and t.get("hedged_return_pct") is None:
            continue
        row = {"short_recomputed_pct": pct(t) - .1 + 100 * t["funding"], "short_saved_pct": t["return_pct"]}
        if hedge:
            h = t["hedge"]
            row.update(saved=t["hedged_return_pct"], recomputed=t["return_pct"] + h["return_pct"] - h["funding_pct"] - h["fee_pct"],
                       matched=h["legs"], planned=len(t["evidence"]["market_basket"]["symbols"]))
        else:
            row.update(saved=t["return_pct"], recomputed=row["short_recomputed_pct"])
        if name == "upbit_krw_listing" or k == "INIT|6708":
            try:
                row["archive"] = archive(t, pd.Timestamp(t["entry_at"], unit="ms"), pd.Timestamp(t["exit_at"], unit="ms"))
            except (FileNotFoundError, KeyError) as exc:
                row["archive_gap"] = str(exc)
        rows[k] = row
    M[name] = {"n": len(rows), "saved_mean_pct": mean(r["saved"] for r in rows.values()),
               "max_abs_error_pp": max(abs(r["saved"] - r["recomputed"]) for r in rows.values()),
               "no_perp": sum(t["status"] == "no_perp" for t in trades.values()), "trades": rows}

rows = {}
for k, t in state("unlock_short_t7", "trades").items():
    if t.get("hedged_return_pct") is None or t.get("late"):
        continue
    entry = pd.Timestamp(t["entry_day"]) + pd.Timedelta(days=1)
    exit_ = pd.Timestamp(t["exit_day_actual"]) + pd.Timedelta(days=1)
    row = {"size": t["size_pct"], "short_saved_pct": t["short_return_pct"],
           "short_recomputed_pct": pct(t) - .2 + t["funding_pct"], "hedged": t["hedged_return_pct"],
           "basket_n": t["basket_n"], "basket_expected_n": len(t["basket"]), "archive": archive(t, entry, exit_, include_start=True)}
    hedge = t.get("hedge_settlement")
    if hedge and hedge.get("resolved"):
        row["hedge_recomputed_pct"] = hedge["short_return_pct_unrounded"] + 100 * mean(hedge["resolved"].values())
    rows[k] = row
dev = pd.read_csv(R / "unlock_study/short_backtest_-7_-1_5.csv").dropna(subset=["basket_hedged"])
size_bins = {}
for lo, hi in ((5, 10), (10, 1000), (5, 7), (7, 10), (10, 20), (20, 1000)):
    d = dev[(dev.size_pct >= lo) & (dev.size_pct < hi)]
    size_bins[f"{lo}-{hi}"] = {"n": len(d), "trade_mean_pct": float(d.basket_hedged.mean() * 100),
                              "batches": d.entry.nunique(), "batch_mean_pct": float(d.groupby("entry").basket_hedged.mean().mean() * 100)}
M["unlock_t7"] = {"trades": rows, "n": len(rows), "saved_mean_pct": mean(r["hedged"] for r in rows.values()),
                  "without_apr_pct": mean(r["hedged"] for k, r in rows.items() if not k.startswith("APR|")),
                  "big_pct": mean(r["hedged"] for r in rows.values() if r["size"] >= 10),
                  "small_pct": mean(r["hedged"] for r in rows.values() if r["size"] < 10), "development_bins": size_bins}
M["supply"] = {}
for day, t in state("supply_factor", "months").items():
    rec = {"backfill": t["backfill"], "status": t["status"], "missing_entry": {}}
    returned = {}
    for leg in ("long", "short"):
        rec[leg + "_intended"] = len(t[leg])
        rec["missing_entry"][leg] = [k for k, p in t[leg].items() if not p.get("entry")]
        vals = [(k, p["exit"] / p["entry"] - 1) for k, p in t[leg].items() if p.get("entry") and p.get("exit")]
        rec[leg + "_settled"] = len(vals)
        if vals:
            returned[leg] = mean(v for _, v in vals)
    if returned:
        rec.update(recomputed_pct=(returned["long"] - returned["short"]) * 100 - .4, saved_pct=t["spread_pct"])
        rec["missing_long_as_cash_pct"] = (returned["long"] * rec["long_settled"] / rec["long_intended"] - returned["short"]) * 100 - .4
    M["supply"][day] = rec
(O / "saved_ledger_recalc.json").write_text(json.dumps(M, indent=2), encoding="utf-8")
for k, v in M.items():
    print(k, {a: b for a, b in v.items() if a not in ("daily", "trades")})
