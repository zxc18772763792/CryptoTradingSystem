"""Independent arithmetic on frozen account paths and the original tracker evidence."""
from collections import Counter
from itertools import product
from pathlib import Path
from statistics import mean, stdev
import hashlib
import json
import math
import pickle

import pandas as pd

P = Path(r"C:\Users\zxc\AppData\Local\Temp\claude\93bc013aacae6232\d7a802af-aec4-4746-bd5c-659cad3a9a92\scratchpad\team_mining\round2")
O = Path("reports/tracker_data_audit_20261106_0215/event_portfolio")
R = Path("data/research")
START, END = pd.Timestamp("2025-01-01"), pd.Timestamp("2026-11-03")
FWD = pd.Timestamp("2026-09-28")
DAY = pd.Timedelta(days=1)


class ResearchUnpickler(pickle.Unpickler):
    allowed = {
        ("builtins", "slice"),
        ("numpy", "dtype"),
        ("numpy", "ndarray"),
        ("numpy.core.multiarray", "_reconstruct"),
        ("numpy.core.multiarray", "scalar"),
        ("numpy.core.numeric", "_frombuffer"),
        ("pandas._libs.arrays", "__pyx_unpickle_NDArrayBacked"),
        ("pandas._libs.tslibs.timestamps", "_unpickle_timestamp"),
        ("pandas._libs.internals", "_unpickle_block"),
        ("pandas.core.arrays.datetimes", "DatetimeArray"),
        ("pandas.core.indexes.base", "_new_Index"),
        ("pandas.core.indexes.base", "Index"),
        ("pandas.core.indexes.datetimes", "_new_DatetimeIndex"),
        ("pandas.core.indexes.datetimes", "DatetimeIndex"),
        ("pandas.core.indexes.range", "RangeIndex"),
        ("pandas.core.internals.managers", "SingleBlockManager"),
        ("pandas.core.internals.managers", "BlockManager"),
        ("pandas.core.series", "Series"),
        ("pandas.core.frame", "DataFrame"),
    }

    def find_class(self, module, name):
        if (module, name) not in self.allowed:
            raise pickle.UnpicklingError(f"Unexpected archive reference: {module}.{name}")
        return super().find_class(module, name)


def load_pickle(path):
    with path.open("rb") as f:
        return ResearchUnpickler(f).load()


def trade_net(t):
    return float(t["daily"].sum() + t["hedge"].sum())


def metrics(returns):
    eq = 1.0
    peak = 1.0
    mdd = 0.0
    monthly = {}
    years = {}
    for day, ret in returns.items():
        eq *= 1 + ret
        peak = max(peak, eq)
        mdd = min(mdd, eq / peak - 1)
        m = day.strftime("%Y-%m")
        y = str(day.year)
        monthly[m] = monthly.get(m, 1.0) * (1 + ret)
        years[y] = years.get(y, 1.0) * (1 + ret)
    n = len(returns)
    return {
        "days": n, "sharpe": mean(returns) / stdev(returns) * math.sqrt(365),
        "cagr_pct": 100 * (eq ** (365 / n) - 1),
        "mdd_pct": 100 * mdd, "total_pct": 100 * (eq - 1),
        "positive_months": sum(v > 1 for v in monthly.values()), "months": len(monthly),
        "worst_month": min(monthly, key=monthly.get), "worst_month_pct": 100 * (min(monthly.values()) - 1),
        "year_pct": {k: 100 * (v - 1) for k, v in years.items()},
    }


def overlap(trades, floor, streams):
    order = {"U": 0, "K": 1, "C": 2}
    candidates = [t for t in trades if t["stream"] in streams and (t["stream"] != "U" or t["size_pct"] >= floor)]
    candidates.sort(key=lambda t: (t["entry_time"], order[t["stream"]], t["perp"]))
    active_until = {}
    result = []
    for t in candidates:
        if active_until.get(t["perp"], pd.Timestamp.min) <= t["entry_time"]:
            result.append(t)
            active_until[t["perp"]] = t["exit_time"]
    return result


def simulate(trades, hedged=True, vol_center=None, pnl_override=None):
    days = pd.date_range(START, END)
    starts = {}
    eq = 1.0
    on = []
    daily = {}
    gross = []
    skipped = 0
    for t in trades:
        starts.setdefault(t["entry_time"].normalize(), []).append(t)
    for day in days:
        for t in sorted(starts.get(day, []), key=lambda x: (x["stream"], x["perp"])):
            weight = min(2, vol_center / t["vol"]) if vol_center and t["vol"] > 0 else 1
            notional = .05 * weight * eq
            if (2 if hedged else 1) * (notional + sum(q for q, _, _ in on)) > eq:
                skipped += 1
                continue
            net = (pnl_override[id(t)] if pnl_override is not None else
                   t["daily"].add(t["hedge"], fill_value=0) if hedged else t["daily"])
            on.append((notional, net, t["daily"].index.max()))
        pnl = sum(q * float(s.get(day, 0)) for q, s, _ in on)
        gross.append((2 if hedged else 1) * sum(q for q, _, _ in on) / eq)
        daily[day] = pnl / eq
        eq += pnl
        on = [p for p in on if day < p[2]]
    return metrics(pd.Series(daily)) | {"avg_gross_pct": mean(gross) * 100,
                                       "max_gross_pct": max(gross) * 100, "taken": len(trades) - skipped}


def summary(trades):
    v = [trade_net(t) for t in trades]
    signed = sum(math.fsum(s*x for s, x in zip(sign, v)) >= math.fsum(v) - 1e-12 for sign in product((-1, 1), repeat=len(v)))
    return {"n": len(v), "mean_pct": mean(v)*100, "exact_sign_flip_p": signed / (2 ** len(v)),
            "without_top_2_mean_pct": mean(sorted(v)[:-2])*100, "independent_entry_dates": len({t["entry_day"] for t in trades})}


def load_state(name):
    return json.loads((R / name / "tracker.json").read_text(encoding="utf-8"))["trades"]


def pct_gap(px, entry):
    return 100 * (px / entry - 1)


trades = load_pickle(P / "trades_hedged.pkl")
hold = [t for t in trades if t["entry_day"] >= START]
ex_bad = lambda rows: [t for t in rows if (t["stream"], t["token"]) not in (("K","CT"),("K","PONS"))]
center = json.loads((P / "dev_results.json").read_text(encoding="utf-8"))["median_vol"]
out = {"archive_sha256": hashlib.sha256((P / "trades_hedged.pkl").read_bytes()).hexdigest(),
       "account": {}, "forward": {}, "forward_members": [], "book_lag": []}
for label, floor, hedge, sizing in (("base_U5_H0_S1",5,False,None),("U10_H1_S2",10,True,center),("U10_H1_S1",10,True,None)):
    pick = overlap(hold, floor, "UCK")
    out["account"][label] = {"original": simulate(pick, hedge, sizing), "ex_CT_PONS": simulate(ex_bad(pick), hedge, sizing)}
    csv = pd.read_csv(P / f"daily_holdout_{label}.csv", index_col=0, parse_dates=True).iloc[:,0]
    out["account"][label]["csv"] = metrics(csv)
post = [t for t in overlap(ex_bad(hold), 10, "UCK") if t["stream"] != "C"]
pure = overlap(ex_bad(hold), 10, "UK")
out["account"]["post_hoc_U10_K"] = simulate(post)
out["account"]["post_hoc_U10_K"]["per_trade_pct"] = 100 * mean(trade_net(t) for t in post)
out["account"]["pure_U10_K_no_caution_suppression"] = simulate(pure)
out["account"]["post_hoc_ex_APR"] = simulate([t for t in post if t["token"] != "APR"])
ranked = sorted(post, key=trade_net)
out["account"]["post_hoc_ex_top20"] = simulate(ranked[:-20]) | {
    "mean_trade_pct": 100 * mean(trade_net(t) for t in ranked[:-20]),
    "top20_trade_pnl_share": sum(trade_net(t) for t in ranked[-20:]) / sum(trade_net(t) for t in ranked),
}
f = [t for t in ex_bad(trades) if t["entry_day"] >= FWD]
fk = [t for t in f if t["stream"] == "K" or (t["stream"] == "U" and t["size_pct"] >= 10)]
out["forward"] = {"original": summary(f), "U10_K": summary(fk), "U10_K_ex_APR": summary([t for t in fk if t["token"] != "APR"])}
for stream, name in (("U", "unlock_short_t7"), ("K", "upbit_krw_listing"), ("C", "upbit_caution")):
    state = load_state(name)
    for t in f:
        if t["stream"] != stream:
            continue
        candidates = []
        for key, v in state.items():
            raw = v.get("unlock_date") or v.get("notice_at")
            stamp = pd.Timestamp(raw).date()
            if stamp == t["day"].date() and v.get("token",v.get("ticker")) == t["token"]:
                candidates.append((key,v))
        book = None
        case = {}
        if len(candidates) == 1:
            key, v = candidates[0]
            d = pd.Timestamp(v["discovered_at"]).tz_localize(None)
            book = (v.get("evidence") or {}).get("book_at_entry")
            case = {"key": key, "discovered_utc": d.isoformat(), "was_known_before": bool(d < t["entry_time"]),
                    "late": v.get("late"), "backfill": v.get("backfilled"), "status": v.get("status"),
                    "tracker_return_pct": v.get("return_pct",v.get("short_return_pct")),
                    "tracker_entry": v.get("entry_price")}
        out["forward_members"].append({"stream": stream, "token": t["token"], "source": t["source"],
                                       "event_utc": t["day"].isoformat(), "entry_utc": t["entry_time"].isoformat(),
                                       "exit_utc": t["exit_time"].isoformat(), "entry": t["entry_px"],
                                       "replay_pct": trade_net(t)*100, **case})
        if stream=="K" and book:
            bid,ask = book.get("bid",book.get("best_bid")),book.get("ask",book.get("best_ask"))
            if bid is None or ask is None:
                out["book_lag"].append({"token":t["token"],"fields":list(book)})
                continue
            mid = (bid + ask) / 2
            out["book_lag"].append({"token":t["token"],"gap_pct":pct_gap(mid,t["entry_px"]),
                                   "lag_s":book.get("lag_after_entry_close_sec"),"bid_usdt_1pct":book.get("bid_usdt_1pct")})
(O / "independent_recalc.json").write_text(json.dumps(out,indent=2),encoding="utf-8")
for k, v in out["account"].items():
    print(k, v)
print("forward",out["forward"])
print("sources",Counter((v["stream"],v["source"],v.get("was_known_before"),v.get("late"),v.get("backfill")) for v in out["forward_members"]))
print("lag",out["book_lag"])
