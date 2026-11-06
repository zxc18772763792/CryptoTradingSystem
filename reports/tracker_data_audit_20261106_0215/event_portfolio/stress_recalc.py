"""Reprice cap, funding, impact, timing, and prior-market sensitivities from local archives."""
from functools import lru_cache
from pathlib import Path
import json
import math
import numpy as np
import pandas as pd

source = Path("reports/tracker_data_audit_20261106_0215/event_portfolio/independent_recalc.py")
prefix = source.read_text(encoding="utf-8-sig").split("\ntrades = load_pickle", 1)[0]
exec(compile(prefix, str(source), "exec"))

HOUR = pd.Timedelta(hours=1)
PANEL = R / "xs_daily_panel"
M = Path("data/historical/binance_futures")
STABLE = {"USDC", "FDUSD", "TUSD", "DAI", "USDP", "BUSD", "USDE", "USD1", "BFUSD", "EUR", "PYUSD", "RLUSD", "USDT"}
panel = {k: pd.read_parquet(PANEL / f"col_{k}.parquet").astype(float) for k in ("close","funding","volume")}
close, volume, rate = (panel[k] for k in ("close", "volume", "funding"))
ret = close.pct_change(fill_method=None)
sigma = ret.rolling(30, min_periods=10).std()
index_r = ret.where(volume.shift(1).rank(axis=1, ascending=False) <= 30).mean(axis=1)
level = (1 + index_r.fillna(0)).cumprod()
prior = level / level.shift(60) - 1
raw = load_pickle(P / "trades_hedged.pkl")
clean = [x for x in raw if not (x["stream"] == "K" and x["token"] in ("CT","PONS"))]
pool = [t for t in overlap(clean,10,"UCK") if t["stream"] != "C"]
hold = [t for t in pool if t["entry_day"] >= START]
no_apr = [t for t in hold if t["token"] != "APR"]
fmt = lambda x: float(x)
def regime(r):
    if pd.isna(r):
        return "missing"
    return "down" if r <= -.15 else "up" if r > .15 else "flat"
phase = pd.DataFrame([{"type": x["stream"], "ticker": x["token"], "date": x["entry_day"], "state": regime(prior.get(x["entry_day"])), "hedged_pct": trade_net(x)*100} for x in pool])
out = {"regime": {}, "steps": {},
       "index_by_year_pct_to_cutoff": {int(y): 100 * (float((1+g.dropna()).prod())-1) for y,g in index_r.loc[:END].groupby(index_r.loc[:END].index.year)}}
for (g,s), part in phase.groupby(["type","state"]):
    vals = part.hedged_pct.tolist()
    parts = [g.hedged_pct.tolist() for _,g in part.groupby("ticker")]
    rng = np.random.default_rng(7)
    boot = []
    for indices in rng.integers(0,len(parts),size=(5000,len(parts))):
        a = [p for i in indices for p in parts[i]]
        boot.append(mean(a))
    out["regime"][f"{g}_{s}"] = {"n":len(part),"mean_pct":mean(vals),"coin_clusters":len(parts),
        "coin_bootstrap_tail_probability":(1+sum(b<=0 for b in boot))/(1+len(boot))}
out["holdout_down_share"] = float((phase.loc[phase.date>=START,"state"]=="down").mean())
out["regime_one_day_early"] = {}
for g, d in phase.groupby("type"):
    for label, part in d.groupby(d.date.map(lambda day: regime(prior.shift(1).get(day)))):
        out["regime_one_day_early"][f"{g}_{label}"] = {"n":len(part),"mean_pct":float(part.hedged_pct.mean())}

@lru_cache(maxsize=None)
def market(symbol, field):
    base = M / (symbol[:-4] + "_USDT")
    files = [base / f"{field}.parquet"]
    if field == "1h":
        files += sorted((base/"1h_parts").glob("*.parquet"))
    columns = ["open","high","close"] if field == "1h" else ["funding_rate","mark_price"]
    pieces = [pd.read_parquet(f,columns=columns) for f in files if f.exists()]
    if not pieces:
        return pd.DataFrame(columns=columns,index=pd.DatetimeIndex([]))
    df = pd.concat(pieces)
    df.index = pd.to_datetime(df.index,utc=True).tz_convert(None)
    return df[~df.index.duplicated(keep="last")].sort_index()

def basket(t, lag=0):
    day = t["entry_day"]
    top = volume.loc[day - lag*DAY].dropna().sort_values(ascending=False)
    return [c for c in top.index if c != t["perp"] and c[:-4] not in STABLE and pd.notna(close.at[day,c])][:30]

def long_path(t, members, cap=None, capped_funding=True):
    days = t["daily"].index
    closes = close.loc[[t["entry_day"]]+[d for d in days if d in close.index],members].ffill()
    entry = closes.iloc[0]
    raw_rel = closes.div(entry)
    rel = raw_rel.clip(upper=cap+1) if cap else raw_rel
    fund = rate.reindex(index=closes.index[1:],columns=members).fillna(0)
    paid = fund * (rel if capped_funding else raw_rel).iloc[1:]
    out = (rel.diff().iloc[1:] - paid).mean(axis=1).reindex(days).fillna(0)
    out.iloc[0] -= .0005
    out.iloc[-1] -= .0005
    return out

def net_cost(t, members, y=1, cap=200000):
    q = .05 * cap
    d, b = t["entry_day"], t["day"] + (-1 if t["stream"]=="U" else 7)*DAY
    val = 0.0
    for date in (d,b):
        if date not in volume.index:
            continue
        def one(symbol, amount):
            v, s = volume.at[date,symbol], sigma.at[d,symbol]
            return y*s*math.sqrt(amount/v) if pd.notna(v) and pd.notna(s) and v>0 else None
        short = one(t["perp"],q)
        if short is not None:
            val += short
        good = [one(m,q/len(members)) for m in members]
        good = [x for x in good if x is not None]
        if good:
            val += mean(good)
    return val

def cash(t, lo, hi, basis, inclusive=False):
    d = market(t["perp"],"funding_rate")
    start = d.index >= lo if inclusive else d.index > lo
    sel = d[start & (d.index<=hi)]
    return float((sel.funding_rate * sel.mark_price.fillna(basis)).sum()) / basis

def delayed_coin_total(t, h=1):
    bars = market(t["perp"],"1h")
    start = t["entry_time"] + h * HOUR
    entry = bars.close.get(start - HOUR)
    if entry is None:
        return None
    plan_end = t["day"] + (0 if t["stream"]=="U" else 8)*DAY
    part = bars[(bars.index>=start)&(bars.index<plan_end)]
    if part.empty:
        return None
    triggers = part[part.high >= 1.40*entry]
    if not triggers.empty:
        exit_ = triggers.index[0] + HOUR
        price = 1.02 * max(1.40*entry,triggers.open.iloc[0])
    else:
        exit_ = part.index[-1] + HOUR
        price = part.close.iloc[-1]
    minute = pd.Timedelta(minutes=1)
    return (entry-price)/entry + cash(t,start+minute,exit_+minute,entry) - .002

out["steps"]["base"] = simulate(hold)
out["steps"]["no_APR"] = simulate(no_apr)
maps = {k:{} for k in ("cap","cap_marks_unclipped","boundary","impact_200k_Y1","impact_200k_Y1p5","impact_200k_Y1_then_1h","prior_day_rank")}
impact_cost = []
funding_adj = {"U":[],"K":[]}
cap_gaps = []
delays = []
for t in no_apr:
    oid = id(t)
    m = basket(t)
    day = t["entry_day"]
    daily = t["daily"]
    start, end = t["entry_time"], t["exit_time"]
    f0 = cash(t,start-pd.Timedelta(seconds=90),start+pd.Timedelta(seconds=90),t["entry_px"],True)
    f1 = cash(t,end-pd.Timedelta(seconds=90),end+pd.Timedelta(seconds=90),t["entry_px"],True) if t["reason"]=="time" else 0
    funding_adj[t["stream"]].append((f0,f1))
    for key, hedge in (("cap",long_path(t,m,1)),("cap_marks_unclipped",long_path(t,m,1,False)),
                       ("prior_day_rank",long_path(t,basket(t,1)))):
        maps[key][oid] = daily.add(hedge,fill_value=0)
    cap_gaps.append((t["token"],str(day.date()),float(daily.sum()+t["hedge"].sum()-maps["cap"][oid].sum())*100))
    consistent = maps["cap"][oid].copy()
    consistent.iloc[0] += f0
    consistent.iloc[-1] -= f1
    maps["boundary"][oid] = consistent
    impact = net_cost(t,m)
    impact_cost.append(impact)
    for key,cost in (("impact_200k_Y1",impact),("impact_200k_Y1p5",1.5*impact)):
        costs = consistent.copy()
        costs.iloc[0] -= cost/2
        costs.iloc[-1] -= cost/2
        maps[key][oid] = costs
    late = maps["impact_200k_Y1"][oid].copy()
    r = delayed_coin_total(t)
    h_change = []
    if r is not None:
        p0 = close.loc[day]
        p1 = close.loc[[d for d in daily.index if d in close.index][-1]]
        for member in m:
            h = market(member,"1h").close.get(start)
            if h is not None and pd.notna(p1[member]) and pd.notna(h):
                h_change.append(p1[member] / h - p1[member] / p0[member])
        shift = r - float(daily.sum()) + (mean(h_change) if h_change else 0)
        late.iloc[0] += shift
        delays.append((t["token"],str(day.date()),float(shift)*100))
    maps["impact_200k_Y1_then_1h"][oid] = late
for label, pnl in maps.items():
    out["steps"][label] = simulate(no_apr,pnl_override=pnl) | {"mean_trade_pct":100*mean(float(v.sum()) for v in pnl.values())}
out["funding_shift_pp"] = {k:100*mean(a-b for a,b in v) for k,v in funding_adj.items()}
out["mean_impact_pct"] = 100*mean(impact_cost)
out["top_cap_diffs"] = sorted(cap_gaps,key=lambda x:x[-1],reverse=True)[:6]
out["top_delay_losses"] = sorted(delays,key=lambda x:x[-1])[:6]
(O / "stress_recalc.json").write_text(json.dumps(out,indent=2),encoding="utf-8")
for k in ("regime","regime_one_day_early","holdout_down_share","index_by_year_pct_to_cutoff","funding_shift_pp","mean_impact_pct","top_cap_diffs","top_delay_losses"):
    print(k,out[k],flush=True)
print("stress", {k:(v["sharpe"],v["cagr_pct"]) for k,v in out["steps"].items()})
