"""Buyback yield and net supply vs next-month returns (round 5 follow-up).

2026-09-27 finding (47 tokens with revenue history + Binance price, ~25 a
month, 44 months; market cap approximated as price x today's circulating
supply): long high / short low buyback yield -1.6%/month [-4.2, +1.2], rank IC
right-signed in only 20/44 months (p=0.77) - no effect, likely because the
"real yield" narrative is already priced. On the overlap with scheduled
tokens, net supply (growth minus 90-day buybacks) is no better than supply
growth alone (27/44 vs 29/44 months right-signed). Run buyback_fetch.py and
scripts/unlock_event_study.py + scripts/delist_risk_fetch.py first.
"""
import json, sys, numpy as np, pandas as pd
from math import comb
sys.path.insert(0,".")
from core.research.unlock_events import unlocked_series, entry_ticker
REV=pd.read_parquet("data/research/buyback/holders_revenue.parquet"); REV.index=pd.to_datetime(REV.index,utc=True); REV=REV.asfreq("1D")
meta=json.load(open("data/research/buyback/meta.json")); circ={t:meta["circ"].get(g) for t,g in meta["tokmap"].items()}
C=pd.read_parquet("data/research/delist_risk/close.parquet"); C.index=pd.to_datetime(C.index,utc=True); C=C.asfreq("1D")
toks=[t for t in REV.columns if t in C.columns and circ.get(t)]
first={t:REV[t].first_valid_index() for t in toks}
print("tokens with revenue history + Binance price + supply:",len(toks))
# schedules for the net-supply comparison
idx=json.load(open("data/research/unlock_study/emissions_index.json",encoding="utf-8")); idx=idx["data"] if isinstance(idx,dict) else idx
unl={}
for e in idx:
    t=entry_ticker(e)
    if t in unl: continue
    try: unl[t]=unlocked_series(json.load(open(f"data/research/unlock_study/emissions/{e['protocolSlug']}.json",encoding="utf-8"))).asfreq("1D").ffill()
    except Exception: pass
def sp(a,b):
    x=a.rank().to_numpy(float); y=b.rank().to_numpy(float); x-=x.mean(); y-=y.mean(); d=np.sqrt((x*x).sum()*(y*y).sum()); return (x*y).sum()/d if d else np.nan
rows=[]
for D in pd.date_range("2023-01-01","2026-08-01",freq="MS",tz="UTC"):
    end=D+pd.Timedelta("30D")
    for t in toks:
        if first[t] is None or first[t]>D-pd.Timedelta("30D"): continue
        if D not in C.index or end not in C.index or pd.isna(C.at[D,t]) or pd.isna(C.at[end,t]): continue
        rev30=REV[t].loc[D-pd.Timedelta("30D"):D-pd.Timedelta("1D")].fillna(0).sum()
        mcap=C.at[D,t]*circ[t]
        infl=np.nan
        u=unl.get(t)
        if u is not None and D in u.index and D+pd.Timedelta("90D") in u.index and u[D]>0: infl=u[D+pd.Timedelta("90D")]/u[D]-1
        rows.append({"date":D,"tok":t,"yield":rev30*12/mcap,"ret30":C.at[end,t]/C.at[D,t]-1,"infl90":infl})
R=pd.DataFrame(rows)
print("panel:",len(R),"token-months,",R.date.nunique(),"months, median tokens/month",int(R.groupby("date").size().median()),"| median annual buyback yield",f"{R['yield'].median():.2%}")
def monthly(P,col,sign,label):
    P=P.copy(); P["s"]=sign*P[col]; P["q"]=P.groupby("date").s.rank(pct=True)
    m=P.groupby("date").apply(lambda g: g[g.q>2/3].ret30.mean()-g[g.q<=1/3].ret30.mean()-0.004).dropna()
    ic=P.groupby("date").apply(lambda g: sp(g.s,g.ret30)).dropna(); pos=(ic>0).sum(); n=len(ic)
    p=sum(comb(n,k) for k in range(pos,n+1))/2**n
    rng=np.random.default_rng(0); bs=[m.sample(len(m),replace=True,random_state=int(x)).mean() for x in rng.integers(0,1e9,2000)]
    print(f"  {label:44s} {m.mean()*100:+.2f}%/month [{np.percentile(bs,5)*100:+.2f}, {np.percentile(bs,95)*100:+.2f}] | IC {ic.mean():+.3f}, right sign {pos}/{n} months (p={p:.4f})")
print("\n== buyback yield (long high / short low):")
monthly(R,"yield",+1,"all tokens with revenue")
monthly(R[R.date<"2025-01-01"],"yield",+1,"  2023-24"); monthly(R[R.date>="2025-01-01"],"yield",+1,"  2025-26")
O=R.dropna(subset=["infl90"]).copy()
print(f"\n== net supply on the overlap (tokens with both schedule and revenue): {len(O)} token-months, ~{int(O.groupby('date').size().median())} tokens/month")
O["net"]=O["infl90"]-O["yield"]*90/365
monthly(O,"infl90",-1,"supply growth alone (low wins)")
monthly(O,"net",-1,"net supply = growth - 90d buybacks (low wins)")
monthly(O,"yield",+1,"buyback yield alone")
