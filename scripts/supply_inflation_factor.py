"""Cross-sectional factor: scheduled 90-day supply growth vs next-period returns.

WHY (2026-09-27, docs/LLM_TRADING_RESEARCH_ROUND5_2026-09-27.md): the unlock
event study found one-off cliffs >= 10% hurt prices. This asks the general
question with continuous data: at each month start, rank ~45 tokens (DefiLlama
schedules x Binance spot prices, cached by scripts/unlock_event_study.py) by
how much NEW supply their public schedule releases in the next 90 days.
Findings (2023-01..2026-06): long low-growth / short high-growth terciles
+2.6%/month after 0.4% costs (76% of months positive); rank IC negative in
33/42 months (p=0.0001), 34/42 within token-age terciles, 33/42 within
market-cap terciles; non-overlapping 90-day holds +14.8%/quarter, 13/14
quarters positive. Excluding tokens with a >=10% cliff in the month still
leaves IC negative in 79% of months (linear vesting matters too).
Caveats: survivorship (tokens listed today), schedules as DefiLlama shows them
now (revisions unaudited), small universe. Run unlock_event_study.py first.
"""
import json, sys, numpy as np, pandas as pd
sys.path.insert(0,".")
from core.research.unlock_events import unlocked_series, cliff_events, entry_ticker
OUT="data/research/unlock_study"
C=pd.read_parquet(f"{OUT}/closes.parquet"); C.index=pd.to_datetime(C.index,utc=True); C=C.asfreq("1D")
index=json.load(open(f"{OUT}/emissions_index.json",encoding="utf-8")); index=index["data"] if isinstance(index,dict) else index
unl={}; big={}
for e in index:
    t=entry_ticker(e)
    if t not in C.columns or t in unl: continue
    try: sched=json.load(open(f"{OUT}/emissions/{e['protocolSlug']}.json",encoding="utf-8"))
    except Exception: continue
    u=unlocked_series(sched)
    if len(u): unl[t]=u.asfreq("1D").ffill(); big[t]=[x["date"] for x in cliff_events(e,u,10)]
print("tokens with prices + schedules:",len(unl))
dates=pd.date_range("2023-01-01","2026-06-01",freq="MS",tz="UTC")
rows=[]
for D in dates:
    for t,u in unl.items():
        if D not in C.index or pd.isna(C.at[D,t]) or D not in u.index or D+pd.Timedelta("90D") not in u.index: continue
        now=u[D]; fut=u[D+pd.Timedelta("90D")]
        if now<=0: continue
        end=D+pd.Timedelta("30D")
        if end not in C.index or pd.isna(C.at[end,t]): continue
        first=C[t].first_valid_index(); end90=D+pd.Timedelta("90D")
        r90=C.at[end90,t]/C.at[D,t]-1 if end90 in C.index and pd.notna(C.at[end90,t]) else np.nan
        rows.append({"date":D,"tok":t,"infl90":fut/now-1,"ret30":C.at[end,t]/C.at[D,t]-1,"ret90":r90,
                     "age":(D-first).days,"logmcap":np.log10(max(now*C.at[D,t],1.0)),
                     "big_cliff_30d":any(D<b<=end for b in big.get(t,[]))})
R=pd.DataFrame(rows)
R["ret_x"]=R.ret30-R.groupby("date").ret30.transform("mean")   # vs equal-weight sample
print("panel:",len(R),"token-months,",R.date.nunique(),"months, median tokens/month",int(R.groupby("date").size().median()))
print("scheduled 90d supply growth: median",f"{R.infl90.median():.1%}","| p90",f"{R.infl90.quantile(.9):.1%}")
def sp(a,b):
    x=a.rank().to_numpy(float); y=b.rank().to_numpy(float); x-=x.mean(); y-=y.mean(); d=np.sqrt((x*x).sum()*(y*y).sum()); return (x*y).sum()/d if d else np.nan
def run(P,label):
    P=P.copy(); P["q"]=P.groupby("date").infl90.rank(pct=True)
    m=P.groupby("date").apply(lambda g: g[g.q<=1/3].ret_x.mean()-g[g.q>2/3].ret_x.mean()-0.004).dropna()
    ic=P.groupby("date").apply(lambda g: sp(g.infl90,g.ret30)).dropna()
    rng=np.random.default_rng(0); bs=[m.sample(len(m),replace=True,random_state=int(s)).mean() for s in rng.integers(0,1e9,2000)]
    early=m[m.index<"2025-01-01"]; late=m[m.index>="2025-01-01"]
    print(f"\n== {label}: {len(m)} months, ~{int(P.groupby('date').size().median())} tokens/month")
    print(f"  long low-inflation / short high-inflation tercile, 30d, after 0.4% costs: mean {m.mean()*100:+.2f}%/month  median {m.median()*100:+.2f}%  positive months {(m>0).mean():.0%}  90%CI [{np.percentile(bs,5)*100:+.2f}, {np.percentile(bs,95)*100:+.2f}]")
    print(f"  by period: 2023-24 {early.mean()*100:+.2f}%/month (n={len(early)}) | 2025-26 {late.mean()*100:+.2f}%/month (n={len(late)})")
    print(f"  mean rank-IC(inflation, next 30d return) {ic.mean():+.3f}  (negative = more supply -> worse)  IC<0 in {(ic<0).mean():.0%} of months")

R["ret90_x"]=R.ret90-R.groupby("date").ret90.transform("mean")
from math import comb
def neutral_ic(P,by):
    P=P.copy(); P["b"]=P.groupby("date")[by].transform(lambda x: pd.qcut(x.rank(method="first"),3,labels=False))
    ics=P.groupby(["date","b"]).apply(lambda g: sp(g.infl90,g.ret30) if len(g)>=6 else np.nan).dropna()
    per_month=ics.groupby(level=0).mean(); neg=(per_month<0).sum(); n=len(per_month)
    p=sum(comb(n,k) for k in range(neg,n+1))/2**n
    print(f"  IC within {by} terciles: mean {per_month.mean():+.3f}, IC<0 in {neg}/{n} months (sign-test p={p:.4f})")
print("== robustness")
ic=R.groupby("date").apply(lambda g: sp(g.infl90,g.ret30)); neg=(ic<0).sum(); n=len(ic)
print(f"  raw IC<0 in {neg}/{n} months, sign-test p={sum(comb(n,k) for k in range(neg,n+1))/2**n:.5f}")
print(f"  corr(infl90, age) {sp(R.infl90,R.age):+.2f} | corr(infl90, logmcap) {sp(R.infl90,R.logmcap):+.2f}")
neutral_ic(R,"age"); neutral_ic(R,"logmcap")
Q=R[R.date.dt.month.isin([1,4,7,10])].dropna(subset=["ret90_x"]).copy()
Q["q"]=Q.groupby("date").infl90.rank(pct=True)
m=Q.groupby("date").apply(lambda g: g[g.q<=1/3].ret90_x.mean()-g[g.q>2/3].ret90_x.mean()-0.004)
print(f"  non-overlapping quarterly (90d hold): mean {m.mean()*100:+.2f}%/quarter, positive {(m>0).mean():.0%} of {len(m)} quarters, median {m.median()*100:+.2f}%")
