"""Can Binance delistings be predicted, and does shorting the at-risk coins pay?

2026-09-27 finding (monthly panel Mar 2023-Jul 2026, 14,680 coin-months, 209
delisting announcements within 60 days; logistic on ranked features fitted on
2023-24, tested on 2025-26): AUC 0.75 in both periods, top-5% risk bucket hits
5x the base rate (9.5% vs 1.9% in test) - delistings ARE predictable from low
volume, weak 90d return, deep drawdown. But a hedged 30-day short of that
bucket lost -4.0%/month in 2023-24 and made an insignificant +1.4% in 2025-26:
illiquid beaten-down coins squeeze. Use: defensive risk flag, not a short.
Run scripts/delist_risk_fetch.py first.
"""
import json, sys, numpy as np, pandas as pd
sys.path.insert(0,".")
from core.research.exchange_notices import load_history, parse_notice
C=pd.read_parquet("data/research/delist_risk/close.parquet"); V=pd.read_parquet("data/research/delist_risk/quote_volume.parquet")
for f in (C,V): f.index=pd.to_datetime(f.index,utc=True)
C=C.asfreq("1D"); V=V.reindex(C.index)
btc=C["BTC"]
# delisting announcements (token -> announce date)
dl={}
for a in load_history()["161"]+load_history()["48"]:
    n=parse_notice(a["title"])
    if n and n["kind"]=="delist":
        t=pd.Timestamp(a["release_ms"],unit="ms",tz="UTC").normalize()
        for tok in n["tokens"]: dl.setdefault(tok,[]).append(t)
print("delisted tokens with announcements:",len(dl),"| of those with price data:",sum(1 for t in dl if t in C))
dates=pd.date_range("2023-03-01","2026-07-01",freq="MS",tz="UTC")
rows=[]
for D in dates:
    hist=C.loc[:D-pd.Timedelta("1D")]
    alive=[s for s in C.columns if pd.notna(C.at[D,s]) and hist[s].notna().sum()>=120] if D in C.index else []
    for s in alive:
        c=hist[s].dropna(); v=V.loc[:D-pd.Timedelta("1D"),s].dropna()
        vol30=v.tail(30).mean(); vol180=v.tail(180).mean()
        ret90=(c.iloc[-1]/c.iloc[-91]-1)-(btc.asof(c.index[-1])/btc.asof(c.index[-91])-1) if len(c)>91 else np.nan
        dd=c.iloc[-1]/c.tail(365).max()-1
        lab=any(D < t <= D+pd.Timedelta("60D") for t in dl.get(s,[]))
        # forward 30d return (short closes at last traded price if the coin stops trading)
        fwd=C.loc[D:D+pd.Timedelta("30D"),s].dropna()
        f30=fwd.iloc[-1]/fwd.iloc[0]-1 if len(fwd)>1 else np.nan
        rows.append({"date":D,"sym":s,"logvol30":np.log10(max(vol30,1)),"voltrend":vol30/max(vol180,1),"ret90":ret90,"dd365":dd,"age":len(c),"label":lab,"fwd30":f30})
R=pd.DataFrame(rows); R.to_parquet("data/research/delist_risk/panel.parquet")
print("panel:",len(R),"coin-months |",R.date.nunique(),"months | positives:",int(R.label.sum()),"| base rate",round(R.label.mean(),4))
# rank features within month; risk-direction signs: low volume, falling volume, weak return, deep drawdown, (age?)
feats={"logvol30":-1,"voltrend":-1,"ret90":-1,"dd365":-1,"age":+1}
for f,sgn in feats.items(): R[f+"_r"]=R.groupby("date")[f].rank(pct=True)*sgn
X=[f+"_r" for f in feats]; R=R.dropna(subset=X+["fwd30"])
dev=R[R.date<"2025-01-01"]; test=R[R.date>="2025-01-01"]
# BLAS-free logistic
Xd=dev[X].to_numpy(float); y=dev.label.to_numpy(float); w=np.zeros(len(X)); b=np.log(y.mean()/(1-y.mean()))
for _ in range(4000):
    z=(Xd*w).sum(1)+b; p=1/(1+np.exp(-z)); g=p-y; w-=0.5*((Xd*g[:,None]).mean(0)+1e-3*w); b-=0.5*g.mean()
print("weights:",dict(zip(X,np.round(w,2))))
def auc(score,lab):
    r=pd.Series(score).rank().to_numpy(); pos=lab.astype(bool); n1=pos.sum(); n0=len(lab)-n1
    return (r[pos].sum()-n1*(n1+1)/2)/(n1*n0) if n1 and n0 else np.nan
for name,part in (("dev 2023-24",dev),("TEST 2025-26",test)):
    s=(part[X].to_numpy(float)*w).sum(1)+b; part=part.assign(score=s)
    part["q"]=part.groupby("date")["score"].rank(pct=True)
    top=part[part.q>=0.95]
    print(f"\n== {name}: coin-months {len(part)}, positives {int(part.label.sum())}")
    print(f"  AUC {auc(s,part.label.to_numpy()):.3f} | base rate {part.label.mean():.3%} | top-5% hit rate {top.label.mean():.2%} (x{top.label.mean()/max(part.label.mean(),1e-9):.1f}) | top-5% captures {top.label.sum()/max(part.label.sum(),1):.0%} of delistings")
    # hedged short: top-5% basket vs equal-weight universe, 30 days, 0.4% costs
    m=part.groupby("date").apply(lambda g: -(g[g.q>=0.95].fwd30.mean()-g.fwd30.mean())-0.004)
    rng=np.random.default_rng(0); bs=[m.sample(len(m),replace=True,random_state=int(x)).mean() for x in rng.integers(0,1e9,2000)]
    print(f"  hedged short of top-5% risk basket, 30d: mean {m.mean()*100:+.2f}%/month  median {m.median()*100:+.2f}%  positive months {(m>0).mean():.0%}  90%CI [{np.percentile(bs,5)*100:+.2f}, {np.percentile(bs,95)*100:+.2f}]  months={len(m)}")
