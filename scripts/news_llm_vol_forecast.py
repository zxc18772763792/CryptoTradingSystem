"""Do the news LLM's event labels forecast BTC volatility?

2026-09-25 finding: next-hour log realized variance, HAR baseline (1h/24h/168h
RV + hour/day seasonality) out-of-sample R^2 0.681; adding LLM event counts /
max impact / high-impact macro counts gives 0.678 (delta CI below zero).
The news adds nothing a standard volatility model does not already know.
Uses a BLAS-free OLS: this conda env's LAPACK can crash natively (exit 127).
Run: python scripts/news_llm_vol_forecast.py
"""
import sqlite3, glob, numpy as np, pandas as pd
def ols(X,y):  # BLAS-free (this env's LAPACK can crash natively)
    A=np.einsum("ij,ik->jk",X,X,optimize=False)+1e-8*np.eye(X.shape[1]); b=np.einsum("ij,i->j",X,y,optimize=False)
    n=len(b); M=np.column_stack([A,b]).astype(float)
    for c in range(n):
        p=c+int(np.argmax(np.abs(M[c:,c]))); M[[c,p]]=M[[p,c]]; M[c]/=M[c,c]
        for r in range(n):
            if r!=c: M[r]-=M[r,c]*M[c]
    return M[:,-1]
def mv(X,beta): return np.einsum("ij,j->i",X,beta,optimize=False)
k=pd.concat([pd.read_parquet(p,columns=["close"]) for p in glob.glob("data/historical/binance/BTC_USDT/1m_parts/*.parquet")])
k=k[~k.index.duplicated()].sort_index()["close"].astype(float); k.index=pd.to_datetime(k.index,utc=True); k=k.loc["2026-04-21":]
r=np.log(k).diff().dropna()
rv=(r**2).resample("1h").sum(); rv=rv[rv>0]
c=sqlite3.connect("file:data/news.db?mode=ro",uri=True)
ev=pd.read_sql("select e.created_at, e.impact_score, e.event_type, e.sentiment from news_events e where e.ts>='2026-04-21' and e.symbol in ('BTCUSDT','ETHUSDT')",c)
ev["t"]=pd.to_datetime(ev.created_at,utc=True,format="mixed",errors="coerce"); ev=ev.dropna(subset=["t"])
ev["h"]=ev.t.dt.floor("1h"); ev["imp"]=ev.impact_score.fillna(0.5)
H=pd.DataFrame(index=rv.index)
H["y"]=np.log(rv).shift(-1)               # next-hour log realized variance (target)
H["rv1"]=np.log(rv); H["rv24"]=np.log(rv.rolling(24).mean()); H["rv168"]=np.log(rv.rolling(168).mean())
H["hod"]=H.index.hour; H["dow"]=H.index.dayofweek
# LLM features known by the END of the current hour (labels created within the hour)
H["n_ev"]=np.log1p(ev.groupby("h").size().reindex(H.index).fillna(0))
H["n_hi"]=np.log1p(ev[ev.imp>=0.8].groupby("h").size().reindex(H.index).fillna(0))
H["max_imp"]=ev.groupby("h").imp.max().reindex(H.index).fillna(0)
H["n_macro_hi"]=np.log1p(ev[(ev.imp>=0.8)&(ev.event_type=="macro")].groupby("h").size().reindex(H.index).fillna(0))
H=H.dropna()
def design(df, llm):
    X=[np.ones(len(df)), df.rv1, df.rv24, df.rv168]
    X+= [ (df.hod==h).astype(float) for h in range(1,24)] + [(df.dow==d).astype(float) for d in range(1,7)]
    if llm: X+=[df.n_ev, df.n_hi, df.max_imp, df.n_macro_hi]
    return np.column_stack(X)
# walk-forward: fit on first 60%, test on last 40% (time split)
cut=int(len(H)*0.6); tr,te=H.iloc[:cut],H.iloc[cut:]
res={}
for llm in (False,True):
    beta=ols(design(tr,llm),tr.y.to_numpy())
    pred=mv(design(te,llm),beta); err=te.y.to_numpy()-pred
    res[llm]=(1-np.sum(err**2)/np.sum((te.y-te.y.mean())**2), err)
print(f"hours: train {len(tr)} test {len(te)} ({te.index.min().date()}..{te.index.max().date()})")
print(f"out-of-sample R^2 next-hour log RV: baseline (HAR+seasonality) {res[False][0]:.4f} | + LLM news features {res[True][0]:.4f} | delta {res[True][0]-res[False][0]:+.4f}")
# bootstrap delta by day
e0,e1=res[False][1]**2,res[True][1]**2; d=pd.Series(e0-e1,index=te.index).groupby(te.index.date).sum()
rng=np.random.default_rng(0); b=[d.iloc[rng.integers(0,len(d),len(d))].sum()/np.sum((te.y-te.y.mean())**2) for _ in range(2000)]
print(f"delta R^2 90% CI (day bootstrap): [{np.percentile(b,5):+.4f}, {np.percentile(b,95):+.4f}]")
# conditional view: next-hour vol ratio vs baseline prediction when high-impact news present
beta=ols(design(tr,False),tr.y.to_numpy()); te=te.assign(resid=te.y-mv(design(te,False),beta))
for name,mask in [("no LLM event",te.n_ev==0),("any event",te.n_ev>0),("high-impact event",te.n_hi>0),("high-impact macro",te.n_macro_hi>0)]:
    s=te[mask]; print(f"  {name:20s} hours={len(s):5d}  next-hour vol vs baseline forecast: x{np.exp(s.resid.mean()/2):.3f}")
