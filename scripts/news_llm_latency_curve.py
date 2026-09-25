"""How fast would the news LLM need to be? (BTC/ETH, 1m klines)

2026-09-25 finding: signed by the LLM's sentiment, price moves the right way in
the first minute after publication (macro: +1.1bp at +1m, CI above zero), but
the whole move is ~1bp while a taker fee alone is 4-10bp; the median 5m move
after an event (6.3bp) is barely above a random minute (5.6bp). From the moment
the label exists (median ~5 min later) to +60m: -0.1bp. So news-driven trading
on majors loses regardless of latency. Run: python scripts/news_llm_latency_curve.py
"""
import sqlite3, glob, importlib.util, sys, numpy as np, pandas as pd
sys.path.insert(0,"."); spec=importlib.util.spec_from_file_location("ace","scripts/agent_call_edge.py"); ace=importlib.util.module_from_spec(spec); spec.loader.exec_module(ace)
c=sqlite3.connect("file:data/news.db?mode=ro",uri=True)
ev=pd.read_sql("""select e.symbol, e.event_type, e.sentiment, e.impact_score, e.created_at, r.published_at, r.source
                  from news_events e join news_raw r on r.id=e.raw_news_id
                  where e.ts>='2026-04-21' and e.symbol in ('BTCUSDT','ETHUSDT') and e.sentiment!=0""",c)
ev["pub"]=pd.to_datetime(ev.published_at,utc=True,format="mixed",errors="coerce"); ev["done"]=pd.to_datetime(ev.created_at,utc=True,format="mixed",errors="coerce")
ev=ev.dropna(subset=["pub","done"]); ev=ev[(ev.done>=ev.pub)&(ev.done-ev.pub<pd.Timedelta("60min"))]
ev["impact"]=ev.impact_score.fillna(0.5)
# de-dup: one event per symbol per 30min window (highest impact)
ev["win"]=ev.pub.dt.floor("30min"); ev=ev.sort_values("impact",ascending=False).drop_duplicates(["symbol","win"]).sort_values("pub")
closes={}
for s in ["BTC_USDT","ETH_USDT"]:
    k=pd.concat([pd.read_parquet(p,columns=["close"]) for p in glob.glob(f"data/historical/binance/{s}/1m_parts/*.parquet")])
    k=k[~k.index.duplicated()].sort_index()["close"].astype(float); k.index=pd.to_datetime(k.index,utc=True); closes[s.replace("_","")]=k
MARKS=[1,2,3,5,10,15,30,60]
def price_at(k,t):  # last close known at time t (bar index = minute open)
    i=int(k.index.searchsorted(t-pd.Timedelta("1min"),side="right"))-1
    return k.iloc[i] if i>=0 else np.nan
rows=[]
for e in ev.itertuples():
    k=closes[e.symbol]; t0=e.pub.floor("1min"); p0=price_at(k,t0)
    rec={"day":t0.date(),"imp":e.impact,"sent":e.sentiment,"lag":(e.done-e.pub).total_seconds()/60,"type":e.event_type}
    for m in MARKS: rec[f"m{m}"]=e.sentiment*(price_at(k,t0+pd.Timedelta(minutes=m))/p0-1)
    pd_=price_at(k,e.done); rec["after_llm_60"]=e.sentiment*(price_at(k,e.done+pd.Timedelta("60min"))/pd_-1)
    rec["abs5"]=abs(price_at(k,t0+pd.Timedelta("5min"))/p0-1)
    rows.append(rec)
R=pd.DataFrame(rows).dropna()
# baseline |5m move| at random minutes
base=pd.concat([ (k/k.shift(5)-1).abs().loc["2026-04-21":] for k in closes.values()]).median()
def show(sub,label):
    days=sub.day
    print(f"\n== {label}: n={len(sub)} events, median LLM lag {sub.lag.median():.1f} min, |5m move| median {sub.abs5.median()*1e4:.1f}bp vs random-minute {base*1e4:.1f}bp")
    line="  signed cum. return from publish (bp): "
    for m in MARKS:
        lo,hi=ace.day_bootstrap_ci(sub[f"m{m}"],days)
        line+=f"+{m}m {sub[f'm{m}'].mean()*1e4:+.1f}[{lo*1e4:+.1f},{hi*1e4:+.1f}]  "
    print(line)
    lo,hi=ace.day_bootstrap_ci(sub["after_llm_60"],days)
    print(f"  tradable: from LLM-done to +60m: {sub.after_llm_60.mean()*1e4:+.1f}bp [{lo*1e4:+.1f},{hi*1e4:+.1f}] | direction hit @+3m {(sub.m3>0).mean():.0%}, @+60m {(sub.m60>0).mean():.0%}")
show(R,"BTC+ETH all LLM-labelled")
show(R[R.imp>=0.8],"impact >= 0.8")
show(R[R.imp>=0.9],"impact >= 0.9")
for t in ["macro","etf","regulation","hack","liquidation","institution"]:
    s=R[R.type==t]
    if len(s)>=80: show(s,f"type={t}")
