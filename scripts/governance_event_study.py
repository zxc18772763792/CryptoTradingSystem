"""Governance proposals (Snapshot) vs price: is there an effect an LLM could read?

2026-09-27 finding (11,209 closed proposals, 38 DAOs, 2022+; 36 tokens with
Binance prices; returns vs an equal-weight index of those tokens): proposals
with buyback / burn / fee-switch / revenue-share / emission-cut in the TITLE
(n=101) show no robust effect - vote window +0.0% [-3.3, +3.9], 30 days after
-1.6% [-4.6, +2.0]; passed ones a weak "sell the news" -2% over 7 days. The
information is likely priced during weeks of forum debate before a Snapshot
vote, so an LLM would have to read forums much earlier; direction parked.
Proposals are cached in data/research/governance/snapshot_proposals.json.
Usage: python scripts/governance_event_study.py <output_dir>
"""
import json, re, time, sys, requests, numpy as np, pandas as pd, pathlib
sys.path.insert(0,".")
from config.settings import settings
px=settings.HTTPS_PROXY or settings.HTTP_PROXY; prox={"https":px,"http":px} if px else None
SP=sys.argv[1]
d=json.load(open("data/research/governance/snapshot_proposals.json",encoding="utf-8")); P=d["proposals"]; spaces=d["spaces"]
VAL=re.compile(r"buy ?back|burn|fee switch|fee-switch|revenue shar|share (protocol )?revenue|distribute (protocol )?(fees|revenue)|fees? to (token)?holders|reduce (token )?emissions|emissions? reduction|cut emissions|lower emissions",re.I)
YES=re.compile(r"^(for|yes|yae|yea|approve|accept|in favor|in favour|support)",re.I)
def get(params):
    for a in range(5):
        try:
            r=requests.get("https://api.binance.com/api/v3/klines",params=params,timeout=30,proxies=prox); time.sleep(0.15); return r
        except Exception: time.sleep(3*(a+1))
    return None
cp=pathlib.Path("data/research/governance/closes.parquet")
if cp.exists(): C=pd.read_parquet(cp)
else:
    closes={}
    for tok in sorted(set(spaces.values())):
        rows=[]; start=int(pd.Timestamp("2021-12-01",tz="UTC").timestamp()*1000)
        while True:
            r=get({"symbol":tok+"USDT","interval":"1d","startTime":start,"limit":1000})
            if r is None or r.status_code!=200: break
            b=r.json(); rows+=b
            if len(b)<1000: break
            start=b[-1][0]+86400000
        if rows: closes[tok]=pd.Series([float(x[4]) for x in rows],index=pd.to_datetime([x[0] for x in rows],unit="ms",utc=True))
    C=pd.DataFrame(closes); C.to_parquet(cp)
C.index=pd.to_datetime(C.index,utc=True)
idx=(1+C.pct_change().mean(axis=1).fillna(0)).cumprod()
print("tokens with Binance prices:",C.shape[1],"of",len(set(spaces.values())))
rows=[]
for p in P:
    tok=spaces.get(p["space"]["id"])
    if tok not in C: continue
    k=C[tok].dropna()
    s=pd.Timestamp(p["start"],unit="s",tz="UTC").normalize(); e=pd.Timestamp(p["end"],unit="s",tz="UTC").normalize()
    def r(a,b):
        if a not in k.index or b not in k.index: return np.nan
        return (k[b]/k[a]-1)-(idx[b]/idx[a]-1)
    ch=p.get("choices") or []; sc=p.get("scores") or []
    passed=None
    if ch and sc and len(ch)==len(sc) and max(sc)>0:
        passed=bool(YES.match(str(ch[int(np.argmax(sc))]).strip()))
    rows.append({"space":p["space"]["id"],"tok":tok,"start":s,"end":e,"title":p["title"][:120],"val":bool(VAL.search(p["title"])),"passed":passed,
                 "vote":r(s-pd.Timedelta("1D"),e),"post7":r(e,e+pd.Timedelta("7D")),"post30":r(e,e+pd.Timedelta("30D")),"pre_to_post30":r(s-pd.Timedelta("1D"),e+pd.Timedelta("30D"))})
R=pd.DataFrame(rows); R.to_csv(f"{SP}/gov_events.csv",index=False)
def boot(x,m):
    f=pd.DataFrame({"v":x.values,"m":m.values}).dropna(); g=[gg.v.values for _,gg in f.groupby("m")]; rng=np.random.default_rng(0)
    b=[np.concatenate([g[i] for i in rng.integers(0,len(g),len(g))]).mean() for _ in range(2000)]; return np.percentile(b,[5,95])
def show(sub,label):
    sub=sub.drop_duplicates(["tok","start"])
    if len(sub)<15: print("\n==",label,"n<15"); return
    m=sub.start.dt.strftime("%Y-%m"); print(f"\n== {label}: n={len(sub)} ({sub.tok.nunique()} tokens)")
    for w in ("vote","post7","post30","pre_to_post30"):
        x=sub[w]; lo,hi=boot(x,m); print(f"  {w:14s} mean {x.mean()*100:+6.2f}% median {x.median()*100:+6.2f}%  90%CI [{lo*100:+.2f}, {hi*100:+.2f}]  n={x.notna().sum()}")
show(R[~R.val],"all other proposals (baseline)")
show(R[R.val],"value-accrual in title")
show(R[R.val & (R.passed==True)],"value-accrual, PASSED")
show(R[R.val & (R.passed==False)],"value-accrual, failed")
