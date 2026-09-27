"""Download DefiLlama "holders revenue" (buybacks / burns / distributions) per token.

Products are mapped to parent protocols (lite/protocols2) to get the token
symbol and CoinGecko id; current circulating supply comes from CoinGecko
(proxy required). Writes data/research/buyback/{holders_revenue.parquet,meta.json}.
"""
import requests, json, time, sys, pathlib, pandas as pd
sys.path.insert(0,".")
from config.settings import settings
px=settings.HTTPS_PROXY or settings.HTTP_PROXY; prox={"https":px,"http":px} if px else None
out=pathlib.Path("data/research/buyback"); out.mkdir(parents=True,exist_ok=True)
def get(url,params=None):
    for a in range(4):
        try:
            r=requests.get(url,params=params,timeout=60,proxies=prox); time.sleep(0.4)
            if r.status_code==200: return r.json()
            if r.status_code in (404,400): return None
        except Exception: time.sleep(3*(a+1))
    return None
ov=get("https://api.llama.fi/overview/fees",{"dataType":"dailyHoldersRevenue","excludeTotalDataChart":"true","excludeTotalDataChartBreakdown":"true"})["protocols"]
lite=get("https://api.llama.fi/lite/protocols2")
parents={p["id"]:p for p in lite["parentProtocols"]}
protos={str(p.get("defillamaId") or p.get("id")):p for p in lite["protocols"]}
prot=[p for p in ov if (p.get("totalAllTime") or 0)>0]
print("protocols with any holders revenue:",len(prot))
series={}; tokmap={}
for i,p in enumerate(prot):
    par=parents.get(p.get("parentProtocol") or "")
    own=protos.get(str(p.get("defillamaId")))
    sym=(par or {}).get("symbol") or (own or {}).get("symbol"); gid=(par or {}).get("gecko_id") or (own or {}).get("gecko_id")
    if not sym or sym=="-" or not gid: continue
    j=get(f"https://api.llama.fi/summary/fees/{p['slug']}",{"dataType":"dailyHoldersRevenue"})
    ch=(j or {}).get("totalDataChart") or []
    if not ch: continue
    s=pd.Series({pd.Timestamp(int(t),unit="s",tz="UTC").normalize():float(v or 0) for t,v in ch})
    key=sym.upper(); series[key]=series[key].add(s,fill_value=0) if key in series else s; tokmap[key]=gid
    if i%40==0: print(i,len(prot),flush=True)
pd.DataFrame(series).sort_index().to_parquet(out/"holders_revenue.parquet")
# current circulating supply from CoinGecko (proxy required)
ids=sorted(set(tokmap.values())); circ={}
for k in range(0,len(ids),200):
    m=get("https://api.coingecko.com/api/v3/coins/markets",{"vs_currency":"usd","ids":",".join(ids[k:k+200]),"per_page":250})
    for row in m or []: circ[row["id"]]=row.get("circulating_supply")
json.dump({"tokmap":tokmap,"circ":circ},open(out/"meta.json","w"))
print("tokens with revenue series:",len(series),"| with circulating supply:",sum(1 for t,g in tokmap.items() if circ.get(g)))
