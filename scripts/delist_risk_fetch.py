"""Download daily close + quote volume for every Binance USDT spot pair, delisted included.

Binance's exchangeInfo still lists delisted pairs (status BREAK) and serves
their klines, so a delisting study can use a survivorship-free universe.
Writes data/research/delist_risk/{close,quote_volume}.parquet (~10 min).
"""
import requests, time, re, pandas as pd, pathlib, sys
sys.path.insert(0,".")
from config.settings import settings
px=settings.HTTPS_PROXY or settings.HTTP_PROXY; prox={"https":px,"http":px} if px else None
out=pathlib.Path("data/research/delist_risk"); out.mkdir(parents=True,exist_ok=True)
info=requests.get("https://api.binance.com/api/v3/exchangeInfo",timeout=60,proxies=prox).json()["symbols"]
skip=re.compile(r"(UP|DOWN|BULL|BEAR)$|^(USDC|BUSD|TUSD|FDUSD|USDP|DAI|PAX|EUR|GBP|AUD|UST|USTC|USDS|USDSB|SUSD|AEUR|XUSD|USD1|PYUSD|RLUSD|U)$")
syms=[s["symbol"] for s in info if s["quoteAsset"]=="USDT" and not skip.search(s["baseAsset"])]
def get(params):
    for a in range(5):
        try:
            r=requests.get("https://api.binance.com/api/v3/klines",params=params,timeout=30,proxies=prox); time.sleep(0.12); return r
        except Exception: time.sleep(3*(a+1))
    return None
close,vol={},{}
for i,sym in enumerate(syms):
    rows=[]; start=int(pd.Timestamp("2022-06-01",tz="UTC").timestamp()*1000)
    while True:
        r=get({"symbol":sym,"interval":"1d","startTime":start,"limit":1000})
        if r is None or r.status_code!=200: break
        b=r.json(); rows+=b
        if len(b)<1000: break
        start=b[-1][0]+86400000
    if rows:
        idx=pd.to_datetime([x[0] for x in rows],unit="ms",utc=True)
        close[sym[:-4]]=pd.Series([float(x[4]) for x in rows],index=idx); vol[sym[:-4]]=pd.Series([float(x[7]) for x in rows],index=idx)
    if i%100==0: print(i,len(syms),flush=True)
pd.DataFrame(close).to_parquet(out/"close.parquet"); pd.DataFrame(vol).to_parquet(out/"quote_volume.parquet")
print("saved",len(close),"symbols")
