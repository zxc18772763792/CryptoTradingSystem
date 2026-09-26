"""Binance announcement event studies (2024+): what structural effects exist?

WHY THIS EXISTS (2026-09-26, docs/LLM_TRADING_RESEARCH_ROUND3_2026-09-26.md):
After intraday LLM timing, news sentiment and LLM-written TA strategies all
failed, the remaining place an LLM could add value is parsing structural
events whose price effect outlasts its latency. This script fetches the
announcement history (public CMS API, paced) and prices around each event
(public Binance klines), then reports three studies:

  futures   perp launched for a coin already on spot: BTC-adjusted drift from
            +1h, vs pumped coins without a launch. Found: -11.7pp at 1d,
            -15pp at 7d vs matched control, only after a >=15% pre-pump;
            ZERO qualifying events in 2026 (perps now launch with spot).
  delist    token delisting notice: spot drift from +1h to delist-1d and the
            perp's squeeze tail. Found: spot median -34%, 91% down, all years;
            shorting the perp wins 82% but averages -22% (ALPACA +2144%).
  listing   short new perps D2 close -> D14 close with a stop, funding counted.
            Found (229 perps): +11..16% mean per trade, CI above zero, strong
            2025-26, weak 2024; stop fills assume no intraday gap.

Research only. Usage:
  python scripts/exchange_event_studies.py --refresh     # re-fetch everything
  python scripts/exchange_event_studies.py               # reuse cached data
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

OUT_DIR = PROJECT_ROOT / "data" / "research" / "binance_announcements"
CMS = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
SPOT = "https://api.binance.com/api/v3/klines"
FAPI = "https://fapi.binance.com/fapi/v1/klines"
FUNDING = "https://fapi.binance.com/fapi/v1/fundingRate"
SINCE_MS = 1704067200000  # 2024-01-01
EXCLUDE = re.compile(r"Pre-IPO|TradFi|Pre-Market|Delivery|Quarterly|COIN-M|Stock|Index", re.I)
PACE = 0.25


def _client():
    import httpx

    from config.settings import settings

    kw: Dict[str, Any] = {"timeout": 25}
    proxy = settings.HTTPS_PROXY or settings.HTTP_PROXY
    if proxy:
        kw["proxy"] = proxy
    return httpx.Client(**kw)


def fetch_history(c) -> Dict[str, List[Dict[str, Any]]]:
    out = {}
    for cat in (161, 48):  # delisting, new listings
        arts, page = [], 1
        while True:
            r = c.get(CMS, params={"type": 1, "catalogId": cat, "pageNo": page, "pageSize": 50})
            batch = [a for cc in ((r.json().get("data") or {}).get("catalogs") or []) for a in (cc.get("articles") or [])]
            if not batch:
                break
            arts += [{"code": a.get("code"), "title": a.get("title"), "release_ms": a.get("releaseDate")} for a in batch]
            page += 1
            time.sleep(0.6)
        out[str(cat)] = arts
    return out


def klines(c, url, symbol, start, end=None, interval="1h") -> Optional[pd.DataFrame]:
    params = {"symbol": symbol, "interval": interval, "startTime": start, "limit": 1000}
    if end:
        params["endTime"] = end
    r = c.get(url, params=params)
    time.sleep(PACE)
    rows = r.json() if r.status_code == 200 else None
    if not isinstance(rows, list) or not rows:
        return None
    frame = pd.DataFrame([[x[0], float(x[1]), float(x[2]), float(x[4])] for x in rows], columns=["t", "o", "h", "c"])
    frame.index = pd.to_datetime(frame.pop("t"), unit="ms", utc=True)
    return frame


def _boot(x: pd.Series) -> str:
    x = x.dropna()
    rng = np.random.default_rng(0)
    means = [x.sample(len(x), replace=True, random_state=int(s)).mean() for s in rng.integers(0, 1e9, 2000)]
    return (f"n={len(x):3d} mean {x.mean() * 100:+6.1f}% median {x.median() * 100:+6.1f}% "
            f"up {(x > 0).mean():.0%} 90%CI [{np.percentile(means, 5) * 100:+.1f}%, {np.percentile(means, 95) * 100:+.1f}%]")


def _adj(k: pd.Series, b: pd.Series, a, z) -> float:
    try:
        z_k = k.index[k.index <= z].max()
        return (k[z_k] / k[a] - 1) - (b.asof(z_k) / b.asof(a) - 1)
    except (KeyError, ValueError, TypeError):
        return np.nan


def study_futures(c, hist) -> None:
    rows = []
    for a in hist["48"]:
        t = a["title"]
        if a["release_ms"] < SINCE_MS or "Binance Futures Will Launch" not in t or EXCLUDE.search(t):
            continue
        for tk in re.findall(r"\b([A-Z0-9]{2,20})USDT\b", t):
            base = re.sub(r"^(1000000|100000|10000|1000)", "", tk)
            start = a["release_ms"] - 72 * 3600_000
            spot = klines(c, SPOT, base + "USDT", start, a["release_ms"] + 8 * 86400_000)
            t0 = pd.Timestamp(a["release_ms"], unit="ms", tz="UTC").floor("1h")
            if spot is None or spot.index.min() > t0 - pd.Timedelta("48h"):
                continue  # spot must pre-exist
            btc = klines(c, SPOT, "BTCUSDT", start, a["release_ms"] + 8 * 86400_000)["c"]
            k = spot["c"]
            rows.append({"base": base, "year": t0.year, "pre24": _adj(k, btc, t0 - pd.Timedelta("24h"), t0),
                         "d1": _adj(k, btc, t0 + pd.Timedelta("1h"), t0 + pd.Timedelta("24h")),
                         "d7": _adj(k, btc, t0 + pd.Timedelta("1h"), t0 + pd.Timedelta("169h"))})
    R = pd.DataFrame(rows)
    print(f"\n[futures launch on existing spot coins] events: {len(R)}, by year {R['year'].value_counts().to_dict() if len(R) else {}}")
    if len(R):
        pumped = R[R["pre24"] >= 0.15]
        print("  pre-pumped >=15%  d1:", _boot(pumped["d1"]), "\n                    d7:", _boot(pumped["d7"]))
        print("  not pre-pumped    d7:", _boot(R[R["pre24"] < 0.15]["d7"]))


def study_delist(c, hist) -> None:
    rows = []
    seen = set()
    for a in hist["161"] + hist["48"]:
        m = re.match(r"^Binance Will Delist (.+?) on (\d{4}-\d{2}-\d{2})", a["title"])
        if not m or a["release_ms"] < SINCE_MS:
            continue
        for tk in re.split(r",\s*|\s+and\s+|\s*&\s*", m.group(1)):
            tk = tk.strip()
            if not re.fullmatch(r"[A-Z0-9]{2,12}", tk) or (tk, a["release_ms"]) in seen:
                continue
            seen.add((tk, a["release_ms"]))
            t0 = pd.Timestamp(a["release_ms"], unit="ms", tz="UTC").floor("1h")
            dl = pd.Timestamp(m.group(2), tz="UTC")
            end = int((dl + pd.Timedelta("1D")).timestamp() * 1000)
            spot = klines(c, SPOT, tk + "USDT", a["release_ms"] - 3600_000, end)
            perp = klines(c, FAPI, tk + "USDT", a["release_ms"] - 3600_000, end)
            btc = klines(c, SPOT, "BTCUSDT", a["release_ms"] - 3600_000, end)
            if spot is None or btc is None:
                continue
            t1 = t0 + pd.Timedelta("1h")
            rows.append({"base": tk, "year": t0.year,
                         "spot_to_dl_minus1d": _adj(spot["c"], btc["c"], t1, dl - pd.Timedelta("1D")),
                         "perp_d7": _adj(perp["c"], btc["c"], t1, t1 + pd.Timedelta("7D")) if perp is not None and t1 in perp.index else np.nan})
    R = pd.DataFrame(rows)
    print(f"\n[delisting notices] coins: {len(R)}")
    if len(R):
        print("  spot, +1h -> delist-1d :", _boot(R["spot_to_dl_minus1d"]))
        short = -R["perp_d7"].dropna()
        print(f"  short perp 7d          : win {(short > 0).mean():.0%} median {short.median() * 100:+.1f}% "
              f"mean {short.mean() * 100:+.1f}% worst {short.min() * 100:+.0f}% (n={len(short)})")


def study_listing(c, hist, stop: float) -> None:
    tickers: Dict[str, int] = {}
    for a in hist["48"]:
        t = a["title"]
        if a["release_ms"] >= SINCE_MS and "Binance Futures Will Launch" in t and not EXCLUDE.search(t):
            for tk in re.findall(r"\b([A-Z0-9]{2,20}USDT)\b", t):
                tickers.setdefault(tk, a["release_ms"])
    results = []
    for tk, ms in tickers.items():
        k = klines(c, FAPI, tk, ms - 86400_000, interval="1d")
        if k is None or len(k) < 15:
            continue
        r = c.get(FUNDING, params={"symbol": tk, "startTime": int(k.index[0].timestamp() * 1000), "limit": 1000})
        time.sleep(PACE)
        funding = r.json() if r.status_code == 200 and isinstance(r.json(), list) else []
        entry = k["c"].iloc[2] * 0.995  # short at D2 close, 0.5% worse (thin books)
        exit_px, exit_t = k["c"].iloc[14], k.index[14]
        for i in range(3, 15):
            if k["h"].iloc[i] >= entry * (1 + stop):
                exit_px, exit_t = entry * (1 + stop) * 1.02, k.index[i]  # stop + 2% slippage; intraday gaps not modelled
                break
        entry_ms, exit_ms = k.index[2].timestamp() * 1000, exit_t.timestamp() * 1000
        fund = sum(float(x["fundingRate"]) for x in funding if entry_ms <= int(x["fundingTime"]) <= exit_ms)
        results.append({"tk": tk, "year": k.index[2].year, "ret": entry / exit_px - 1 + fund - 0.003})
    R = pd.DataFrame(results)
    print(f"\n[new perps: short D2 close -> D14 close, stop +{stop:.0%}, funding + 0.3% costs]")
    if len(R):
        print("  all      :", _boot(R["ret"]))
        for year, grp in R.groupby("year"):
            print(f"  {year}     :", _boot(grp["ret"]))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--refresh", action="store_true", help="re-fetch the announcement history")
    parser.add_argument("--stop", type=float, default=0.40)
    parser.add_argument("--only", choices=["futures", "delist", "listing"])
    args = parser.parse_args(argv)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / "history.json"
    with _client() as c:
        if args.refresh or not path.exists():
            path.write_text(json.dumps(fetch_history(c), ensure_ascii=False), encoding="utf-8")
        hist = json.loads(path.read_text(encoding="utf-8"))
        print(f"announcements: delisting {len(hist['161'])}, listings {len(hist['48'])}")
        if args.only in (None, "futures"):
            study_futures(c, hist)
        if args.only in (None, "delist"):
            study_delist(c, hist)
        if args.only in (None, "listing"):
            study_listing(c, hist, args.stop)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
