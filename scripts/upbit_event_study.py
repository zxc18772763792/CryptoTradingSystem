"""Upbit trading notices vs Binance prices (round 7).

WHY: Upbit is retail-dominated Korean flow; its notices are Korean text that
Binance-centric traders under-read, and exactly the kind of source an LLM
parses for free. If Binance prices keep drifting days after an Upbit notice,
it is tradable at our speed. Notices from
api-manager.upbit.com/api/v1/announcements (category "trade"); tickers are the
"(XXX)" groups in the title; prices from the survivorship-free Binance spot
panel data/research/delist_risk/close.parquet, market-adjusted by the panel
median.

Day 0 = the UTC day of the notice. reaction = close(-1) -> close(0) (contains
the notice); drift = close(0) -> close(+7 / +14 / +30), i.e. what a trade
opened at the first daily close after the notice earns. Only tokens that
already traded on Binance 30 days before the notice (a real "cross-listing"
reaction, not a Binance debut). Month-clustered bootstrap CIs.
"""
import json
import re
import sys
import time
from math import comb
from pathlib import Path

import httpx
import numpy as np
import pandas as pd

sys.path.insert(0, ".")

OUT = Path("data/research/upbit")
API = "https://api-manager.upbit.com/api/v1/announcements"
KINDS = [  # first match wins; order matters (release before designation)
    ("caution_released", re.compile(r"유의\s*종목\s*지정\s*해제")),
    ("caution", re.compile(r"유의\s*종목\s*지정")),
    ("delist", re.compile(r"거래\s*지원\s*종료")),
    ("warning_urged", re.compile(r"유의\s*촉구")),
    # Listings. Round 7 v1 only knew the "거래지원 안내" wording and missed most of them;
    # 2026-09-28 added the "마켓 ... 추가 / 신규 상장 / 원화 마켓 오픈" wordings.
    ("listing_krw", re.compile(r"(신규\s*)?거래\s*지원\s*안내.*KRW|KRW.*(신규\s*)?거래\s*지원|(KRW|원화)[^(]*마켓[^(]*(추가|상장|오픈)|(원화|KRW)\s*마켓\s*(신규\s*)?상장")),
    ("listing_other", re.compile(r"신규\s*거래\s*지원|거래\s*지원\s*안내|(BTC|ETH|USDT)[^(]*마켓[^(]*(추가|오픈|거래\s*지원\s*예정)|\s상장(\s*안내)?$")),
    ("supply_change", re.compile(r"유통량")),
]
NOT_TICKER = {"KRW", "BTC", "USDT", "ETH"}


def fetch():
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / "notices.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    rows, page = [], 1
    with httpx.Client(timeout=30, headers={"User-Agent": "Mozilla/5.0"}) as c:
        while True:
            for attempt in range(6):
                resp = c.get(API, params={"os": "web", "page": page, "per_page": 20, "category": "trade"})
                if resp.status_code == 200 and resp.text.startswith("{"):
                    break
                time.sleep(5 * (attempt + 1))  # rate limited: back off
            data = resp.json()["data"]
            rows += data["notices"]
            if page >= data["total_pages"]:
                break
            page += 1
            time.sleep(1.5)
    path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return rows


def classify(title):
    for kind, rx in KINDS:
        if rx.search(title):
            return kind
    return None


def main():
    notices = fetch()
    close = pd.read_parquet("data/research/delist_risk/close.parquet")
    close.index = pd.to_datetime(close.index, utc=True)
    close = close.asfreq("1D")
    mkt = (1 + close.pct_change(fill_method=None).median(axis=1).fillna(0)).cumprod()
    events, kinds = [], {}
    for n in notices:
        kind = classify(n["title"])
        kinds[kind] = kinds.get(kind, 0) + 1
        if not kind:
            continue
        d0 = pd.Timestamp(n["first_listed_at"]).tz_convert("UTC").normalize()
        for t in {m for grp in re.findall(r"\(([^)]*)\)", n["title"]) for m in re.findall(r"[A-Z0-9]{2,10}", grp)} - NOT_TICKER:
            if t not in close.columns:
                continue
            pts = {k: d0 + pd.Timedelta(days=k) for k in (-30, -1, 0, 7, 14, 30)}
            if any(p not in close.index for p in pts.values()) or close.loc[[pts[-30], pts[-1], pts[0]], t].isna().any():
                continue
            px = close[t]

            def adj(a, b):
                if pd.isna(px[pts[b]]):
                    return np.nan  # delisted before the horizon
                return (px[pts[b]] / px[pts[a]] - 1) - (mkt[pts[b]] / mkt[pts[a]] - 1)

            events.append({"date": d0, "token": t, "kind": kind, "title": n["title"], "reaction": adj(-1, 0),
                           "d7": adj(0, 7), "d14": adj(0, 14), "d30": adj(0, 30)})
    ev = pd.DataFrame(events).drop_duplicates(["token", "kind", "date"])
    ev.to_csv(OUT / "events.csv", index=False)
    print(f"notices {len(notices)} ({notices[-1]['first_listed_at'][:10]} .. {notices[0]['first_listed_at'][:10]}) | kinds {kinds}")
    print(f"events with Binance history: {len(ev)}")

    def ci(frame, col):
        groups = [g[col].dropna().to_numpy() for _, g in frame.groupby(frame.date.dt.to_period("M"))]
        groups = [g for g in groups if len(g)]
        rng = np.random.default_rng(0)
        boots = [np.concatenate([groups[i] for i in rng.integers(0, len(groups), len(groups))]).mean() for _ in range(2000)]
        return np.percentile(boots, 5) * 100, np.percentile(boots, 95) * 100

    for kind in ["listing_krw", "listing_other", "caution", "caution_released", "warning_urged", "delist"]:
        f = ev[ev.kind == kind]
        if len(f) < 10:
            print(f"== {kind}: n={len(f)} (too few)")
            continue
        print(f"== {kind}: n={len(f)} on {f.token.nunique()} tokens | reaction day 0 (market-adj) mean {f.reaction.mean() * 100:+.1f}% median {f.reaction.median() * 100:+.1f}%")
        for col in ("d7", "d14", "d30"):
            x = f[col].dropna()
            lo, hi = ci(f, col)
            k = int((x > 0).sum())
            p_up = sum(comb(len(x), i) for i in range(k, len(x) + 1)) / 2 ** len(x)
            p_dn = sum(comb(len(x), i) for i in range(0, k + 1)) / 2 ** len(x)
            print(f"   close(0)->{col}: mean {x.mean() * 100:+.1f}% median {x.median() * 100:+.1f}% 90% CI [{lo:+.1f}, {hi:+.1f}] up {k}/{len(x)} (p_up={p_up:.3f}, p_down={p_dn:.3f}) delisted-before {f[col].isna().sum()}")


if __name__ == "__main__":
    main()
